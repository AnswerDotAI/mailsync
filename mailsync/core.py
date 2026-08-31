from email import policy
from email.parser import BytesParser
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote

from fastcore.xdg import Config, xdg_cache_home, xdg_config_home

import apsw, json, os


class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text,self.hidden = [],0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'): self.hidden += 1
        elif tag in ('br', 'p', 'div', 'li', 'tr'): self.text.append('\n')

    def handle_endtag(self, tag):
        if tag in ('script', 'style') and self.hidden: self.hidden -= 1
        elif tag in ('p', 'div', 'li', 'tr'): self.text.append('\n')

    def handle_data(self, data):
        if not self.hidden: self.text.append(data)


def _part_text(part):
    if part.get_content_disposition() == 'attachment' or part.get_filename(): return [],[]
    if part.is_multipart():
        plain,html = [],[]
        for child in part.iter_parts():
            p,h = _part_text(child)
            plain.extend(p)
            html.extend(h)
        return plain,html
    typ = part.get_content_type()
    if typ not in ('text/plain', 'text/html'): return [],[]
    try: text = part.get_content()
    except (LookupError, UnicodeError): text = part.get_payload(decode=True).decode('utf-8', 'replace')
    return ([text],[]) if typ == 'text/plain' else ([],[text])


def parse_email(raw:bytes) -> dict:
    "Parse searchable fields and all decoded headers from an RFC 822 message"
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    plain,html = _part_text(msg)
    if plain: body = '\n'.join(plain)
    else:
        parser = _HTMLText()
        for part in html: parser.feed(part)
        body = ' '.join(''.join(parser.text).split())
    headers = [(k, str(v)) for k,v in msg.items()]
    vals = lambda name: ', '.join(str(v) for v in msg.get_all(name, []))
    return dict(subject=vals('Subject'), sender=vals('From'), recipients=vals('To'), cc=vals('Cc'),
        message_id=vals('Message-ID'), body=body, headers=headers)


def config_dir(): return xdg_config_home()/'mailsync'
def cache_dir(): return xdg_cache_home()/'mailsync'
def account_slug(account): return quote(account.casefold(), safe='@._+-')
def db_path(account): return cache_dir()/f'{account_slug(account)}.sqlite3'


def settings(account=None, token_path=None, initial_lookback_days=365, include_labels=None, exclude_labels=None, poll_seconds=60):
    "Load settings, creating them from `account` when run for the first time"
    path = config_dir()
    cfg_file = path/'config.ini'
    if not cfg_file.exists():
        if not account: raise FileNotFoundError(f'No {cfg_file}; run mailsync --account EMAIL first')
        create = dict(account=account, initial_lookback_days=initial_lookback_days,
            include_labels=json.dumps(include_labels or []), exclude_labels=json.dumps(exclude_labels or []), poll_seconds=poll_seconds)
        if token_path: create['token_path'] = Path(token_path).expanduser()
    else: create = None
    cfg = Config(path, 'config.ini', create=create,
        types=dict(token_path=Path, initial_lookback_days=int, include_labels=json.loads, exclude_labels=json.loads, poll_seconds=int))
    if account and account.casefold() != cfg.account.casefold(): raise ValueError(f'Configured account is {cfg.account}, not {account}')
    if cfg.initial_lookback_days < 0: raise ValueError('initial_lookback_days must be non-negative')
    if cfg.poll_seconds <= 0: raise ValueError('poll_seconds must be positive')
    return cfg


SCHEMA = '''
pragma journal_mode=wal;
pragma synchronous=normal;
pragma foreign_keys=on;
create table if not exists state (key text primary key, value text not null);
create table if not exists labels (id text primary key, name text not null, type text);
create table if not exists messages (
    id text primary key, thread_id text not null, history_id integer not null, internal_date integer not null,
    size_estimate integer, snippet text, subject text, sender text, recipients text, cc text, message_id text, body text, raw blob not null);
create index if not exists messages_date on messages(internal_date);
create index if not exists messages_message_id on messages(message_id);
create table if not exists message_labels (
    message_id text not null references messages(id) on delete cascade, label_id text not null,
    primary key(message_id, label_id));
create index if not exists message_labels_label on message_labels(label_id);
create table if not exists headers (
    message_id text not null references messages(id) on delete cascade, seq integer not null, name text not null, value text not null,
    primary key(message_id, seq));
create index if not exists headers_name_value on headers(name, value);
create virtual table if not exists message_fts using fts5(
    message_id unindexed, subject, sender, recipients, cc, body, tokenize='unicode61 remove_diacritics 2');
'''


class Store:
    def __init__(self, path):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = apsw.Connection(str(self.path))
        self.db.set_busy_timeout(5000)
        list(self.db.execute(SCHEMA))
        os.chmod(self.path, 0o600)

    def close(self): self.db.close()

    def state(self, key, default=None):
        row = self.db.execute('select value from state where key=?', (key,)).fetchone()
        return row[0] if row else default

    def set_state(self, key, value):
        self.db.execute('insert into state values (?,?) on conflict(key) do update set value=excluded.value', (key,str(value)))

    def ids(self, floor=None):
        sql,args = ('select id from messages', ()) if floor is None else ('select id from messages where internal_date>=?', (floor,))
        return {r[0] for r in self.db.execute(sql, args)}

    def delete(self, ids):
        deleted = 0
        for mid in ids:
            self.db.execute('delete from message_fts where message_id=?', (mid,))
            self.db.execute('delete from messages where id=?', (mid,))
            deleted += self.db.changes()
        return deleted

    def labels(self, labels):
        self.db.executemany('insert into labels values (?,?,?) on conflict(id) do update set name=excluded.name,type=excluded.type',
            ((x['id'], x['name'], x.get('type')) for x in labels))
        ids = [x['id'] for x in labels]
        if ids:
            slots = ','.join('?' for _ in ids)
            self.db.execute(f'delete from labels where id not in ({slots})', ids)

    def put(self, message):
        parsed = parse_email(message['raw'])
        vals = (message['id'], message['threadId'], int(message['historyId']), int(message['internalDate']), message.get('sizeEstimate'),
            message.get('snippet'), parsed['subject'], parsed['sender'], parsed['recipients'], parsed['cc'], parsed['message_id'], parsed['body'], message['raw'])
        self.db.execute('''insert into messages values (?,?,?,?,?,?,?,?,?,?,?,?,?) on conflict(id) do update set
            thread_id=excluded.thread_id,history_id=excluded.history_id,internal_date=excluded.internal_date,size_estimate=excluded.size_estimate,
            snippet=excluded.snippet,subject=excluded.subject,sender=excluded.sender,recipients=excluded.recipients,cc=excluded.cc,
            message_id=excluded.message_id,body=excluded.body,raw=excluded.raw''', vals)
        self.put_labels(message['id'], message.get('labelIds', []))
        self.db.execute('delete from headers where message_id=?', (message['id'],))
        self.db.executemany('insert into headers values (?,?,?,?)',
            ((message['id'], i, k, v) for i,(k,v) in enumerate(parsed['headers'])))
        self.db.execute('delete from message_fts where message_id=?', (message['id'],))
        self.db.execute('insert into message_fts values (?,?,?,?,?,?)',
            (message['id'], parsed['subject'], parsed['sender'], parsed['recipients'], parsed['cc'], parsed['body']))

    def put_labels(self, mid, label_ids):
        "Update only a cached message's canonical Gmail labels"
        self.db.execute('delete from message_labels where message_id=?', (mid,))
        self.db.executemany('insert into message_labels values (?,?)', ((mid, x) for x in label_ids))

    def search(self, query, label=None, limit=20):
        label_sql = 'and exists (select 1 from message_labels ml join labels l on l.id=ml.label_id where ml.message_id=m.id and l.name=?)' if label else ''
        args = [query]
        if label: args.append(label)
        args.append(limit)
        return list(self.db.execute(f'''select m.id,m.internal_date,m.sender,m.subject,
            coalesce((select group_concat(l.name) from message_labels ml join labels l on l.id=ml.label_id where ml.message_id=m.id),''),
            snippet(message_fts,5,'[',']',' … ',12)
            from message_fts join messages m on m.id=message_fts.message_id
            where message_fts match ? {label_sql} order by rank limit ?''', args))

    def backup(self, path):
        path = Path(path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        dest = apsw.Connection(str(path))
        with dest.backup('main', self.db, 'main') as backup: backup.step()
        dest.close()
        os.chmod(path, 0o600)
        return path

    def __enter__(self): return self
    def __exit__(self, *args): self.close()
