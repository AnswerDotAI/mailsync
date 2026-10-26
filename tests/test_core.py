from email.message import EmailMessage

from mailsync.core import Store, parse_email
from mailsync.sync import changes, keep, selected


def test_parse_email_prefers_plain_and_skips_attachment():
    msg = EmailMessage()
    msg['Subject'] = 'Café plans'
    msg['From'] = 'Alice <alice@example.com>'
    msg['To'] = 'Bob <bob@example.com>'
    msg.set_content('The plain body')
    msg.add_alternative('<p>The <b>HTML</b> body</p>', subtype='html')
    msg.add_attachment('private attachment', filename='secret.txt')
    parsed = parse_email(msg.as_bytes())
    assert parsed['subject'] == 'Café plans'
    assert parsed['body'].strip() == 'The plain body'
    assert 'private attachment' not in parsed['body']


def test_mirror_state_logic():
    assert selected(['INBOX', 'work'], {'work'}, {'SPAM'})
    assert not selected(['work', 'SPAM'], {'work'}, {'SPAM'})
    history = [{'messagesAdded':[{'message':{'id':'a'}}]},
        {'labelsRemoved':[{'message':{'id':'b'}}]}, {'messagesDeleted':[{'message':{'id':'c'}}]}]
    assert changes(history) == ({'a','b','c'}, {'a'}, {'c'})
    assert keep(dict(internalDate='5', labelIds=['work']), {'work'}, set(), floor=5)
    assert not keep(dict(internalDate='4', labelIds=['work']), {'work'}, set(), floor=5)
    assert keep(dict(labelIds=['work']), {'work'}, set())


def test_store_replaces_derived_state(tmp_path):
    msg = EmailMessage()
    msg['Subject'] = 'before'
    msg.set_content('alpha')
    record = dict(id='1', threadId='t', historyId='1', internalDate='1', raw=msg.as_bytes(), labelIds=['INBOX'])
    with Store(tmp_path/'mail.sqlite3') as store:
        store.labels([dict(id='INBOX', name='Inbox', type='system')])
        store.put(record)
        assert store.ids(1) == {'1'} and not store.ids(2)
        assert store.search('alpha')[0][0] == '1'
        msg.replace_header('Subject', 'after')
        msg.set_content('beta')
        record['raw'] = msg.as_bytes()
        store.put(record)
        assert not store.search('alpha')
        assert store.search('beta')[0][3] == 'after'
        store.put_labels('1', [])
        assert store.ids() == {'1'}
        assert store.delete({'missing'}) == 0
        assert store.delete({'1', 'missing'}) == 1
        assert not store.search('beta')
