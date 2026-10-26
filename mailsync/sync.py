from base64 import urlsafe_b64decode
from datetime import datetime, timedelta, timezone

from fastgws import GMail
from fastgws.auth import oauth_creds
from fasttransport.errors import APIError

from .core import Store, db_path

import asyncio, hashlib, json
from contextlib import suppress


class HistoryExpired(Exception): pass


def _fingerprint(cfg):
    data = dict(include=cfg.include_labels, exclude=cfg.exclude_labels)
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def _resolve_labels(names, labels):
    by_name = {x['name'].casefold():x['id'] for x in labels}
    ids = {x['id'] for x in labels}
    missing = [x for x in names if x not in ids and x.casefold() not in by_name]
    if missing: raise ValueError(f'Unknown Gmail labels: {", ".join(missing)}')
    return {x if x in ids else by_name[x.casefold()] for x in names}


def selected(label_ids, include, exclude):
    "Whether canonical Gmail label IDs match the configured mirror selection"
    label_ids = set(label_ids)
    return (not include or bool(label_ids & include)) and not bool(label_ids & exclude)

def keep(msg, include, exclude, floor=None):
    "Whether a fetched message belongs in the mirror: label-selected, and dated on or after `floor` when one is given"
    if floor is not None and int(msg['internalDate']) < floor: return False
    return selected(msg.get('labelIds', []), include, exclude)


def changes(history):
    "Return (all changed, added, deleted) message ID sets"
    ids,added,deleted = set(),set(),set()
    for event in history:
        ids.update(x['id'] for x in event.get('messages', []))
        a = {x['message']['id'] for x in event.get('messagesAdded', [])}
        d = {x['message']['id'] for x in event.get('messagesDeleted', [])}
        other = {x['message']['id'] for kind in ('labelsAdded', 'labelsRemoved') for x in event.get(kind, [])}
        ids.update(a|d|other)
        added.update(a)
        deleted.update(d)
    return ids,added,deleted


class GmailMirror:
    def __init__(self, cfg, batch_size=50, progress=None, progress_seconds=10):
        if progress_seconds < 0: raise ValueError('progress_seconds must be non-negative')
        self.cfg,self.batch_size,self.progress,self.progress_seconds = cfg,batch_size,progress,progress_seconds
        self.store = Store(db_path(cfg.account))

    async def connect(self):
        token_path = self.cfg.get('token_path')
        creds = await oauth_creds(token_path=token_path) if token_path else await oauth_creds(account=self.cfg.account)
        self.gmail = GMail(creds=creds)
        return self

    async def labels(self):
        response = await self.gmail.users.labels.list(user_id='me', fields='labels(id,name,type)')
        labels = list(response.get('labels', []))
        include = _resolve_labels(self.cfg.include_labels, labels)
        exclude = _resolve_labels(self.cfg.exclude_labels, labels)
        return labels,include,exclude

    async def messages(self, ids, format='raw'):
        "Fetch `ids` from Gmail in batches of `batch_size`, yielding each batch as a list of message dicts; a 404 drops that message"
        ids = list(ids)
        done,reported = 0,-1
        def emit():
            nonlocal reported
            if done != reported:
                self.progress(done, len(ids))
                reported = done
        async def report():
            while True:
                await asyncio.sleep(self.progress_seconds)
                emit()
        reporter = asyncio.create_task(report()) if ids and self.progress and self.progress_seconds else None
        if reporter: emit()
        fields = 'id,threadId,labelIds,snippet,historyId,internalDate,sizeEstimate,raw' if format == 'raw' else 'id,labelIds'
        try:
            for i in range(0, len(ids), self.batch_size):
                mids = ids[i:i+self.batch_size]
                calls = [dict(user_id='me', id=mid, format=format, fields=fields) for mid in mids]
                values = await self.gmail.users.messages.get.batch(calls, chunk=self.batch_size, return_exceptions=True, n_retries=8)
                batch = []
                for value in values:
                    if isinstance(value, APIError):
                        if value.status_code == 404: continue
                        raise value
                    if format == 'raw':
                        raw = value.get('raw', '')
                        value['raw'] = urlsafe_b64decode(raw + '='*(-len(raw)%4))
                    batch.append(value)
                done += len(mids)
                yield batch
        finally:
            if reporter:
                reporter.cancel()
                with suppress(asyncio.CancelledError): await reporter
                emit()

    async def listed_ids(self, floor):
        after = max(0, int(floor)//1000-1)
        ids = set()
        async for page in self.gmail.users.messages.list.pages(user_id='me', q=f'after:{after}', include_spam_trash=True,
                max_results=500, fields='messages(id),nextPageToken'):
            ids.update(x['id'] for x in page.get('messages', []))
        return ids

    async def _store(self, ids, format, include, exclude, floor=None):
        "Fetch `ids` in `format` and write each batch as it arrives; returns (ids Gmail returned, ids kept)"
        seen,kept = set(),set()
        async for batch in self.messages(ids, format):
            with self.store.db:
                for msg in batch:
                    seen.add(msg['id'])
                    if not keep(msg, include, exclude, floor): continue
                    kept.add(msg['id'])
                    if format == 'raw': self.store.put(msg)
                    else: self.store.put_labels(msg['id'], msg.get('labelIds', []))
        return seen,kept

    async def reconcile(self):
        profile = await self.gmail.users.get_profile(user_id='me')
        highwater = profile['historyId']
        labels,include,exclude = await self.labels()
        floor = self.store.state('sync_floor')
        if floor is None:
            floor = int((datetime.now(timezone.utc) - timedelta(days=self.cfg.initial_lookback_days)).timestamp()*1000)
        floor = int(floor)
        listed,local,managed = await self.listed_ids(floor),self.store.ids(),self.store.ids(floor)
        candidates = listed - (local-managed)
        _,raw_kept = await self._store(candidates-local, 'raw', include, exclude, floor)
        _,min_kept = await self._store(candidates&managed, 'minimal', include, exclude)
        removed = managed - (raw_kept|min_kept)
        with self.store.db:
            self.store.labels(labels)
            n_removed = self.store.delete(removed)
            self.store.set_state('sync_floor', floor)
            self.store.set_state('history_id', highwater)
            self.store.set_state('selection', _fingerprint(self.cfg))
        return len(raw_kept)+len(min_kept),n_removed

    async def incremental(self):
        start = self.store.state('history_id')
        if start is None: return await self.reconcile()
        labels,include,exclude = await self.labels()
        changed,added,deleted,highwater = set(),set(),set(),start
        try:
            fields = ('history(messages/id,messagesAdded/message/id,messagesDeleted/message/id,'
                'labelsAdded/message/id,labelsRemoved/message/id),nextPageToken,historyId')
            async for page in self.gmail.users.history.list.pages(user_id='me', start_history_id=start, max_results=500, fields=fields):
                c,a,d = changes(page.get('history', []))
                changed.update(c)
                added.update(a)
                deleted.update(d)
                highwater = page.get('historyId', highwater)
        except APIError as e:
            if e.status_code == 404: raise HistoryExpired from e
            raise
        floor = int(self.store.state('sync_floor'))
        local,managed = self.store.ids(),self.store.ids(floor)
        deleted_only = deleted-added
        current = (changed-deleted_only) - (local-managed)
        raw_ids = (added&current) | (current-local)
        minimal_ids = (current&managed) - raw_ids
        raw_seen,raw_kept = await self._store(raw_ids, 'raw', include, exclude, floor)
        min_seen,min_kept = await self._store(minimal_ids, 'minimal', include, exclude)
        missing = ((raw_ids-raw_seen) | (minimal_ids-min_seen)) & managed
        removed = (deleted_only&managed) | missing | ((raw_seen&managed)-raw_kept) | (min_seen-min_kept)
        with self.store.db:
            self.store.labels(labels)
            n_removed = self.store.delete(removed)
            self.store.set_state('history_id', highwater)
            self.store.set_state('selection', _fingerprint(self.cfg))
        return len(raw_kept)+len(min_kept),n_removed

    async def reconcile_catchup(self):
        "Reconcile the managed set, then replay changes since its captured high-water mark"
        updated,removed = await self.reconcile()
        inc_updated,inc_removed = await self.incremental()
        return updated+inc_updated,removed+inc_removed

    async def sync(self):
        fresh = self.store.state('history_id') is None or self.store.state('selection') != _fingerprint(self.cfg)
        if fresh: return await self.reconcile_catchup()
        try: return await self.incremental()
        except HistoryExpired: return await self.reconcile_catchup()

    def close(self): self.store.close()


async def sync_once(cfg, progress=None, progress_seconds=10):
    mirror = GmailMirror(cfg, progress=progress, progress_seconds=progress_seconds)
    try:
        await mirror.connect()
        return await mirror.sync()
    finally: mirror.close()


async def follow(cfg, progress=None, progress_seconds=10):
    mirror = GmailMirror(cfg, progress=progress, progress_seconds=progress_seconds)
    try:
        await mirror.connect()
        while True:
            changed,removed = await mirror.sync()
            print(f'Synced {cfg.account}: {changed} updated, {removed} removed')
            await asyncio.sleep(cfg.poll_seconds)
    finally: mirror.close()
