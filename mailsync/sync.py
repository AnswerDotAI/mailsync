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
        results = []
        try:
            for i in range(0, len(ids), self.batch_size):
                mids = ids[i:i+self.batch_size]
                calls = [dict(user_id='me', id=mid, format=format, fields=fields) for mid in mids]
                values = await self.gmail.users.messages.get.batch(calls, chunk=self.batch_size, return_exceptions=True, n_retries=8)
                for value in values:
                    if isinstance(value, APIError):
                        if value.status_code == 404: continue
                        raise value
                    if format == 'raw':
                        raw = value.get('raw', '')
                        value['raw'] = urlsafe_b64decode(raw + '='*(-len(raw)%4))
                    results.append(value)
                done += len(mids)
            return results
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

    async def reconcile(self):
        profile = await self.gmail.users.get_profile(user_id='me')
        highwater = profile['historyId']
        labels,include,exclude = await self.labels()
        floor = self.store.state('sync_floor')
        if floor is None:
            floor = int((datetime.now(timezone.utc) - timedelta(days=self.cfg.initial_lookback_days)).timestamp()*1000)
        listed,local,managed = await self.listed_ids(int(floor)),self.store.ids(),self.store.ids(floor)
        candidates = listed - (local-managed)
        raw = {x['id']:x for x in await self.messages(candidates-local)}
        minimal = {x['id']:x for x in await self.messages(candidates&managed, 'minimal')}
        raw = {mid:x for mid,x in raw.items() if int(x['internalDate']) >= int(floor) and selected(x.get('labelIds', []), include, exclude)}
        minimal = {mid:x for mid,x in minimal.items() if selected(x.get('labelIds', []), include, exclude)}
        remote = set(raw)|set(minimal)
        removed = managed - remote
        with self.store.db:
            self.store.labels(labels)
            n_removed = self.store.delete(removed)
            for msg in raw.values(): self.store.put(msg)
            for mid,msg in minimal.items(): self.store.put_labels(mid, msg.get('labelIds', []))
            self.store.set_state('sync_floor', floor)
            self.store.set_state('history_id', highwater)
            self.store.set_state('selection', _fingerprint(self.cfg))
        return len(remote),n_removed

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
        raw = {x['id']:x for x in await self.messages(raw_ids)}
        minimal = {x['id']:x for x in await self.messages(minimal_ids, 'minimal')}
        missing = ((raw_ids-set(raw)) | (minimal_ids-set(minimal))) & managed
        raw_keep = {mid:x for mid,x in raw.items() if int(x['internalDate']) >= floor and selected(x.get('labelIds', []), include, exclude)}
        minimal_keep = {mid:x for mid,x in minimal.items() if selected(x.get('labelIds', []), include, exclude)}
        removed = (deleted_only&managed) | missing | ((set(raw)&managed)-set(raw_keep)) | (set(minimal)-set(minimal_keep))
        with self.store.db:
            self.store.labels(labels)
            n_removed = self.store.delete(removed)
            for msg in raw_keep.values(): self.store.put(msg)
            for mid,msg in minimal_keep.items(): self.store.put_labels(mid, msg.get('labelIds', []))
            self.store.set_state('history_id', highwater)
            self.store.set_state('selection', _fingerprint(self.cfg))
        return len(raw_keep)+len(minimal_keep),n_removed

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
