from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastcore.script import call_parse

from .core import Store, db_path, settings
from .sync import follow as follow_sync, sync_once

import asyncio


@call_parse
def main(
    Account:str=None, # Gmail address; required only on first run
    token_path:Path=None, # Authorized-user JSON; otherwise use gclientid's standard path for Account
    initial_lookback_days:int=365, # Initial mirror history; the resulting absolute floor is then retained
    include_label:Annotated[str, dict(action='append')]=None, # Mirror messages with this label; repeatable, empty means all
    exclude_label:Annotated[str, dict(action='append')]=None, # Never mirror messages with this label; repeatable
    poll_seconds:int=60, # Seconds between syncs with --follow
    progress_seconds:int=10, # Seconds between fetch progress updates; 0 disables them
    follow:bool=False, # Keep polling Gmail?
):
    "Mirror Gmail into a local searchable SQLite database"
    cfg = settings(Account, token_path, initial_lookback_days, include_label, exclude_label, poll_seconds)
    progress = lambda done,total: print(f'Fetching messages: {done}/{total}')
    try:
        if follow: asyncio.run(follow_sync(cfg, progress, progress_seconds))
        else:
            changed,removed = asyncio.run(sync_once(cfg, progress, progress_seconds))
            print(f'Synced {cfg.account}: {changed} updated, {removed} removed')
            print(db_path(cfg.account))
    except KeyboardInterrupt: pass


@call_parse
def search(
    Query:str, # SQLite FTS5 query
    label:str=None, # Require this exact Gmail label name
    limit:int=20, # Maximum results
):
    "Search the local Gmail mirror"
    cfg = settings()
    with Store(db_path(cfg.account)) as store:
        for mid,stamp,sender,subject,labels,snippet in store.search(Query, label, limit):
            date = datetime.fromtimestamp(stamp/1000, timezone.utc).strftime('%Y-%m-%d')
            print(f'{date}  {sender}  {subject}\n  {labels}\n  {snippet}\n  {mid}')


@call_parse
def status():
    "Show local mirror state"
    cfg = settings()
    path = db_path(cfg.account)
    with Store(path) as store:
        count = store.db.execute('select count(*) from messages').fetchone()[0]
        print(f'Account: {cfg.account}\nMessages: {count}\nHistory ID: {store.state("history_id", "not synced")}\nDatabase: {path}')


@call_parse
def backup(
    Output:Path=None, # Destination SQLite file; defaults to the current directory
):
    "Make a consistent standalone backup of the mirror"
    cfg = settings()
    output = Output or Path(f'mailsync-{cfg.account}-{datetime.now().strftime("%Y%m%d")}.sqlite3')
    with Store(db_path(cfg.account)) as store: store.backup(output)
    print(Path(output).expanduser())
