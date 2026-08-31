# Development notes

`mailsync` is a one-way Gmail mirror, not an archive: remote deletions, label changes, and messages leaving the configured selection are reflected locally. The raw RFC 822 message is the source of truth; normalized headers, labels, and FTS5 text are derived search indexes.

Every full reconciliation records Gmail's `historyId` before listing and fetching messages, then replays history from that high-water mark. Later syncs use `history.list`, fetch each changed message's canonical current state, and checkpoint only after applying the whole batch transactionally. An expired Gmail history cursor triggers a full ID reconciliation from the persisted absolute sync floor followed by the same high-water replay. `initial_lookback_days` therefore limits initial discovery, not ongoing retention. The floor never moves: messages do not age out, and IDs locally known to be older than the floor are outside the managed set rather than update or deletion candidates. A changed ID that Gmail now reports as missing is deleted only when its stored `internal_date` places it inside that managed set.

A history entry can name a message that disappears before `messages.get` runs. fastgws returns that batch part as `fastspec.errors.APIError`; treat a 404 as missing so the incremental transaction removes it locally. Other API errors must still propagate.

Full reconciliation lists IDs, fetches `RAW` only for uncached candidates, and uses `MINIMAL` to refresh labels for cached managed messages. Incremental synchronization retains Gmail's specific added/deleted change types: additions and uncached label changes need `RAW`, cached label changes need only `MINIMAL`, and definite deletions need no fetch. Reads use Google HTTP batches of 50 and partial `fields` selectors. fastgws applies gzip and the same quota-aware retry policy to ordinary responses and individual failed batch parts.

Configuration lives in `$XDG_CONFIG_HOME/mailsync/config.ini`; the APSW database lives in `$XDG_CACHE_HOME/mailsync/<account>.sqlite3`. The database uses WAL while active. `mailsync-backup` uses SQLite's online backup API to produce a consistent portable single file.

API orchestration is tested against a real disposable Gmail account. Unit tests are reserved for MIME/index behavior and pure reconciliation rules; do not build mock-heavy tests of Google's API.
