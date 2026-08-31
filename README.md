# mailsync

`mailsync` keeps a searchable local mirror of Gmail in one SQLite file. It is a **sync, not an archive**: Gmail deletions, moves, and label changes are applied locally too. Close it for any length of time and the next run catches up from Gmail's history; if that history has expired, it safely reconciles the whole selected set.

## Start

First create a Gmail token with [gclientid](https://github.com/AnswerDotAI/gclientid):

```bash
gclientid-auth --account you@example.com --preset gmail
```

Then initialize and sync the mirror:

```bash
mailsync --account you@example.com --initial-lookback-days 365
```

This creates:

- `$XDG_CONFIG_HOME/mailsync/config.ini` — account, token override, selection, and polling settings
- `$XDG_CACHE_HOME/mailsync/you@example.com.sqlite3` — raw messages plus structured and full-text indexes

fastgws reads gclientid's standard `$XDG_CONFIG_HOME/gclientid/oauth-token-you@example.com.json` location directly. Pass `--token-path` only for a google-auth authorized-user JSON file stored elsewhere.

After setup, run `mailsync` whenever you want to catch up, or leave it polling:

```bash
mailsync
mailsync --follow
```

During larger fetches, mailsync prints a compact `fetched/total` update every 10 seconds. Change the cadence with `--progress-seconds`, or pass `--progress-seconds 0` for quiet operation.

mailsync follows Gmail's synchronization model: it downloads complete RFC 822 data only for uncached messages, refreshes cached label state with minimal responses, batches reads in groups of 50, and asks Google only for the response fields it uses. fastgws enables gzip and handles quota-aware retries for both ordinary and batched calls.

The initial lookback becomes a fixed sync floor. Mail older than that floor is outside the managed set: it is not silently aged out, updated, or deleted locally.

## Choose labels

With no includes, all messages are mirrored, including Spam and Trash. Repeat options to select labels; label names and Gmail label IDs both work. Any excluded label wins over an included label.

```bash
mailsync --account you@example.com --include-label INBOX --include-label work --exclude-label SPAM
```

Selection is stored at first setup. Edit `config.ini` later; changing it automatically triggers a full reconciliation.

## Search, inspect, and back up

```bash
mailsync-search 'sender:"alice@example.com" AND roadmap'
mailsync-search 'subject:"release notes"' --label work
mailsync-status
mailsync-backup --output mail.sqlite3
```

Search accepts SQLite FTS5 syntax. SQLite itself is also a supported interface: `messages`, `headers`, `labels`, and `message_labels` provide structured fields, while `message_fts` indexes subject, sender, recipients, CC, and decoded body text. The original decoded RFC 822 bytes are in `messages.raw`.

`mailsync-backup` uses SQLite's online backup API, so the resulting file is a consistent standalone snapshot even while the working database uses WAL.
