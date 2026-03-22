CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    pubkey_fingerprint BLOB UNIQUE,
    username TEXT NOT NULL,
    locale TEXT NOT NULL,
    session_time REAL NOT NULL DEFAULT(0)
);

CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    shortname TEXT NOT NULL UNIQUE,
    wasm BLOB NOT NULL,
    details JSON NOT NULL CHECK(json_valid(details)),
    current_version INTEGER NOT NULL DEFAULT(0),
    env_salt BLOB NOT NULL,
    env_blob BLOB NOT NULL,
    duration_seconds REAL NOT NULL DEFAULT(0)
);

CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY,
    shortname TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS replays (
    asciinema_url TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE,
    PRIMARY KEY(user_id, created_at DESC)
);

CREATE TRIGGER IF NOT EXISTS limit_replays_per_user
AFTER INSERT ON replays
BEGIN
    DELETE FROM replays
    WHERE user_id = NEW.user_id
      AND created_at NOT IN (
          SELECT created_at
          FROM replays
          WHERE user_id = NEW.user_id
          ORDER BY created_at DESC
          LIMIT 50
      );
END;

CREATE TABLE IF NOT EXISTS user_game_durations (
    user_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    duration_seconds REAL NOT NULL DEFAULT(0),
    PRIMARY KEY(user_id, game_id),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ip_bans (
    ip TEXT PRIMARY KEY,
    reason TEXT,
    expires_at INTEGER,
    inserted_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
);

CREATE INDEX IF NOT EXISTS idx_ip_bans_inserted_at ON ip_bans(inserted_at);

CREATE TABLE IF NOT EXISTS status_tickers (
    id INTEGER PRIMARY KEY,
    sort_order INTEGER NOT NULL,
    content TEXT NOT NULL,
    expires_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_status_tickers_sort_order
ON status_tickers(sort_order, id);

CREATE TABLE IF NOT EXISTS status_broadcasts (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL,
    regions TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
