CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    pubkey_fingerprint BLOB UNIQUE,
    username TEXT NOT NULL,
    locale TEXT NOT NULL,
    session_time INTEGER NOT NULL DEFAULT(0)
);

CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    shortname TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    details JSON NOT NULL CHECK(json_valid(details))
);

CREATE TABLE IF NOT EXISTS envs (
    game_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(game_id, name),
    FOREIGN KEY(game_id) REFERENCES games(id)
);

CREATE TABLE IF NOT EXISTS replays (
    asciinema_url TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(game_id) REFERENCES games(id),
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
