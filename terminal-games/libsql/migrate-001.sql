CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    pubkey_fingerprint BLOB UNIQUE NOT NULL,
    username TEXT NOT NULL,
    locale TEXT NOT NULL,
    session_time INTEGER NOT NULL DEFAULT(0)
);

CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    shortname TEXT UNIQUE,
    path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS envs (
    game_id INTEGER,
    name TEXT,
    value TEXT NOT NULL,
    PRIMARY KEY(game_id, name),
    FOREIGN KEY(game_id) REFERENCES games(id)
);

CREATE TABLE IF NOT EXISTS replays (
    asciinema_id TEXT PRIMARY KEY,
    user_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id)
);
