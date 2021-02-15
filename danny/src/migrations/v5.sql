BEGIN TRANSACTION;

CREATE TABLE profile (
    id         INTEGER NOT NULL,
    hostname   TEXT NOT NULL,
    thread     TEXT NOT NULL,
    name       TEXT NOT NULL,
    frame_total      INT NOT NULL,
    frame_count      INT NOT NULL,
    FOREIGN KEY (id) REFERENCES result (id)
);

ALTER TABLE result ADD COLUMN profile_frequency INTEGER DEFAULT 0;

PRAGMA user_version = 5;

END TRANSACTION;