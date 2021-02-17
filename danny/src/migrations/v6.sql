BEGIN TRANSACTION;

DROP TABLE profile;
DELETE FROM result WHERE profile_frequency > 0;

CREATE TABLE profile (
    id         INTEGER NOT NULL,
    hostname   TEXT NOT NULL,
    thread     TEXT NOT NULL,
    name       TEXT NOT NULL,
    frame_count      INT NOT NULL,
    FOREIGN KEY (id) REFERENCES result (id)
);

PRAGMA user_version = 6;

END TRANSACTION;