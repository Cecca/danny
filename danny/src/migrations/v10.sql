BEGIN TRANSACTION;

CREATE TABLE system (
    id    INTEGER NOT NULL,
    time   REAL,
    hostname   TEXT,
    cpu_user   REAL,
    cpu_system   REAL,
    net_tx   REAL,
    net_rx   REAL,
    mem_total   REAL,
    mem_used    REAL,
    FOREIGN KEY (id) REFERENCES result (id)
);

PRAGMA user_version = 10;

END TRANSACTION;