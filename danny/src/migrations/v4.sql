BEGIN TRANSACTION;

-- This is not much of a migration, but more of a full reset.
-- We change the way experiments are identifies (no longer a 
-- hash, but a more concide) autoincrementing integer, and in the
-- counters table we also store the worker id

DROP TABLE counters;
DROP TABLE network;
DROP TABLE result;

CREATE TABLE result (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    code_version     TEXT NOT NULL,
    date             TEXT NOT NULL,
    params_sha       TEXT NOT NULL,
    seed             INTEGER NOT NULL,
    threshold        REAL NOT NULL,
    algorithm        TEXT NOT NULL,
    algorithm_version        INTEGER NOT NULL,
    k                INTEGER NOT NULL,
    k2               INTEGER NOT NULL,
    sketch_bits      INTEGER NOT NULL,
    threads          INTEGER NOT NULL,
    hosts            TEXT NOT NULL,
    sketch_epsilon   REAL NOT NULL,
    required_recall  REAL NOT NULL,
    no_dedup         BOOL NOT NULL,
    no_verify        BOOL NOT NULL,
    repetition_batch INTEGER,
    balance          TEXT,
    path             TEXT NOT NULL,

    total_time_ms    INTEGER,
    output_size      INTEGER,
    recall           REAL,
    speedup          REAL
);

CREATE TABLE enum_kind (
    kind_id INTEGER PRIMARY KEY,
    kind    TEXT
);
INSERT INTO enum_kind (kind_id, kind)
VALUES 
    (0, "SketchDiscarded"),
    (1, "DuplicatesDiscarded"),
    (2, "OutputPairs"),
    (3, "Load");

CREATE TABLE counters_raw (
    id       INTEGER NOT NULL,
    kind_id      INTEGER NOT NULL,
    step      INTEGER NOT NULL,
    worker    INTEGER NOT NULL,
    count     INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES result (id),
    FOREIGN KEY (kind_id) REFERENCES enum_kind (kind_id)
);

CREATE VIEW counters AS
SELECT * 
FROM counters_raw
NATURAL JOIN enum_kind;

CREATE TABLE network (
    id          INTEGER NOT NULL,
    hostname     TEXT NOT NULL,
    interface    TEXT NOT NULL,
    transmitted  INTEGER NOT NULL,
    received     INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES result (id)
);

DROP VIEW result_recent;
DROP VIEW latest_version;

CREATE VIEW latest_version AS
SELECT algorithm, MAX(algorithm_version) as algorithm_version
FROM result
GROUP BY algorithm;

CREATE VIEW result_recent AS
SELECT r.*
FROM result as r
NATURAL JOIN latest_version;


PRAGMA user_version = 4;

END TRANSACTION;