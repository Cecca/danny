BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS result (
    sha              TEXT PRIMARY KEY,
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
    path             TEXT NOT NULL,

    total_time_ms    INTEGER,
    output_size      INTEGER,
    recall           REAL,
    speedup          REAL
);

CREATE VIEW IF NOT EXISTS result_recent AS
SELECT sha, code_version, date, params_sha, seed, threshold, algorithm, MAX(algorithm_version) AS algorithm_version, k, k2, sketch_bits, threads, hosts, sketch_epsilon, required_recall, 
        no_dedup, no_verify, repetition_batch, path,
        total_time_ms, output_size, recall, speedup
FROM result
GROUP BY seed, threshold, algorithm, k, k2, sketch_bits, threads, hosts, sketch_epsilon, required_recall, 
            no_dedup, no_verify, repetition_batch, path;

CREATE TABLE IF NOT EXISTS counters (
    sha       TEXT NOT NULL,
    kind      TEXT NOT NULL,
    step      INTEGER NOT NULL,
    count     INTEGER NOT NULL,
    FOREIGN KEY (sha) REFERENCES result (sha)
);

CREATE TABLE IF NOT EXISTS network (
    sha          TEXT NOT NULL,
    hostname     TEXT NOT NULL,
    interface    INTEGER NOT NULL,
    transmitted  INTEGER NOT NULL,
    received     INTEGER NOT NULL,
    FOREIGN KEY (sha) REFERENCES result (sha)
);

PRAGMA user_version = 1;

END TRANSACTION;
