BEGIN TRANSACTION;

DROP VIEW result_recent;

CREATE VIEW latest_version AS
SELECT algorithm, MAX(algorithm_version)
FROM result
GROUP BY algorithm;

CREATE VIEW result_recent AS
SELECT r.*
FROM result as r
NATURAL JOIN latest_version;

PRAGMA user_version = 2;

END TRANSACTION;

