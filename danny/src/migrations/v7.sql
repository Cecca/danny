BEGIN TRANSACTION;

INSERT INTO enum_kind (kind_id, kind)
VALUES 
    (4, "CandidatePairs");

PRAGMA user_version = 7;

END TRANSACTION;