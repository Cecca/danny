BEGIN TRANSACTION;

INSERT INTO enum_kind (kind_id, kind)
VALUES 
    (6, "SelfPairsDiscarded");

PRAGMA user_version = 8;

END TRANSACTION;