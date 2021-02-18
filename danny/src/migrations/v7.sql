BEGIN TRANSACTION;

INSERT INTO enum_kind (kind_id, kind)
VALUES 
    (4, "CandidatePairs")
    (5, "SimilarityDiscarded");

PRAGMA user_version = 7;

END TRANSACTION;