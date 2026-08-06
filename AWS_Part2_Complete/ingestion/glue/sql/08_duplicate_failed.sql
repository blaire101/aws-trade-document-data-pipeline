SELECT *, 'DUPLICATE_LOWER_AMOUNT' AS failure_reason FROM dedup_ranked WHERE duplicate_rank > 1
