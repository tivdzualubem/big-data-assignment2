SELECT
    product_id
FROM fact_events
WHERE product_id IN (
    28718136,
    12709709,
    12710984,
    12711508,
    12720525
)
LIMIT 10;
