WITH target_user AS (
    SELECT 557442625::bigint AS user_id
),
user_seen_products AS (
    SELECT DISTINCT e.product_id
    FROM fact_events e
    JOIN target_user t
      ON e.user_id = t.user_id
),
user_friends AS (
    SELECT
        CASE
            WHEN f.friend1 = t.user_id THEN f.friend2
            ELSE f.friend1
        END AS friend_user_id
    FROM fact_friendships f
    CROSS JOIN target_user t
    WHERE f.friend1 = t.user_id
       OR f.friend2 = t.user_id
),
friend_products AS (
    SELECT
        e.product_id,
        SUM(
            CASE
                WHEN e.event_type = 'purchase' THEN 5
                WHEN e.event_type = 'cart' THEN 3
                WHEN e.event_type = 'view' THEN 1
                ELSE 0
            END
        ) AS recommendation_score
    FROM user_friends uf
    JOIN fact_events e
      ON e.user_id = uf.friend_user_id
    WHERE e.product_id NOT IN (
        SELECT product_id FROM user_seen_products
    )
    GROUP BY e.product_id
)
SELECT
    fp.product_id,
    fp.recommendation_score
FROM friend_products fp
JOIN dim_products dp
  ON fp.product_id = dp.product_id
ORDER BY fp.recommendation_score DESC, fp.product_id
LIMIT 10;
