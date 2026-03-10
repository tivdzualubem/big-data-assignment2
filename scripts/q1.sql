SELECT 
    campaign_id,
    COUNT(*) AS total_messages,
    COUNT(*) FILTER (WHERE is_purchased = true) AS purchases,
    ROUND(
        COUNT(*) FILTER (WHERE is_purchased = true) * 100.0 / COUNT(*),
        2
    ) AS purchase_rate_percent
FROM fact_messages
GROUP BY campaign_id
ORDER BY purchase_rate_percent DESC
LIMIT 10;
