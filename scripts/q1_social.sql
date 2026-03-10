SELECT 
    f.friend1 AS potential_target_user,
    COUNT(*) AS friends_who_purchased
FROM fact_friendships f
JOIN fact_messages m
    ON f.friend2 = m.user_id
WHERE m.is_purchased = TRUE
GROUP BY f.friend1
ORDER BY friends_who_purchased DESC
LIMIT 10;
