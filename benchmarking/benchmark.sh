#!/bin/bash

echo "Benchmarking PostgreSQL queries"
hyperfine --runs 5 \
"sudo -u postgres psql ecommerce_custom -f scripts/q1.sql" \
"sudo -u postgres psql ecommerce_custom -f scripts/q2.sql" \
"sudo -u postgres psql ecommerce_custom -f scripts/q3.sql"

echo "Benchmarking MongoDB queries"
hyperfine --runs 5 \
"mongosh --file scripts/q1.js" \
"mongosh --file scripts/q2.js" \
"mongosh --file scripts/q3.js"

echo "Benchmarking Neo4j queries"
hyperfine --runs 5 \
"cypher-shell -u neo4j -p 'Teeroyce@07' -f scripts/q1.cypherl" \
"cypher-shell -u neo4j -p 'Teeroyce@07' -f scripts/q2.cypherl" \
"cypher-shell -u neo4j -p 'Teeroyce@07' -f scripts/q3.cypherl"
