// Query: Top Agencies by Number of Awards
// Returns agencies ranked by the number of tenders they have published
// Optional parameter: $limit - Maximum number of agencies to return (default: 10)

MATCH (t:Tender)-[:PUBLISHED_BY]->(a:Agency)
WITH a, count(t) AS tender_count
ORDER BY tender_count DESC
LIMIT toInteger(coalesce($limit, 10))
RETURN
  a.name AS agency_name,
  tender_count
ORDER BY tender_count DESC
