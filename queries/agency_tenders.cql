// Query: Tenders by Agency
// Returns all tenders published by a specific agency
// Parameter: $agency_name - Name of the agency to filter by

MATCH (t:Tender)-[:PUBLISHED_BY]->(a:Agency)
WHERE a.name = $agency_name
OPTIONAL MATCH (t)-[:MENTIONS]-(c:Chunk)
RETURN
  t.name AS tender_name,
  c.tender_no AS tender_no,
  c.award_date AS award_date,
  c.awarded_amt AS awarded_amt,
  c.supplier AS supplier,
  c.category AS category
ORDER BY c.award_date DESC
