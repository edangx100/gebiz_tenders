// Query: Tenders by Supplier
// Returns all tenders awarded to a specific supplier
// Parameter: $supplier_name - Name of the supplier to filter by

MATCH (t:Tender)-[:AWARDED_TO]->(s:Supplier)
WHERE s.name = $supplier_name
OPTIONAL MATCH (t)-[:MENTIONS]-(c:Chunk)
RETURN
  t.name AS tender_name,
  s.name AS supplier_name,
  c.tender_no AS tender_no,
  c.award_date AS award_date,
  c.awarded_amt AS awarded_amt,
  c.agency AS agency,
  c.category AS category
ORDER BY c.award_date DESC
