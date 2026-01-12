// Query: Top Suppliers by Total Awarded Amount
// Returns suppliers ranked by the total value of awards they have received
// Optional parameter: $limit - Maximum number of suppliers to return (default: 10)

MATCH (t:Tender)-[:AWARDED_TO]->(s:Supplier)
OPTIONAL MATCH (t)-[:MENTIONS]-(c:Chunk)
WITH s, sum(toFloat(coalesce(c.awarded_amt, 0))) AS total_amount, count(t) AS tender_count
WHERE total_amount > 0
ORDER BY total_amount DESC
LIMIT toInteger(coalesce($limit, 10))
RETURN
  s.name AS supplier_name,
  total_amount,
  tender_count
ORDER BY total_amount DESC
