// Query: Tender Requirements Overlap
// Returns pairs of tenders that share requirements
// Parameter: $min_overlap - Minimum number of shared requirements (default: 1)
// Parameter: $limit - Maximum number of pairs to return (optional)

MATCH (t1:Tender)-[:HAS_REQUIREMENT]->(r:Requirement)<-[:HAS_REQUIREMENT]-(t2:Tender)
WHERE elementId(t1) < elementId(t2)  // Ensure each pair appears once
WITH t1, t2, collect(DISTINCT r.name) AS shared_requirements, count(DISTINCT r) AS overlap_count
WHERE overlap_count >= toInteger(coalesce($min_overlap, 1))
OPTIONAL MATCH (t1)-[:MENTIONS]-(c1:Chunk)
OPTIONAL MATCH (t2)-[:MENTIONS]-(c2:Chunk)
WITH
  t1.name AS tender1_name,
  c1.tender_no AS tender1_no,
  c1.category AS tender1_category,
  t2.name AS tender2_name,
  c2.tender_no AS tender2_no,
  c2.category AS tender2_category,
  overlap_count,
  shared_requirements
ORDER BY overlap_count DESC, tender1_name, tender2_name
LIMIT toInteger(coalesce($limit, 20))
RETURN
  tender1_name,
  tender1_no,
  tender1_category,
  tender2_name,
  tender2_no,
  tender2_category,
  overlap_count,
  shared_requirements
