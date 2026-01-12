// Query: Similar Tenders
// Returns tenders similar to a given tender based on shared keywords and requirements
// Parameter: $tender_name - Name of the reference tender to find similar tenders for
// Parameter: $limit - Maximum number of similar tenders to return (default: 10)
// Parameter: $include_category - Whether to include category similarity (default: false)

// Match the reference tender
MATCH (ref:Tender {name: $tender_name})

// Find other tenders that share keywords or requirements
MATCH (ref)-[:HAS_KEYWORD|HAS_REQUIREMENT]->(shared)<-[:HAS_KEYWORD|HAS_REQUIREMENT]-(other:Tender)
WHERE ref <> other

// Optionally include category similarity
OPTIONAL MATCH (ref)-[:IN_CATEGORY]->(cat:Category)<-[:IN_CATEGORY]-(other)

// Aggregate shared terms by type
WITH ref, other,
  // Collect shared keywords
  [k IN collect(DISTINCT CASE WHEN shared:Keyword THEN shared.name ELSE null END) WHERE k IS NOT NULL] AS shared_keywords,
  // Collect shared requirements
  [r IN collect(DISTINCT CASE WHEN shared:Requirement THEN shared.name ELSE null END) WHERE r IS NOT NULL] AS shared_requirements,
  // Check if category matches
  count(DISTINCT cat) > 0 AS same_category,
  // Calculate total overlap count
  count(DISTINCT shared) AS overlap_count

// Calculate similarity score (keywords + requirements, with optional category boost)
WITH ref, other, shared_keywords, shared_requirements, same_category,
  overlap_count,
  overlap_count + (CASE WHEN coalesce($include_category, false) AND same_category THEN 1 ELSE 0 END) AS similarity_score

// Get chunk metadata for display
OPTIONAL MATCH (other)-[:MENTIONS]-(c:Chunk)

// Return results ordered by similarity score
WITH
  other.name AS tender_name,
  c.tender_no AS tender_no,
  c.category AS category,
  c.awarded_amt AS awarded_amt,
  similarity_score,
  overlap_count,
  shared_keywords,
  shared_requirements,
  same_category
ORDER BY similarity_score DESC, tender_name
LIMIT toInteger(coalesce($limit, 10))

RETURN
  tender_name,
  tender_no,
  category,
  awarded_amt,
  similarity_score,
  overlap_count,
  shared_keywords,
  shared_requirements,
  same_category
