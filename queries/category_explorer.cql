// Query: Category Explorer with Keywords and Requirements
// Returns categories with their associated keywords and requirements
// Optional parameters:
// - $category_name - Filter by specific category (default: all categories)
// - $category_group - Filter by specific category group (default: all groups)

MATCH (t:Tender)-[:IN_CATEGORY]->(cat:Category)
// Keywords/requirements are captured via chunk mentions in current pipeline output.
OPTIONAL MATCH (t)<-[:MENTIONS]-(kw_chunk:Chunk)-[:MENTIONS]->(k:Keyword)
OPTIONAL MATCH (t)<-[:MENTIONS]-(req_chunk:Chunk)-[:MENTIONS]->(r:Requirement)
OPTIONAL MATCH (cat)-[:IN_CATEGORY_GROUP]->(cg:CategoryGroup)
WITH
  cat,
  collect(DISTINCT k.name) AS keywords,
  collect(DISTINCT r.name) AS requirements,
  count(DISTINCT t) AS tender_count,
  head(collect(DISTINCT cg.name)) AS category_group
WHERE
  ($category_name IS NULL OR cat.name = $category_name)
  AND ($category_group IS NULL OR category_group = $category_group)
RETURN
  cat.name AS category_name,
  category_group,
  tender_count,
  keywords,
  requirements
ORDER BY tender_count DESC
