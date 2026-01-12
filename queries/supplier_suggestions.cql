// Query: Supplier suggestions
// Returns suppliers with names that normalized-match the input
// Parameters:
// - $supplier_name: Supplier name input to match
// - $limit: Maximum number of suggestions to return

MATCH (s:Supplier)
WITH s,
  // Normalize names to improve matching on partial or punctuation-variant input.
  replace(
    replace(
      replace(
        replace(
          replace(
            replace(
              replace(
                replace(toLower(s.name), ".", ""),
                ",",
                ""
              ),
              "'",
              ""
            ),
            "(",
            ""
          ),
          ")",
          ""
        ),
        "-",
        ""
      ),
      "/",
      ""
    ),
    " ",
    ""
  ) AS normalized_name,
  replace(
    replace(
      replace(
        replace(
          replace(
            replace(
              replace(
                replace(toLower($supplier_name), ".", ""),
                ",",
                ""
              ),
              "'",
              ""
            ),
            "(",
            ""
          ),
          ")",
          ""
        ),
        "-",
        ""
      ),
      "/",
      ""
    ),
    " ",
    ""
  ) AS normalized_query
WHERE normalized_name CONTAINS normalized_query
RETURN s.name AS supplier_name
ORDER BY s.name
LIMIT $limit
