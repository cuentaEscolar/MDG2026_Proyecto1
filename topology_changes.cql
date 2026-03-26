
MATCH (n:Character)-[r:TALKS_TO]->(m:Character)
CALL apoc.merge.relationship(n,
  'TALKS_TO_IN_BOOK_' + left(r.book,2),
  {},
  {},
  m ,
  {}
) YIELD rel
RETURN count(*) AS `Number of relationships merged`;
