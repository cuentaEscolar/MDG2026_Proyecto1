

//name
LOAD CSV WITH HEADERS FROM "http://localhost:8080/lotr/characters.csv" AS row 
WITH 
row.name as name

merge ( p:Character
{ 
  name: name

} )
;


CREATE CONSTRAINT name for (n:Character) REQUIRE n.name is UNIQUE;

//source,target,book,value
LOAD CSV WITH HEADERS FROM "http://localhost:8080/lotr/all_interactions.csv" AS row 

WITH (row.source) AS origen, 
     (row.target) AS destino,
     row.book as book,
     row.value as interactions 

MATCH (o:Character {name: origen})
MATCH (d:Character {name: destino})
MERGE (o)-[r:TALKS_TO]-(d)
;


