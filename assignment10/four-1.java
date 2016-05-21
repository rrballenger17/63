
#actors
match (a:Actor) return ID(a) as actorId, a.name as name;

#movies
match (m:Movie) return ID(m) as movieId, m.title as title, m.year as year;

#directors
match (a:Director) return ID(a) as directorId, a.name as name;


#acting
match (p:Actor)-[r]->(m:Movie) return ID(p) as actorId, ID(m) as movieId, r.role as role;

#directing
match (p:Director)-[r]->(m:Movie) return ID(p) as directorId, ID(m) as movieId, r.role as role;


#####

LOAD CSV WITH HEADERS FROM "file:///actors.csv" AS line CREATE (a:Actor { id:line.actorId,name:line.name});

LOAD CSV WITH HEADERS FROM "file:///movies.csv" AS test CREATE (m:Movie { id:test.movieId,title:test.title, year:test.year});

LOAD CSV WITH HEADERS FROM "file:///directors.csv" AS test CREATE (d:Director { id:test.directorId,name:test.name});

LOAD CSV WITH HEADERS FROM "file:///acting.csv" AS line 
MATCH (m:Movie { id:line.movieId })
MATCH (a:Actor { id:line.actorId })
CREATE (a)-[:ACTS_IN { role:line.role}]->(m);

LOAD CSV WITH HEADERS FROM "file:///directing.csv" AS line 
MATCH (m:Movie { id:line.movieId })
MATCH (a:Director { id:line.directorId })
CREATE (a)-[:DIRECTS { role:line.role}]->(m);

match(n) remove n.id;







