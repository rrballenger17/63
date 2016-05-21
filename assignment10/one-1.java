

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (n) DETACH DELETE n"}]}'


curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"CREATE (matrix1:Movie {props}) RETURN matrix1", "parameters" : { "props" : {"title" : "The Matrix", "year" : "1999-03-31"}}}]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"CREATE (matrix2:Movie {props}) RETURN matrix2", "parameters" : { "props" : {"title" : "The Matrix Reloaded", "year" : "2003-05-07"}}}]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"CREATE (matrix3:Movie {props}) RETURN matrix3", "parameters" : { "props" : {"title" : "The Matrix Revolutions", "year" : "2003-10-27"}}}]}'



curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"CREATE (keanu:Actor {props}) RETURN keanu", "parameters" : { "props" : {"name" : "Keanu Reeves"}}}]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"CREATE (laurence:Actor {props}) RETURN laurence", "parameters" : { "props" : {"name" : "Laurence Fishburne"}}}]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"CREATE (carrieanne:Actor {props}) RETURN carrieanne", "parameters" : { "props" : {"name" : "Carrie-Anne Moss"}}}]}'



curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Keanu Reeves\"}), (m:Movie{title:\"The Matrix\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Neo"} } } ]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Keanu Reeves\"}), (m:Movie{title:\"The Matrix Reloaded\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Neo"} } } ]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Keanu Reeves\"}), (m:Movie{title:\"The Matrix Revolutions\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Neo"} } } ]}'



curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Laurence Fishburne\"}), (m:Movie{title:\"The Matrix\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Morpheus"} } } ]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Laurence Fishburne\"}), (m:Movie{title:\"The Matrix Reloaded\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Morpheus"} } } ]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Laurence Fishburne\"}), (m:Movie{title:\"The Matrix Revolutions\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Morpheus"} } } ]}'



curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Carrie-Anne Moss\"}), (m:Movie{title:\"The Matrix\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Trinity"} } } ]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Carrie-Anne Moss\"}), (m:Movie{title:\"The Matrix Reloaded\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Trinity"} } } ]}'

curl -i -H accept:application/json --user neo4j:rrb123 -H content-type:application/json -XPOST http://localhost:7474/db/data/transaction/commit -d '{"statements": [ {"statement":"MATCH (a:Actor{name:\"Carrie-Anne Moss\"}), (m:Movie{title:\"The Matrix Revolutions\"}) CREATE (a)-[r:ACTS_IN {props}]->(m)", "parameters" : { "props" : {"role" : "Trinity"} } } ]}'



