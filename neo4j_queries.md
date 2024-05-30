# Consultas de Neo4j
Las siguientes consultas se realizan a la base de datos en Neo4j.

## Géneros en los que ha participado un actor
La siguiente consulta regresa, dado un actor, los géneros de las películas en las que ha participado. En este caso analizamos a Stan Lee.

```cypher
MATCH (a:Actor {name: "Stan Lee"})-[:ACTED_IN]->(m:Movie)-[:BELONGS_TO]->(g:Genre)
RETURN a, m, g
```

## Promedio de actores por género
La siguiente consulta regresa el promedio de actores que participan en cada película según el género.

```cypher
MATCH (g:Genre)<-[:BELONGS_TO]-(m:Movie)<-[:ACTED_IN]-(a:Actor)
WITH g, m, COUNT(a) AS actorCount
WITH g, AVG(actorCount) AS avgActorsPerMovie
RETURN g.name AS genre, round(avgActorsPerMovie) AS avgActorsPerMovie
ORDER BY avgActorsPerMovie DESC
```

## Número de _Bacon_
Esta consulta calcula el número de _Bacon_, o los grados de separación entre un actor dado y una submuestra de otros actores en la base de datos. Por cuestiones de procesamiento se limita esta búsqueda a cien actores. En este caso analizamos a Samuel L. Jackson.

```cypher
MATCH (stan:Actor {name: 'Samuel L. Jackson'}), (other:Actor)
WHERE stan <> other
WITH stan, other
ORDER BY other.name ASC
LIMIT 100
MATCH p = shortestPath((stan)-[:ACTED_IN*]-(other))
RETURN other.name AS actorName, length(p)/2 AS baconNumber, p
ORDER BY baconNumber ASC
```

## Actores más alejados
La siguiente consulta calcula, para algunos actores dados, los grados de separación con los demás actores en la base de datos, para regresar el camino hacia el actor más alejado. En este caso analizamos a Scarlett Johansson, Timothée Chalamet y Bruce Willis.

```cypher
MATCH (a:Actor {name: 'Scarlett Johansson'}), (b:Actor)
WHERE a <> b
WITH b, shortestPath((a)-[:ACTED_IN*]-(b)) AS p
WHERE p IS NOT NULL
RETURN 'Scarlett Johansson' AS StartingActor, b.name AS Actor, length(p)/2 AS MovieCount, p
ORDER BY MovieCount DESC
LIMIT 1

UNION

MATCH (a:Actor {name: 'Timothée Chalamet'}), (b:Actor)
WHERE a <> b
WITH b, shortestPath((a)-[:ACTED_IN*]-(b)) AS p
WHERE p IS NOT NULL
RETURN 'Timothée Chalamet' AS StartingActor, b.name AS Actor, length(p)/2 AS MovieCount, p
ORDER BY MovieCount DESC
LIMIT 1

UNION

MATCH (a:Actor {name: 'Bruce Willis'}), (b:Actor)
WHERE a <> b
WITH b, shortestPath((a)-[:ACTED_IN*]-(b)) AS p
WHERE p IS NOT NULL
RETURN 'Bruce Willis' AS StartingActor, b.name AS Actor, length(p)/2 AS MovieCount, p
ORDER BY MovieCount DESC
LIMIT 1
```

## Actores más alejados (Solución Analítica)
En esta alternativa mostramos el camino en una lista en lugar de buscarlo visualmente.

```cypher
MATCH (a:Actor {name: 'Scarlett Johansson'}), (b:Actor)
WHERE a <> b
WITH b, shortestPath((a)-[:ACTED_IN*]-(b)) AS p
WHERE p IS NOT NULL
UNWIND [index IN range(0, size(nodes(p)) - 2) | 
  CASE 
    WHEN (nodes(p)[index]):Movie THEN [(nodes(p)[index + 1]).name, (nodes(p)[index]).title]
    ELSE NULL 
  END] AS actorMoviePair
WITH b, collect(actorMoviePair) AS actorMoviePairs
RETURN 'Scarlett Johansson' AS StartingActor, b.name AS Actor, actorMoviePairs, size(actorMoviePairs) AS MovieCount
ORDER BY MovieCount DESC
LIMIT 1

UNION

MATCH (a:Actor {name: 'Timothée Chalamet'}), (b:Actor)
WHERE a <> b
WITH b, shortestPath((a)-[:ACTED_IN*]-(b)) AS p
WHERE p IS NOT NULL
UNWIND [index IN range(0, size(nodes(p)) - 2) | 
  CASE 
    WHEN (nodes(p)[index]):Movie THEN [(nodes(p)[index + 1]).name, (nodes(p)[index]).title]
    ELSE NULL 
  END] AS actorMoviePair
WITH b, collect(actorMoviePair) AS actorMoviePairs
RETURN 'Timothée Chalamet' AS StartingActor, b.name AS Actor, actorMoviePairs, size(actorMoviePairs) AS MovieCount
ORDER BY MovieCount DESC
LIMIT 1

UNION

MATCH (a:Actor {name: 'Bruce Willis'}), (b:Actor)
WHERE a <> b
WITH b, shortestPath((a)-[:ACTED_IN*]-(b)) AS p
WHERE p IS NOT NULL
UNWIND [index IN range(0, size(nodes(p)) - 2) | 
  CASE 
    WHEN (nodes(p)[index]):Movie THEN [(nodes(p)[index + 1]).name, (nodes(p)[index]).title]
    ELSE NULL 
  END] AS actorMoviePair
WITH b, collect(actorMoviePair) AS actorMoviePairs
RETURN 'Bruce Willis' AS StartingActor, b.name AS Actor, actorMoviePairs, size(actorMoviePairs) AS MovieCount
ORDER BY MovieCount DESC
LIMIT 1
```