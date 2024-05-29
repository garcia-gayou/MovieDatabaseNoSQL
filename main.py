import os
import requests
import pymongo
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Cargar las variables de entorno
load_dotenv()

# Configuración de la API de TMDb
api_key = os.getenv('TMDB_API_KEY')
base_url = 'https://api.themoviedb.org/3'

# Conexión a MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = mongo_client["tmdb_db"]
collection = db["movies"]

# Conexión a Neo4j
neo4j_uri = "bolt://neo4j:7687"
neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", "password"))

# Función para obtener detalles de una película
def get_movie_details(movie_id):
    url = f"{base_url}/movie/{movie_id}?api_key={api_key}&language=en-US"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Función para obtener créditos de una película
def get_movie_credits(movie_id):
    url = f"{base_url}/movie/{movie_id}/credits?api_key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Función para obtener y guardar datos de películas, actores y géneros en MongoDB
def load_data_to_mongodb(pages=5):
    for page in range(1, pages + 1):
        url = f"{base_url}/movie/popular?api_key={api_key}&language=en-US&page={page}"
        response = requests.get(url)
        if response.status_code == 200:
            movies = response.json()['results']
            for movie in movies:
                movie_details = get_movie_details(movie['id'])
                if movie_details:
                    collection.insert_one(movie_details)
                    print(f"Inserted movie {movie_details['title']} details into MongoDB")

                    credits = get_movie_credits(movie['id'])
                    if credits:
                        for actor in credits['cast']:
                            collection.insert_one({'type': 'actor', 'id': actor['id'], 'name': actor['name']})
                            actor_ids = [actor['id'] for actor in credits.get('cast', [])] if credits else []
                            # Add actor_ids to the movie details
                            movie_details['actor_ids'] = actor_ids
                        for genre in movie_details['genres']:
                            collection.insert_one({'type': 'genre', 'id': genre['id'], 'name': genre['name']})
        else:
            print("Error fetching movies data")

# Función para crear nodos y relaciones en Neo4j
def load_data_to_neo4j():
    actors = list(collection.find({'type': 'actor'}))
    genres = list(collection.find({'type': 'genre'}))

    with neo4j_driver.session() as session:
        for actor in actors:
            session.run(
                "MERGE (a:Actor {id: $id, name: $name})",
                id=actor['id'],
                name=actor['name']
            )
        for genre in genres:
            session.run(
                "MERGE (g:Genre {id: $id, name: $name})",
                id=genre['id'],
                name=genre['name']
            )

        for movie in collection.find({}):
            movie_id = movie['id']
            for actor in collection.find({'type': 'actor'}):
                actor_id = actor['id']
                session.run(
                    """
                    MATCH (m:Movie {id: $movie_id})
                    MATCH (a:Actor {id: $actor_id})
                    MERGE (a)-[:ACTED_IN]->(m)
                    """,
                    movie_id=movie_id,
                    actor_id=actor_id
                )

# Ejecutar la carga de datos
load_data_to_mongodb(pages=5)
load_data_to_neo4j()

print("Proceso de carga de datos completado.")