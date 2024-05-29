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

# Función para obtener datos de películas populares
def get_popular_movies(pages=5):
    all_movies = []
    for page in range(1, pages + 1):
        url = f"{base_url}/movie/popular?api_key={api_key}&language=en-US&page={page}"
        response = requests.get(url)
        if response.status_code == 200:
            all_movies.extend(response.json()['results'])
        else:
            break
    return all_movies

# Obtener y guardar datos de películas populares en MongoDB
movies = get_popular_movies(pages=5)  # Obtiene 5 páginas de resultados (100 películas)
for movie in movies:
    collection.insert_one(movie)
    print(f"Inserted movie {movie['title']} into MongoDB")

# Conexión a Neo4j
neo4j_uri = "bolt://neo4j:7687"
neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", "password"))

# Función para crear nodos y relaciones en Neo4j
def create_movie_nodes(session, movies):
    for movie in movies:
        session.run(
            "MERGE (m:Movie {id: $id, title: $title, release_date: $release_date})",
            id=movie['id'],
            title=movie['title'],
            release_date=movie['release_date']
        )

def create_relationships(session, movies):
    for movie in movies:
        for genre in movie['genre_ids']:
            session.run(
                """
                MATCH (m:Movie {id: $movie_id})
                MERGE (g:Genre {id: $genre_id})
                MERGE (m)-[:BELONGS_TO]->(g)
                """,
                movie_id=movie['id'],
                genre_id=genre
            )

# Extracción y transformación de datos desde MongoDB
movies = list(collection.find({}))

# Carga de datos en Neo4j
with neo4j_driver.session() as session:
    create_movie_nodes(session, movies)
    create_relationships(session, movies)

print("Datos transformados y cargados en Neo4j.")
