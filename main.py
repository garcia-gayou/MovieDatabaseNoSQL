import os
import requests
import pymongo
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# TMDb API configuration
api_key = os.getenv('TMDB_API_KEY')
base_url = 'https://api.themoviedb.org/3'

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = mongo_client["tmdb_db"]
collection = db["movies"]

# Neo4j connection
neo4j_uri = "bolt://neo4j:7687"
neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", "password"))

# Function to get movie details from TMDb
def get_movie_details(movie_id):
    url = f"{base_url}/movie/{movie_id}?api_key={api_key}&language=en-US"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

# Function to get movie credits from TMDb
def get_movie_credits(movie_id):
    url = f"{base_url}/movie/{movie_id}/credits?api_key={api_key}"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None

# Function to load movie data into MongoDB, storing only actor and genre IDs
def load_data_to_mongodb(pages=5):
    for page in range(1, pages + 1):
        url = f"{base_url}/movie/popular?api_key={api_key}&language=en-US&page={page}"
        response = requests.get(url)
        if response.status_code == 200:
            movies = response.json()['results']
            for movie in movies:
                movie_details = get_movie_details(movie['id'])
                if movie_details:
                    # Fetch credits and extract only actor IDs
                    credits = get_movie_credits(movie['id'])
                    actor_ids = [actor['id'] for actor in credits.get('cast', [])] if credits else []
                    movie_details['actor_ids'] = actor_ids

                    # Extract only genre IDs
                    genre_ids = [genre['id'] for genre in movie_details.get('genres', [])]
                    movie_details['genre_ids'] = genre_ids

                    # Insert movie details into MongoDB
                    collection.update_one({'id': movie_details['id']}, {'$set': movie_details}, upsert=True)
                    print(f"Inserted/Updated movie {movie_details['title']} in MongoDB")

                    # Insert actors and genres into MongoDB if they don't exist
                    for actor in credits.get('cast', []):
                        collection.update_one({'type': 'actor', 'id': actor['id']}, {'$setOnInsert': {'type': 'actor', 'id': actor['id'], 'name': actor['name']}}, upsert=True)
                    for genre in movie_details.get('genres', []):
                        collection.update_one({'type': 'genre', 'id': genre['id']}, {'$setOnInsert': {'type': 'genre', 'id': genre['id'], 'name': genre['name']}}, upsert=True)
        else:
            print("Error fetching movies data")

# Function to load data into Neo4j and create relationships
def load_data_to_neo4j():
    with neo4j_driver.session() as session:
        # Load actors and genres into Neo4j
        actors = list(collection.find({'type': 'actor'}))
        genres = list(collection.find({'type': 'genre'}))

        for actor in actors:
            session.run(
                "MERGE (a:Actor {id: $id}) "
                "ON CREATE SET a.name = $name",
                id=actor['id'],
                name=actor['name']
            )
        for genre in genres:
            session.run(
                "MERGE (g:Genre {id: $id}) "
                "ON CREATE SET g.name = $name",
                id=genre['id'],
                name=genre['name']
            )

        # Load movies and create relationships with actors and genres
        for movie in collection.find({"type": {"$exists": False}}):  # Assuming movies don't have 'type'
            movie_id = movie['id']
            session.run(
                "MERGE (m:Movie {id: $id}) "
                "ON CREATE SET m.title = $title, m.release_date = $release_date, m.overview = $overview",
                id=movie_id,
                title=movie.get('title'),
                release_date=movie.get('release_date'),
                overview=movie.get('overview')
            )

            for actor_id in movie.get('actor_ids', []):
                session.run(
                    """
                    MATCH (m:Movie {id: $movie_id})
                    MATCH (a:Actor {id: $actor_id})
                    MERGE (a)-[:ACTED_IN]->(m)
                    """,
                    movie_id=movie_id,
                    actor_id=actor_id
                )
            for genre_id in movie.get('genre_ids', []):
                session.run(
                    """
                    MATCH (m:Movie {id: $movie_id})
                    MATCH (g:Genre {id: $genre_id})
                    MERGE (m)-[:BELONGS_TO]->(g)
                    """,
                    movie_id=movie_id,
                    genre_id=genre_id
                )

# Execute data loading
load_data_to_mongodb(pages=5)
load_data_to_neo4j()

print("Proceso de carga de datos completado.")

