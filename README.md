# MovieDatabaseNoSQL

Este proyecto conecta a la API de TMDb, guarda datos de películas populares en MongoDB, y luego transforma y carga esos datos en Neo4j.

## Requisitos

- Docker
- Docker Compose

## Instrucciones

1. Clona este repositorio:
    ```bash
    git clone https://github.com/tu-usuario/tu-repositorio.git
    cd tu-repositorio
    ```

2. Crea un archivo `.env` con tu API Key de TMDb:
    ```env
    API_KEY=TU_API_KEY
    ```

3. Ejecuta Docker Compose:
    ```bash
    docker-compose up
    ```

4. Los datos se cargarán en MongoDB y Neo4j automáticamente.
