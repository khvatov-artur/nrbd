import requests
import time
from neo4j import GraphDatabase
import json

class KinopoiskAPI:  
    def __init__(self, api_key, request_history_file):
        self.api_key = api_key
        self.request_count = 0  # Счетчик запросов
        self.request_history_file = request_history_file
        self.cache = {}  # Кеш для хранения ответов от API

    def make_request(self, url):
        if url in self.cache:
            # Возвращаем закешированный ответ, если доступен
            return self.cache[url]

        headers = {
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json',
        }
        response = requests.get(url, headers=headers)
        self.request_count += 1  # Увеличиваем счетчик при каждом запросе
        print(self.request_count)
        # Сохраняем запрос в файл
        with open(self.request_history_file, 'a') as history_file:
            history_file.write(f"{self.request_count}: {url}\n")
        # Кешируем ответ для будущего использования
        self.cache[url] = response.json()
        return self.cache[url]
    
    def get_movie_info(self, movie_id):
        url = f'https://kinopoiskapiunofficial.tech/api/v2.2/films/{movie_id}'
        return self.make_request(url)

    def get_movie_cast(self, movie_id):
        url = f'https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={movie_id}'
        return self.make_request(url)

    def get_actor_info(self, actor_id):
        url = f'https://kinopoiskapiunofficial.tech/api/v1/staff/{actor_id}'
        return self.make_request(url)

    def process_movie_cast_recursive(self, movie_id, depth=2):
            cast = self.get_movie_cast(movie_id)

            for actor in cast:
                if 'staffId' in actor:
                    actor_id = actor['staffId']
                elif 'personId' in actor:
                    actor_id = actor['personId']
                else:
                    continue

                self.get_actor_info_recursive(actor_id, depth-1)

    def get_actor_info_recursive(self, actor_id, depth=2):
        actor_info = self.get_actor_info(actor_id)
        neo4j_handler.create_actor_node(actor_info)
        # time.sleep(3)  # Добавлено ожидание между запросами

        # Рекурсивный вызов для обработки фильмов актера
        if depth > 0 and 'films' in actor_info:
            for film in actor_info['films']:
                film_id = film['filmId']
                film_info = self.get_movie_info(film_id)
                neo4j_handler.create_movie_node(film_info)
                # time.sleep(3)

    def run(self, movie_id):
        print("Fetching movie info...")
        movie_info = self.get_movie_info(movie_id)
        neo4j_handler.create_movie_node(movie_info)
        print("Movie info processed.")

        print("Processing movie cast...")
        self.process_movie_cast_recursive(movie_id)
        print("Movie cast processed.")


class Neo4jHandler:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def create_movie_node(self, movie_data):
        with self._driver.session() as session:
            existing_node = session.execute_read(self._get_movie_node, movie_data['kinopoiskId'])
            if not existing_node:
                session.execute_write(self._create_movie_node, movie_data)

    def create_actor_node(self, actor_data):
        with self._driver.session() as session:
            existing_node = session.execute_read(self._get_actor_node, actor_data['personId'])
            if not existing_node:
                session.execute_write(self._create_actor_node, actor_data)

    @staticmethod
    def _get_movie_node(tx, kinopoisk_id):
        query = "MATCH (m:Movie { kinopoiskId: $kinopoiskId }) RETURN m"
        result = tx.run(query, kinopoiskId=kinopoisk_id)
        return result.single()

    @staticmethod
    def _create_movie_node(tx, movie_data):
        query = (
            "CREATE (m:Movie { "
            "kinopoiskId: $kinopoiskId, "
            "nameRu: $nameRu, "
            "posterUrl: $posterUrl"
            "}) RETURN m"
        )
        tx.run(query, **movie_data)

    @staticmethod
    def _get_actor_node(tx, person_id):
        query = "MATCH (a:Actor { personId: $personId }) RETURN a"
        result = tx.run(query, personId=person_id)
        return result.single()

    @staticmethod
    def _create_actor_node(tx, actor_data):
        query = (
            "CREATE (a:Actor { "
            "personId: $personId, "
            "nameRu: $nameRu, "
            "posterUrl: $posterUrl"
            "}) RETURN a"
        )
        tx.run(query, **actor_data)


# Инициализация
# api_key = '4127c378-fbdb-4b66-a529-0216213f3f88'
api_key =   "7f523596-9895-438b-ae1e-a1df025cee47"
neo4j_uri = "neo4j+s://8b2acce1.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "S9lpswz5O41Isk8XFtaOG4V1kRRiow0wOth4sVNsw3A"

history_file = "request_history.txt"
kinopoisk_api = KinopoiskAPI(api_key, history_file)
# kinopoisk_api = KinopoiskAPI(api_key)
neo4j_handler = Neo4jHandler(neo4j_uri, neo4j_user, neo4j_password)

# Запуск
movie_id = 301  # ID фильма "Матрица"

try:
    kinopoisk_api.run(movie_id)
except Exception as e:
    print(f"An error occurred: {e}")

# Завершение
neo4j_handler.close()
