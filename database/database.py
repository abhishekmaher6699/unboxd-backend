import sqlite3
import json
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

db = "movies.db"

def create_static_table() -> None:
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
                CREATE TABLE IF NOT EXISTS staticData (
                name TEXT PRIMARY KEY,
                title TEXT,
                tmdb_id INTEGER,
                release_date TEXT,
                countries TEXT ,
                spoken_languages TEXT,
                original_language TEXT,
                genres TEXT,
                runtime INTEGER,
                actors TEXT,
                director TEXT,
                themes TEXT,
                nanogenres TEXT
                )
            '''
        )

def fetch_static_row(name: str) -> Optional[Tuple]:
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM staticData WHERE name = ?', (name,))
        return cursor.fetchone()

def insert_into_static(data: Tuple) -> None:
    
    name, tmdb_data, actors, dir, themes, nanogenres = data
    try:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO staticData (
                    name, title, tmdb_id, release_date, countries, spoken_languages, original_language,
                    genres, runtime, actors, director, themes, nanogenres
                ) VALUES (?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?)
                ''', (
                    name, 
                    tmdb_data['title'],
                    tmdb_data['tmdb_id'],
                    tmdb_data['release_date'],
                    json.dumps(tmdb_data['countries']),
                    json.dumps(tmdb_data['spoken_languages']),
                    tmdb_data['og_lang'],
                    json.dumps(tmdb_data['genres']),
                    tmdb_data['runtime'],
                    json.dumps(actors),
                    dir,
                    json.dumps(themes),
                    json.dumps(nanogenres),

                ))
    except sqlite3.IntegrityError:
        print(f"Entry with name '{name}' already exists. Skipping insert.")
    except Exception as e:
        print(f"error {e} for {name}")
    

def create_semistatic_table() -> None:
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
                CREATE TABLE IF NOT EXISTS semi_static_data (
                name TEXT PRIMARY KEY,
                timestamp DATE,
                watched_by INTEGER,
                liked_by INTEGER,
                top250 INTEGER,
                avg_rating FLOAT,
                rating_count INTEGER
                )
            '''
        )

def does_data_exists(name:str) -> Optional[Tuple]:
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM semi_static_data WHERE name = ?", (name,))
        return cursor.fetchone()

def fetch_semistatic_row(name:str) -> Optional[Tuple]:
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM semi_static_data WHERE name = ?", (name,))
        return cursor.fetchone()

def insert_into_semistatic(data:Tuple) -> None:
    name, ratings, stats, time = data
    try:
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO semi_static_data (
                    name, timestamp, watched_by, liked_by, top250, avg_rating, rating_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (name, time, stats['icon-watched'], stats['icon-liked'], stats['icon-top250'], ratings['rating'], ratings['count'])
            )
    except sqlite3.IntegrityError:
        print(f"Entry with name '{name}' already exists. Skipping insert.")

def update_semistatic(data:Tuple) -> None:
    name, ratings, stats, time = data
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE semi_static_data 
            SET timestamp = ?, watched_by = ?, liked_by = ?, top250 = ?, avg_rating = ?, rating_count = ?
            WHERE name = ?
        ''', (time, stats['icon-watched'], stats['icon-liked'], stats['icon-top250'], ratings['rating'], ratings['count'], name)
        )


# Ranking

users_db = Path(__file__).parent / "users.db"

def create_friends_table():
    with sqlite3.connect(users_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
                CREATE TABLE IF NOT EXISTS user_data (
                name TEXT PRIMARY KEY,
                timestamp DATE,
                titles TEXT,
                links TEXT,
                ratings TEXT
                )
            '''
        )

def does_user_exist(user):
    with sqlite3.connect(users_db) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM user_data WHERE name = ?", (user,))
        return cursor.fetchone()

def fetch_user_data(user):
    with sqlite3.connect(users_db) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT titles, links, ratings FROM user_data WHERE name = ?", (user,))
        return cursor.fetchone()

def insert_user_data(data):
    name, time, titles, links, ratings = data
    try:
        with sqlite3.connect(users_db) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO user_data (
                    name, timestamp, titles, links, ratings
                ) VALUES (?, ?, ?, ?, ?)
            ''', (name, time, json.dumps(titles), json.dumps(links), json.dumps(ratings))
            )
    except sqlite3.IntegrityError:
        print(f"Entry with name '{name}' already exists. Skipping insert.")

def update_user_data(data):
    name, time, titles, links, ratings = data
    with sqlite3.connect(users_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE user_data 
            SET timestamp = ?, titles = ?, links = ?, ratings = ?
            WHERE name = ?
        ''', (time, json.dumps(titles), json.dumps(links), json.dumps(ratings), name)
        )