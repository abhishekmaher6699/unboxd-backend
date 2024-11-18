import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import json
import requests
from pydantic import BaseModel
from typing import List, Optional, Tuple, Dict, Any, Union
from datetime import datetime
from tqdm.asyncio import tqdm
from dotenv import load_dotenv
import os
from database.database import (create_static_table, insert_into_static, 
                      fetch_static_row, create_semistatic_table, does_data_exists, 
                      fetch_semistatic_row, insert_into_semistatic, 
                      update_semistatic)

load_dotenv()

class UserMovieCountError(ValueError):
    status_code = 400

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class MovieData(BaseModel):
    title: str = 'Unknown'
    tmdb_id: Optional[int] = None
    release_date: str = "Unknown"
    countries: List[str] = []
    spoken_languages: List[str] = []
    original_language: str = "Unknown"
    runtime: Optional[int] = "Unknown"
    genres: List[str] = []
    actors: List[str] = []
    director: str = "Unknown"
    themes: List[str] = []
    nanogenres: List[str] = []
    last_watched: str
    is_rewatched: bool
    rating: Optional[float] = "Unknown"
    rating_count: Optional[int] = "Unknown"
    stats_watched: int = 0
    stats_liked: int = 0
    stats_rank: int = 0
    user_rating: float = 0
    is_liked: bool = False
    is_reviewed: bool = False

class MovieDataScraper:

    def __init__(self, user):
        self.user = user
        self.TMDB_KEY = os.getenv('TMDB_KEY')
        create_static_table()
        create_semistatic_table()

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> Union[dict, str, None]:
        max_attempts = 3
        base_wait_time = 1
        max_wait_time = 10
        timeout = 10

        for attempt in range(max_attempts):
            try:
                async with session.get(url,timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    # print(f"Fetching {url} (Attempt {attempt + 1}/{max_attempts})")
                    if 'api.themoviedb.org' in url:
                        return await response.json()
                    else:
                        return await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if isinstance(e, aiohttp.ClientError):
                    print(f"Client error while fetching {url}: {e}")
                else:
                    print(f"Timeout error while fetching {url}")
                
                if attempt < max_attempts - 1:
                    wait_time = min(base_wait_time * (2 ** attempt), max_wait_time)
                    print(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"Max attempts reached. Fetch failed for {url}")
            except Exception as e:
                print(f"An error occurred while fetching {url}: {e}")
                break  # Exit the retry loop for unexpected errors

        return None

    async def extract_movie_links(self, html:str) -> Tuple[List[str], List[bool], List[bool], List[Optional[float]]]:
        titles = []
        user_ratings = []
        liked = []
        reviews = []
        soup = BeautifulSoup(html, 'lxml')

        for li in soup.find_all('li', class_="poster-container"):
            poster_div = li.find('div', class_="poster")
            if poster_div:
                link = poster_div.get('data-target-link')
                if link:
                    titles.append(link)

            rating = li.find('span', class_='rating')
            if rating:
                match = re.search(r"rated-(\d+)", rating['class'][-1])
                number = int(match.group(1)) / 2
                user_ratings.append(number)
            else:
                user_ratings.append(0)

            is_liked = li.find('span', class_='like liked-micro has-icon icon-liked icon-16')
            liked.append(True if is_liked else False)
            
            is_reviewed = li.find('a', class_='review-micro has-icon icon-review tooltip')
            reviews.append(True if is_reviewed else False)

        return titles, reviews, liked, user_ratings

    async def extract_watch_activity(self, session: aiohttp.ClientSession, name: str) -> Tuple[Optional[str], bool]:

        url = f"https://letterboxd.com/{self.user}/film/{name}/activity/"
        html= await self.fetch(session, url)    

        soup = BeautifulSoup(html, 'lxml')
        words_to_check = {"watched", "watched,", "reviewed", "reviewed,", "rewatched", "rewatched,"}
        rewatch_words = {"rewatched", "rewatched,"}
        activities = soup.find_all(class_="activity-row -basic")
        results = []
        is_rewatch = False

        for activity in activities:
            text = activity.get_text().lower()
            if any(word in text.split() for word in words_to_check):
                is_rewatch = is_rewatch or any(word in text.split() for word in rewatch_words)
                date_element = activity.find("span", class_="nobr")
                if date_element:
                    date = date_element.get_text()
                    date = datetime.strptime(date, "%b %d, %Y")
                else:
                    date = activity.find('time')['datetime']
                    date = datetime.fromisoformat(date.replace("Z", "+00:00"))
                results.append(date.strftime("%Y-%m-%d"))
        if not results:
            return None, False
        is_rewatch = is_rewatch | (len(results) > 1)
        return results[0], is_rewatch
         
    async def fetch_tmdb_details(self, movie_id: int, session: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:    
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={self.TMDB_KEY}"
        tmdb_data = await self.fetch(session, url)
        if tmdb_data:
            if 'id' in tmdb_data:
                return {
                    "tmdb_id": tmdb_data['id'],
                    "title": tmdb_data.get('title', 'Unknown'),
                    "release_date": tmdb_data.get('release_date', 'Unknown'),
                    "countries":  [country['name'] for country in tmdb_data.get('production_countries', [])],
                    "spoken_languages" : [language['english_name'] for language in tmdb_data.get('spoken_languages', [])],
                    "runtime" : tmdb_data.get('runtime', 'Unknown'),
                    "og_lang" : tmdb_data.get('original_language', 'Unknown'),
                    "genres" :  [genre['name'] for genre in tmdb_data.get('genres', [])],
                }
        else:
            print(f"Failed to fetch data for TMDb ID {movie_id}.")
            return None
        
    async def extract_stats(self, session: aiohttp.ClientSession, name: str) -> Dict[str, int]:

        url = f"https://letterboxd.com/csi/film/{name}/stats/"
        page = await self.fetch(session, url)
        soup = BeautifulSoup(page, "lxml")

        tags = ['icon-watched', 'icon-liked', 'icon-top250']
        result = {tag:0 for tag in tags}

        for tag in tags:
            if soup.find('a', class_=tag):
                text = soup.find('a', class_=tag)['title']
                match = re.search(r"\d{1,3}(?:,\d{3})*(?=\s)", text)
                if match:
                    result[tag] = int(match.group(0).replace(",", ""))
        return result
    
    async def extract_average_rating(self, session: aiohttp.ClientSession, name: str) -> Dict[str, Any]:

        url = f"https://letterboxd.com//csi/film/{name}/rating-histogram/"
        page = await self.fetch(session, url)
        soup = BeautifulSoup(page, "lxml")

        result = {}

        item = soup.find('a', class_='display-rating')
        if item:
            text = item['title']
            match = re.search(r"(\d+\.\d+)\s*based on\s*(\d{1,3}(?:,\d{3})*)\s*ratings", text)
            
            if match:
                rating = float(match.group(1))
                count = int(match.group(2).replace(",", ""))
                result['rating'] = rating
                result['count'] = count
            
        else:
            total_count = 0
            total_weighted_sum = 0
            for i, bar in enumerate(soup.find_all("li", class_="rating-histogram-bar")):
                title = bar.select("a")
                if title:
                    parts = title[0].get("title").split(' ')
                    count = int(re.match(r'^\d+', parts[0]).group()) if re.match(r'^\d+', parts[0]) else None
                    total_count += count
                    rating = (i + 1) / 2
                    total_weighted_sum += count * rating
            
            result['rating'] = round(total_weighted_sum / total_count, 2) if total_count > 0 else 0
            result['count'] = total_count
        
        # print(result)
        return result
    
    def extract_metadata(self, soup: BeautifulSoup) -> Tuple[str, List[str], List[str], Optional[str]]:

        try:
            dir = soup.find("div", id="tab-crew").find_all("a", class_="text-slug")[0].text
        except:
            dir = ""
        try:
            actors = [i.text for i in soup.find("div", class_="cast-list").find_all("a")[:3]]
        except:
            actors = []
        try:
            themes = [i.text for i in soup.find("div", id="tab-genres").find_all("div")[1].find_all("a")[:-1]]
        except:
            themes = []

        element = soup.find(class_="micro-button track-event", attrs={'data-track-action': 'TMDb'})
        if element:
            link = element['href']
            tmdb_id_match = re.search(r'/movie/(\d+)', link)
            tmdb_id =  tmdb_id_match.group(1) if tmdb_id_match else None

        return dir, actors, themes, tmdb_id

    async def extract_nanogenres(self, session: aiohttp.ClientSession, name: str) -> List[str]:
        url = f"https://letterboxd.com/film/{name}/nanogenres/"
        page = await self.fetch(session, url)
        soup = BeautifulSoup(page, "lxml")
        if soup.find_all("h2", class_="title"):
            nanogenres = list(set([i.strip() for sublist in [i.text.strip().split(",") for i in soup.find_all("h2", class_="title")] for i in sublist]))
        else:
            nanogenres = []
        return nanogenres

    async def fetch_static_data(self, session: aiohttp.ClientSession, name: str) -> Tuple[Optional[Dict[str, Any]], List[str], str, List[str], List[str]]:
   
        static_data = fetch_static_row(name)
        if static_data:
            # print("fetching data from db")
            tmdb_data = {
                'title': static_data[1],
                'tmdb_id': int(static_data[2]),
                'release_date': static_data[3],
                'countries': json.loads(static_data[4]),
                'spoken_languages': json.loads(static_data[5]),
                'og_lang': static_data[6],
                'genres': json.loads(static_data[7]),
                'runtime': int(static_data[8]),
            }
            actors = json.loads(static_data[9])
            dir = static_data[10]
            themes = json.loads(static_data[11])
            nanogenres = json.loads(static_data[12])

            return tmdb_data, actors, dir, themes, nanogenres 

        else:
            # print("using api")
            url = f"https://letterboxd.com/film/{name}"
            html = await self.fetch(session, url)  
            soup = BeautifulSoup(html, 'lxml') 

            nanogenres = await self.extract_nanogenres(session, name)
            dir, actors, themes, tmdb_id = self.extract_metadata(soup)
            tmdb_data = await self.fetch_tmdb_details(tmdb_id, session) if tmdb_id else None
            if tmdb_data:
                insert_into_static((name, tmdb_data, actors, dir, themes, nanogenres))
                pass
            return tmdb_data, actors, dir, themes, nanogenres 

    async def fetch_semistatic_data(self, session: aiohttp.ClientSession, name: str):

        async def new_data():
            ratings = await self.extract_average_rating(session, name)
            stats = await self.extract_stats(session, name)
            return ratings, stats
        
        result = does_data_exists(name) #check if the data exists in the db
        current_time = datetime.now()

        if result: #if it does
            # print("results exist")
            timestamp = datetime.fromisoformat(result[0])
            is_stale = (current_time - timestamp).days >= 5
            if is_stale: # if data is older than 2 days, update it
               ratings, stats = await new_data()
               update_semistatic((name, ratings, stats, current_time.isoformat()))
               return ratings, stats
            #    print("data updated")
            else: # if data is fresh, fetch it from db
                data = fetch_semistatic_row(name)
                stats = {
                    'icon-watched': data[2],
                    'icon-liked': data[3],
                    'icon-top250': data[4]
                }   
                ratings = {
                    'rating': data[5],
                    'count': data[6]
                } 
                # print("semi static data fetched")
                return ratings, stats
            
        else: # if data doesnt exist, insert it
            ratings, stats = await new_data()
            insert_into_semistatic((name, ratings, stats, current_time.isoformat()))
            return ratings, stats
            
    async def compile_data(self, session: aiohttp.ClientSession, link: str, review: str, like: bool, user_rating: float) -> Dict[str, Any]:
        name = link.split('/')[-2]

        static_data_task = self.fetch_static_data(session, name)
        semi_static_data_task = self.fetch_semistatic_data(session, name)

        static_data, semi_static_data = await asyncio.gather(static_data_task, semi_static_data_task)
        # Extract data from the results
        tmdb_data, actors, dir, themes, nanogenres = static_data
        ratings, stats = semi_static_data
        #user-specific data
        last_watched_date, is_rewatched = await self.extract_watch_activity(session, name)

        return {
            #static data
           'title': tmdb_data.get('title') if isinstance(tmdb_data, dict) else 'Unknown',
            'tmdb_id': tmdb_data.get('tmdb_id') if isinstance(tmdb_data, dict) else 'Unknown',
            'release_date': tmdb_data.get('release_date') if isinstance(tmdb_data, dict) else 'Unknown',
            'countries': tmdb_data.get('countries') if isinstance(tmdb_data, dict) else [],
            'spoken_languages': tmdb_data.get('spoken_languages') if isinstance(tmdb_data, dict) else [],
            'original_language': tmdb_data.get('og_lang') if isinstance(tmdb_data, dict) else [],
            'runtime': tmdb_data.get('runtime') if isinstance(tmdb_data, dict) else 'Unknown',
            'genres': tmdb_data.get('genres') if isinstance(tmdb_data, dict) else [],
            'actors': actors,
            'director': dir,
            'themes': themes,
            'nanogenres': nanogenres,
            #semi-static
            'rating': ratings.get('rating', 0),
            'rating_count': ratings.get('count', 0),
            'stats_watched': stats.get('icon-watched', 0),
            'stats_liked': stats.get('icon-liked', 0),
            'stats_rank' : stats.get('icon-top250', 0),
            #user-specific
            'last_watched': last_watched_date,
            'is_rewatched' : is_rewatched,
            'user_rating': user_rating,
            'is_liked': like,
            'is_reviewed': review,
        }

    async def start_process(self, session: aiohttp.ClientSession, url: str, batch_size=24) -> List[Dict[str, Any]]:
        html = await self.fetch(session, url)
        if not html:
            return []  # Return empty list if fetching failed
        
        movie_links, reviews, likes, user_ratings = await self.extract_movie_links(html)
        
        tasks = [self.compile_data(session, link, review, like, user_rating)
                for link, review, like, user_rating in zip(movie_links, reviews, likes, user_ratings)]
        
        batched_data = []
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_result = await asyncio.gather(*batch, return_exceptions=True)
            batch_result = [data for data in batch_result if data is not None and not isinstance(data, Exception)]
            batched_data.extend(batch_result)
        
        return batched_data

    def page_nums(self) -> int:
        url = f"https://letterboxd.com/{self.user}/films"
        html = requests.get(url)
        soup = BeautifulSoup(html.text, "lxml")
        try:
            num_pages = int(soup.find_all("li", class_="paginate-page")[-1].get_text())
        except:
            num_pages = 1
        return num_pages
    
    async def scrape(self, batch_size: int = 2) -> List['MovieData']:
        pages = self.page_nums()
        urls = [f"https://letterboxd.com/{self.user}/films/page/{i}/" for i in range(1, pages + 1)]
        all_movie_data = []

        async with aiohttp.TCPConnector(limit_per_host=3) as connector:
            async with aiohttp.ClientSession(connector=connector) as session:

                for i in range(0, len(urls), batch_size):
                    batch = urls[i:i + batch_size]
                    
                    try:
                        batch_results = await asyncio.gather(
                            *(self.start_process(session, url) for url in batch),
                            return_exceptions=True
                        )
                        
                        def result_generator():
                            for sublist in batch_results:
                                if isinstance(sublist, list):
                                    yield from sublist

                        for movie_data in result_generator():
                            try:
                                valid_data = MovieData(**movie_data)
                                all_movie_data.append(valid_data)
                                
                            except Exception:
                                continue

                        if len(all_movie_data) < 20 and i == 0:
                            raise UserMovieCountError(f"User has not watched enough movies")

                    except Exception as e:
                        print(f"Batch processing error: {e}")

        return all_movie_data