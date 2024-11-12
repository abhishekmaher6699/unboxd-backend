import asyncio
import aiohttp
from bs4 import BeautifulSoup
import requests
from typing import List, Dict, Any, Union
import json

class UserReviewCountError(ValueError):
    status_code = 400

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ReviewScraper():

    def __init__(self, user):
        self.user = user

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> Union[str, None]:
        try:
            async with session.get(url) as response:
                # print(f"Fetching {url}")
                return await response.text()
        except aiohttp.ClientError as e:
            print(f"Client error while fetching {url}: {e}")
        except asyncio.TimeoutError:
            print(f"Timeout error while fetching {url}")
        except Exception as e:
            print(f"An error occurred while fetching {url}: {e}")

    async def extract_review(self, session: aiohttp.ClientSession, url: str) -> str:
        full_url = f"https://letterboxd.com/{url}"
        html = await self.fetch(session, full_url)
        soup = BeautifulSoup(html, "html.parser")
        review = ' '.join([' '.join(p.stripped_strings) for p in soup.find("div", class_="review").select("p")])
        return review

    async def extract_likes_data(self, session: aiohttp.ClientSession, name: str, review_num: int = None) -> List[str]:
        if not review_num:
            url = f"https://letterboxd.com/{self.user}/film/{name}/likes"
        else:
            url = f"https://letterboxd.com/{self.user}/film/{name}/{review_num}/likes"

        html = await self.fetch(session, url)
        soup = BeautifulSoup(html, "html.parser")
        likers = [i.get("href") for i in soup.find_all("a", class_ = "name")]
        return likers

    async def compile_data(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        
        parts = url.split("/")
        if not parts[-2].isdigit():
            name = parts[-2]
            review_num = None
        else:
            name = parts[-3]
            review_num = int(parts[-2])

        review = await self.extract_review(session, url)
        liked_by = await self.extract_likes_data(session, name, review_num=review_num)

        return {
            "name": name,
            "review": review,
            "liked_by" : liked_by
        }

    async def start_process(self, session: aiohttp.ClientSession, url: str) -> List[Dict[str, Any]]:
        html = await self.fetch(session, url)
        soup = BeautifulSoup(html, 'html.parser')

        links = [i.find("a")["href"] for i in soup.find_all("li", class_="film-detail")]
        tasks = [self.compile_data(session, link) for link in links]
        reviews = await asyncio.gather(*tasks)

        return [review for review in reviews if review]

    def page_nums(self) ->int:
        url = f"https://letterboxd.com/{self.user}/films/reviews"
        html = requests.get(url)
        soup = BeautifulSoup(html.text, "html.parser")
        try:
            num_pages = int(soup.find_all("li", class_="paginate-page")[-1].get_text())
        except:
            num_pages = 1
        return num_pages

    async def scrape(self) -> Dict[str,Dict[str, Any]]:
        num_pages = self.page_nums()
        urls = [f"https://letterboxd.com/{self.user}/films/reviews/page/{i}/" for i in range(1, num_pages + 1)]

        connector = aiohttp.TCPConnector(limit_per_host=5)  # Limit parallel connections
        async with aiohttp.ClientSession(connector=connector) as session:
                tasks = [self.start_process(session, url) for url in urls]
                results = await asyncio.gather(*tasks)

        reviews = {}
        for review_list in results:
            for review in review_list:
                movie_name = review['name']
                review_data = {
                    'review': review['review'],
                    'liked_by': review['liked_by']
                }
                if movie_name not in reviews:
                    reviews[movie_name] = [review_data]
                else:
                    reviews[movie_name].append(review_data)
        
        if len(reviews) < 10:
            raise UserReviewCountError("You must review atleast 10 movies")
        else:
            return reviews
        

