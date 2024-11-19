import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import json
import requests
# from tqdm.asyncio import tqdm
import pandas as pd
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from database.database import (create_friends_table, does_user_exist, fetch_user_data, insert_user_data, update_user_data)

class Ranking:

    def __init__(self, user, subset):
        self.user = user
        self.subset = subset
        self.name_map = {}
        self.rev_name_map = {}
        self.pic_map = {}
        self.link_title_map = {}
        create_friends_table()
        
    async def fetch(self, session: aiohttp.ClientSession, url: str):
        max_attempts = 3
        base_wait_time = 1
        max_wait_time = 10

        for attempt in range(max_attempts):
            try:
                async with session.get(url) as response:
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
                break  

        return None

    def profile_info(self):
        profile = f"https://letterboxd.com/{self.user}/"
        page = requests.get(profile)
        soup = BeautifulSoup(page.text, 'lxml')

        h = soup.find('div', class_="profile-stats js-profile-stats").select('h4')
        follower_count = int(h[-1].find('a').find('span', class_='value').text) if "followers" in h[-1].find('a')['href'] else 0
        following_count = int(h[-2].find('a').find('span', class_='value').text) if "following" in h[-2].find('a')['href'] else 0
        dp = soup.find('h1', class_='person-display-name').text
        follower_pages = (follower_count // 25) + 1
        following_pages = (following_count // 25) + 1

        return follower_pages, following_pages, dp

    async def fetch_friend_list(self, session, url):
        
        names = []
        urls = []
        pics = []
        
        response = await self.fetch(session, url)
        if response is None:
            return names, urls
        soup = BeautifulSoup(response, "lxml")

        for person in soup.find_all('td', class_='table-person'):
            name = person.find('img')['alt']
            pic = person.find('img')['src'] if person.find('img')['src'] else None
            url = person.find('a', class_='avatar')['href']
            names.append(name)
            urls.append(url)
            pics.append(pic)
            assert len(names) == len(urls)
        
        return names, urls, pics

    async def extract_friends(self, type, page_num):

        urls =  [f"https://letterboxd.com/{self.user}/{type}/page/{i}/" for i in range(1, page_num + 1)]

        connector = aiohttp.TCPConnector(limit_per_host=5) 
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.fetch_friend_list(session, url) for url in urls]
            results = await asyncio.gather(*tasks)
        
        names, urls, pics = [], [], []
        for result in results:
            names.extend(result[0])
            urls.extend(result[1])
            pics.extend(result[2])

        return urls, names, pics

    async def extract_movie_data(self, session, url):
        html = await self.fetch(session, url)
        if not html:
            return [], [], []
        
        links = []
        user_ratings = []
        titles = []

        soup = BeautifulSoup(html, 'lxml')

        for li in soup.find_all('li', class_="poster-container"):
            poster_div = li.find('div', class_="poster")
            if poster_div:
                link = poster_div.get('data-target-link')
                links.append(link) if link else links.append(None)

                title = poster_div.find('img')['alt']
                titles.append(title) if title else titles.append(None)
                
            rating = li.find('span', class_='rating')
            if rating:
                match = re.search(r"rated-(\d+)", rating['class'][-1])
                number = int(match.group(1)) / 2
                user_ratings.append(number)
            else:
                user_ratings.append(0)

        assert len(titles) == len(links)
        return links, titles, user_ratings
   
    async def page_nums(self, session, user):
        url = f"https://letterboxd.com/{user}/films"
        html = await self.fetch(session, url)
        if html is None:
            return 1
        soup = BeautifulSoup(html, "lxml")
        try:
            num_pages = int(soup.find_all("li", class_="paginate-page")[-1].get_text())
        except:
            num_pages = 1
        return num_pages
    
    async def fetch_batch(self, session, urls):
        connector = aiohttp.TCPConnector(limit_per_host=5)  # Limit parallel connections
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.extract_movie_data(session, url) for url in urls]
            return await asyncio.gather(*tasks)
        
    async def extract_movies_for_user(self, session, user, batch_size=10):


        async def get_data():        
            pages = await self.page_nums(session, user)
            urls = [f"https://letterboxd.com/{user}/films/page/{i}/" for i in range(1, pages + 1)]
        
            results = []
            for i in range(0, len(urls), batch_size):
                batch = urls[i:i + batch_size]
                results.extend(await self.fetch_batch(session, batch))
            titles = []
            ratings = []
            links = []

            for link, title, rating in results:
                links.extend(link)
                ratings.extend(rating)
                titles.extend(title)
                
            return titles, ratings, links
        
        result = does_user_exist(user)
        current_time = datetime.now()
        if result:
            timestamp = datetime.fromisoformat(result[0])
            is_stale = (current_time - timestamp).days >= 5
            if is_stale:
                titles, ratings, links = await get_data()
                update_user_data((user, current_time.isoformat(), titles, links, ratings))
            else:

                data = fetch_user_data(user)
                titles, links, ratings = json.loads(data[0]), json.loads(data[1]), json.loads(data[2])
        
        else:
            titles, ratings, links = await get_data()
            insert_user_data((user, current_time.isoformat(), titles, links, ratings))
  


        if user == self.user:
            if len(titles) < 20:

                raise ValueError(f"The user {user} has less than 5 movies in their Letterboxd profile.")
            
        return {
            'titles': titles,
            'ratings': ratings,
            'links' : links
        }
    
    async def start_extraction(self, batch_size = 10):
        follower_pages, following_pages, dp = self.profile_info()
        if self.subset == "followers":
            user_names, names, pics = await self.extract_friends("followers", follower_pages)
        elif self.subset == "following":
            user_names, names, pics = await self.extract_friends("following", following_pages)
        elif self.subset == "both":
            user_names1, names1, pics1 = await self.extract_friends("followers", follower_pages)
            user_names2, names2, pics2 = await self.extract_friends("following", following_pages)
            
            # Create temporary mappings to preserve picture relationships
            temp_pic_map1 = {username: pic for username, pic in zip(user_names1, pics1)}
            temp_pic_map2 = {username: pic for username, pic in zip(user_names2, pics2)}
            
            # Combine usernames and names while preserving uniqueness
            user_names = list(dict.fromkeys(user_names1 + user_names2))
            names = []
            pics = []
            
            # Create final name and pic lists while maintaining correct relationships
            for username in user_names:
                # Find the corresponding name from either list
                name = None
                if username in user_names1:
                    idx = user_names1.index(username)
                    name = names1[idx]
                elif username in user_names2:
                    idx = user_names2.index(username)
                    name = names2[idx]
                
                # Find the corresponding picture
                pic = temp_pic_map1.get(username) or temp_pic_map2.get(username)
                
                names.append(name)
                pics.append(pic)
        
        name_map = {username: name for username, name in zip(user_names, names)}
        reversed_name_map = {name: username for username, name in zip(user_names, names)}
        pic_map = {name: pic for name, pic in zip(names, pics)}
        name_map[self.user] = dp
        self.name_map = name_map
        self.rev_name_map = reversed_name_map
        self.pic_map = pic_map

        urls =  user_names + [self.user] 

        async def user_batch_fetch(urls):
            connector = aiohttp.TCPConnector(limit_per_host=5)  
            async with aiohttp.ClientSession(connector=connector) as session:
                tasks = [self.extract_movies_for_user(session, user) for user in urls] 
                return await asyncio.gather(*tasks)
                # return await tqdm.gather(*tasks, desc="Scraping User List", total=len(urls))

        results = []
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            try:
                results.extend(await user_batch_fetch(batch))
            except ValueError as e:
                print(f"Error: {e}")
                return None
            
        data = {'user': [], 'title': [], 'rating': [], 'links' : []}

        for user, result in zip(user_names + [self.user], results):
            titles = result['titles']
            ratings = result['ratings']
            links = result['links']

            data['user'].extend([name_map[user]] * len(links))
            data['title'].extend(titles)
            data['rating'].extend(ratings)
            data['links'].extend(links)


        link_title_map = {link: title for title, link in zip(data['title'], data['links'])}
        self.link_title_map = link_title_map

        return data

    def recommend_movies(self, pivot_table, user_similarity_df, n_recommendations=10):
        user = self.name_map[self.user]

        user_ratings = pivot_table.loc[user]
        sum_of_sim = user_similarity_df.drop(columns=[user]).loc[user].sum()
        similar_users = user_similarity_df[user].sort_values(ascending=False)
        recommendations = pd.Series()

        for similar_user, similarity_score in similar_users.items():
            if similar_user != user:
                similar_user_ratings = pivot_table.loc[similar_user]
                unrated_movies = similar_user_ratings[user_ratings == 0]
                recommendations = recommendations.add(unrated_movies * similarity_score, fill_value=0)

        recommendations = recommendations / sum_of_sim
        recommendations = recommendations.sort_values(ascending=False)
        return recommendations.head(n_recommendations).to_dict()
    
    async def rank_friends(self):

        data = await self.start_extraction()
        user = self.name_map[self.user]

        df = pd.DataFrame(data)
        rating_pivot = df.pivot_table(index='user', columns='links', values='rating', aggfunc='mean')
        updated_ratings = rating_pivot.fillna(0) + 1
        updated_ratings[rating_pivot.isna()] = None
        rating_pivot = updated_ratings.fillna(0)

        index_to_keep = rating_pivot.loc[user][rating_pivot.loc[user] != 0].index
        filtered_pivot = rating_pivot.loc[:, index_to_keep]
        user_similarity = cosine_similarity(filtered_pivot)
        user_similarity_df = pd.DataFrame(user_similarity, index=rating_pivot.index, columns=rating_pivot.index)

        rankings = user_similarity_df.drop(columns=[user]).loc[user].sort_values(ascending=False).to_dict()
        rankings = { key : {
                'url' : (self.rev_name_map[key] if key in self.rev_name_map else key),
                'similarity' : value,
                'pic' : self.pic_map[key] if key in self.pic_map else ''
            } for key, value in rankings.items()}
        
        reccomendations = self.recommend_movies(rating_pivot, user_similarity_df)
        reccomendations = {self.link_title_map[key] : {
                'url' : key,
                'rating' : value
            } for key, value in reccomendations.items()}
        
        return {
            'rankings': rankings,
            'reccomendations' : reccomendations
        }
