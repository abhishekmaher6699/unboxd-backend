from typing import List, Any
from fastapi import FastAPI, HTTPException
import logging
import json
import os
import psutil
import gc
from functools import wraps
from memory_profiler import profile
from contextlib import contextmanager
from components.MovieScraper import MovieDataScraper, MovieData, UserMovieCountError
from components.ReviewScraper import ReviewScraper, UserReviewCountError
from components.Ranking import Ranking
from components.DataProcessor import Processor
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

@contextmanager
def memory_tracker(section_name: str):
    """Context manager to track memory usage before and after a code block"""
    process = psutil.Process(os.getpid())
    start_mem = process.memory_info().rss / 1024 / 1024  # MB
    logging.info(f"Starting {section_name}: {start_mem:.2f} MB")
    try:
        yield
    finally:
        gc.collect()  # Force garbage collection
        end_mem = process.memory_info().rss / 1024 / 1024  # MB
        logging.info(f"Finished {section_name}: {end_mem:.2f} MB (Difference: {end_mem - start_mem:.2f} MB)")

def memory_logged(func):
    """Decorator to log memory usage of functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        with memory_tracker(func.__name__):
            return await func(*args, **kwargs)
    return wrapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MovieResponse(BaseModel):
    og_data: List[MovieData]  
    processed_data: Any 

app = FastAPI()

allowed_origins = [
    "http://localhost:5173", 
    "https://unboxd-frontend.vercel.app",
    "https://unboxdbyabhi.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@memory_logged
@app.get("/movies-data/", response_model=MovieResponse)
async def movie_info(user:str):
    user = user.strip()
    try:
        logging.info(f"Getting Movie Data for {user}")
        movie_scraper = MovieDataScraper(user)
        movie_data = await movie_scraper.scrape()

        processor = Processor([movie.model_dump() for movie in movie_data], user)
        processed_data = processor.main()

        return  {
            'og_data' : movie_data,
            'processed_data' : processed_data
        }
    except KeyError:
        raise HTTPException(status_code=404, detail="Stat_404")
    except UserMovieCountError:
        raise HTTPException(status_code=400, detail="Stat_400")

@app.get("/reviews/")
async def reviews(user:str):
    user = user.strip()
    try:
        logging.info(f"Getting reviews for {user}")
        review_scraper = ReviewScraper(user)
        results = await review_scraper.scrape()
        return results
    except UserReviewCountError:
        raise HTTPException(status_code=400, detail = "Review_400")

@app.get("/rank")
async def friends_ranking(user:str, group:str):
    user = user.strip()
    try:
        logging.info(f"Getting rank data for {user}")
        ranker = Ranking(user, group)
        results = await ranker.rank_friends()
        return results
    except AttributeError:
        raise HTTPException(status_code=404, detail="Rank_404")
    except ValueError:
        raise HTTPException(status_code=400, detail="Rank_400")
    except KeyError:
        raise HTTPException(status_code=400, detail="Rank_400")

@app.get("/")
def main():
    return "Hey"
