import pandas as pd
import numpy as np
import json
import requests
from pathlib import Path
from bs4 import BeautifulSoup

personal_identity = [
    'Politics and human rights',
    'Religious faith, sin, and forgiveness',
    'Captivating relationships and charming romance',
    'Challenging or sexual themes & twists',
    # 'Charming romances and delightful chemistry',
    'Emotional LGBTQ relationships',
    'Inspiring sports underdog stories',
    'Emotional life of renowned artists',
    'Emotional and touching family dramas',
    'Fascinating, emotional stories and documentaries',
    'Emotional teen coming-of-age stories',
    'Enduring stories of family and marital drama',
    'Erotic relationships and desire',
    'Faith and religion',
    'Faith and spiritual journeys',
    'Political drama, patriotism, and war',
    'Heartbreaking and moving family drama',
    'Moving relationship stories',
    'Powerful stories of heartbreak and suffering',
    'Racism and the powerful fight for justice',
    'Student coming-of-age challenges',
    'Teen friendship and coming-of-age',
    # 'Tragic sadness and captivating beauty',
    'Underdogs and coming of age',
    'Passion and romance',

]
escapist = [
    'Crime, drugs and gangsters',
    'Chilling experiments and classic monster horror',
    'Creepy, chilling, and terrifying horror',
    'Action-packed space and alien sagas',
    'Brutal, violent prison drama',
    'Captivating vision and Shakespearean drama',
    'Dangerous technology and the apocalypse',
    'Disastrous voyages and heroic survival',
    'Dazzling vocal performances and musicals',
    'Explosive and action-packed heroes vs. villains',
    'Dreamlike, quirky, and surreal storytelling',
    'Song and dance',
    'Spooky, scary comedy',
    'Emotional and captivating fantasy storytelling',
    'Epic adventure and breathtaking battles',
    'Epic heroes',
    'Relationship comedy',
    'Fairy-tale fantasy and enchanted magic',
    'Fantasy adventure, heroism, and swordplay',
    'Gory, gruesome, and slasher horror',
    'Gothic and eerie haunting horror',
    'Heists and thrilling action',
    'Historical battles and epic heroism',
    'Horror, the undead and monster classics',
    'Imaginative space odysseys and alien encounters',
    "Kids' animated fun and adventure",
    'Lavish dramas and sumptuous royalty',
    'Monsters, aliens, sci-fi and the apocalypse',
    'Sci-fi horror, creatures, and aliens',
    'Sci-fi monster and dinosaur adventures',
    'Superheroes in action-packed battles with villains',
    'Survival horror and zombie carnage',
    'Thought-provoking sci-fi action and future technology',
    'Terrifying, haunted, and supernatural horror',
    'Adrenaline-fueled action and fast cars',
    'Horror, the undead and monster classics',
    "Kids' animated fun and adventure",

]

def theme_score(df):
    score = {
        'personal_identity': 0,
        'escapist': 0,
    }
    df = df[df['user_rating'] != 0]
    themes = df.themes.values

    for i in themes:
        for j in i:
            if j in personal_identity:
                score['personal_identity'] += 1 / len(themes)
                break
            if j in escapist:
                score['escapist'] += 1 / len(themes)
                break
    return score

def date_diff(df):
    df = df.copy()
    df['date_diff'] = (df['last_watched'] - df['release_date']).dt.days
    return df[df['date_diff'] < 60].shape[0] / df.shape[0]

def review_counts(df):
    return df[df['is_reviewed']].shape[0] / df.shape[0]

def popularity(df):
  highly_watched = df[df['stats_watched'] > 1000000].shape[0]
  less_watched = df[df['stats_watched'] < 10000].shape[0]
  return (highly_watched - less_watched) / df.shape[0]

def rate_difference(df):
    rated_movies = df[df['user_rating'] != 0]
    rate_diff = rated_movies['user_rating'] - rated_movies['rating']
    return rate_diff.var()

class Processor:
    def __init__(self, data, user):
        self.data = data
        self.user = user
        self.df = pd.DataFrame(data)
    
    def preprocess_df(self):
        df = self.df
        df['rating_difference'] = df['user_rating'] - df['rating']
        df['watched_to_like_ratio'] = df['stats_liked'] / df['stats_watched']
        df['last_watched'] = pd.to_datetime(df['last_watched'], errors='coerce')
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        df['release_year'] = df['release_date'].dt.year
        lang_codes = pd.read_csv(Path(__file__).resolve().parent.parent / 'public' / 'code.csv')
        df = df.merge(lang_codes, left_on='original_language', right_on='ISO_code', how='left').drop(columns=['ISO_code', 'original_language'])

        self.df = df
    
    def basic_info(self):
        df = self.df
        url = f"https://letterboxd.com/{self.user}/"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        profile_pic = soup.find('div', class_='profile-avatar').select('img')[0]['src']
        profile_name = soup.find('h1', class_='person-display-name').span.get_text()

        movie_count = df.shape[0]
        rated_movie_count = df[df['user_rating'] != 0].shape[0]
        liked_movie_count = df[df['is_liked']].shape[0]
        reviewed_movie_count = df[df['is_reviewed']].shape[0]
        top250_movie_count = df[df['stats_rank'] != 0].shape[0]

        languages = df['spoken_languages'].explode().unique()
        language_count = len(languages) - 1 if 'No Language' in languages else len(languages) 
        themes_count = len(df['themes'].explode().unique()) - 1

        countries_explored = list(df['countries'].explode().dropna().unique())

        return {
            'profile_pic': profile_pic,
            'profile_name': profile_name,
            'movie_count': movie_count,
            'rated_movie_count': rated_movie_count,
            'liked_movie_count': liked_movie_count,
            'reviewed_movie_count': reviewed_movie_count,
            'top250_movie_count': top250_movie_count,
            'language_count': language_count,
            'themes_count': themes_count,
            'countries_explored': countries_explored
        }
    
    def log_activity(self):
        df = self.df 
        date = df['last_watched'].dt.date
        monthly_counts = date.groupby(date.apply(lambda x: (x.year, x.month))).count()
        log_data = {f'{year}-{month}': int(count) for (year, month), count in zip(monthly_counts.index, monthly_counts.values)}

        return log_data
    
    def like_to_watch_movie(self):

        df = self.df
        highly_liked = df[df['stats_watched'] > 10].sort_values(by='watched_to_like_ratio', ascending=False)[:10][['title', 'stats_liked', 'stats_watched', 'watched_to_like_ratio']].to_dict()
        low_liked = df[df['stats_watched'] > 10].sort_values(by='watched_to_like_ratio', ascending=True)[:10][['title', 'stats_liked', 'stats_watched', 'watched_to_like_ratio']].to_dict()

        return {
            'high': highly_liked,
            'low': low_liked
        }
    
    def high_rated_genres_themes(self):

        df = self.df
        def get_data(feature):
            if feature == "genres":
                threshold = 1 if df.shape[0] < 50 else 10
            else:
                threshold = 5 if df.shape[0] < 50 else 10

            temp_df = df.explode(feature)
            temp_df = temp_df.groupby([feature, 'user_rating']).size().unstack(fill_value=0)
            if 0 in temp_df.columns:
                temp_df = temp_df.drop(columns=[0])
            temp_df['total'] = temp_df.sum(axis=1)
            high_rating_columns = [col for col in [4.0, 4.5, 5] if col in temp_df.columns]
            temp_df['high_rating'] = temp_df[high_rating_columns].sum(axis=1)
            temp_df['high_rate_ratio'] = temp_df['high_rating'] / temp_df['total']
            return temp_df[temp_df['total'] > threshold].sort_values(by='high_rate_ratio', ascending=False)['high_rate_ratio'].to_dict()
        
        genre_dict = get_data('genres')
        themes_dict = get_data('themes')

        return {
            'high_rated_genres' : genre_dict,
            'high_rated_themes' : themes_dict
        }
    
    def monthly_summary(self):

        def most_watched(df, feature):
            col = df[feature]
            counts = col.dropna().explode().value_counts()
            data = counts[counts > 1].nlargest(2)
            return data.to_dict()
        
        data = []

        df = self.df
        for i in df['last_watched'].dt.to_period('M').unique():
            year = i.year
            month = i.month

            filtered_data = df[
                (df['user_rating'] != 0) &
                (df['last_watched'].dt.year == year) &
                (df['last_watched'].dt.month == month)
            ]

            if filtered_data.shape[0] == 0:
                continue

            most_watched_genre = most_watched(filtered_data, 'genres')
            most_watched_country = most_watched(filtered_data, 'countries')
            most_watched_language = most_watched(filtered_data, 'spoken_languages')
            most_watched_director = most_watched(filtered_data, 'director')
            most_watched_year = most_watched(filtered_data, 'release_year')
            most_watched_theme = most_watched(filtered_data, 'themes')
            most_watched_actor = most_watched(filtered_data, 'actors')

            data.append({'time' : f'{year}-{month}',
                    'data' : {
                                'total_movies' : filtered_data.shape[0],
                                'most_watched_genre': most_watched_genre ,
                                'most_watched_country': most_watched_country,
                                'most_watched_director': most_watched_director,
                                'most_watched_year': most_watched_year,
                                'most_watched_theme': most_watched_theme,
                                'most_watched_language': most_watched_language,
                                'most_watched_actor': most_watched_actor,
                    }
            })
        return data
    
    def diversity_score(self):
            
        def entropy(df, column, max):
            df = df[df['user_rating'] != 0]
            temp = df[column].explode().value_counts().reset_index()
            probs = (temp['count'] / temp['count'].sum()).tolist()

            entropy = -sum([i * np.log2(i) for i in probs])
            return  (entropy / np.log2(max))
        
        df = self.df
        genre_score = entropy(df, 'genres', 20)
        country_score =  entropy(df, 'countries', 195)
        themes_score =  entropy(df, 'themes', 120)
        language_score =  entropy(df, 'spoken_languages', 100)
        year_score = entropy(df, 'release_year', 136)
        og_language_score = entropy(df, 'Language', 100)

        return (
                (genre_score * 0.15) +
                (country_score * 0.2) +
                (themes_score * 0.15) +
                (language_score * 0.1) +
                (og_language_score * 0.2) +
                (year_score * 0.2)
                )
    
    def obscurity_score(self):

        df = self.df
        df = df[df['user_rating'] != 0]
        total_movies = df.shape[0]
        obscure_popularity = df[df['stats_watched'] < 10000].shape[0] / total_movies
        obscure_ratings = df[df['rating'] < 3].shape[0] / total_movies
        ranked_movies = df[df['stats_rank'] != 0].shape[0] / total_movies
        older_movies = df[df['release_year'] < 1950].shape[0] / total_movies

        score = (
            (obscure_popularity * 0.35) +
            (obscure_ratings * 0.2) +
            (older_movies * 0.25) +
            (ranked_movies * 0.2)
            )

        return score
    
    def word_cloud(self):

        df = self.df
        threshold = 3 if df.shape[0] < 50 else 10
        genre_df = df.explode('nanogenres')
        genre_df = genre_df.groupby(['nanogenres', 'user_rating']).size().unstack(fill_value=0)

        if 0 in genre_df.columns:
            genre_df = genre_df.drop(columns=[0])

        genre_df['total'] = genre_df.sum(axis=1)
        cols = list([col for col in [4.0, 4.5, 5] if col in genre_df.columns])
        genre_df['high_rating'] = genre_df[cols].sum(axis=1)
        genre_df['high_rate_ratio'] = genre_df['high_rating'] / genre_df['total']

        return genre_df[genre_df['total'] > threshold].sort_values(by='high_rate_ratio', ascending=False)[:50]['high_rate_ratio'].to_dict()
    
    def achievements(self):
        df = self.df
        countries = df['countries'].explode().dropna().unique()
        country_count = len(countries)
        languages = df['spoken_languages'].explode().dropna().unique()
        language_count = len(languages) - 1 if 'No Language' in languages else len(languages) 
        theme_count = len(df['themes'].explode().dropna().unique())
        genre_count = len(df['genres'].explode().dropna().unique())
        director_count = len(df['director'].dropna().unique())
        reviewed_movie_count = df[df['is_reviewed']].shape[0]
        top250_movie_count = df[df['stats_rank'] != 0].shape[0]
        decade_count = len(((df['release_year'].dropna() // 10) * 10).unique())
        obscure_movies = df[df['stats_watched'] < 1000].shape[0]

        continents = pd.read_csv(Path(__file__).resolve().parent.parent / 'public' / 'continents.csv')
        country_to_continent = dict(zip(continents['Country'], continents['Continent']))

        unique_continents = {country_to_continent[country] for country in countries if country in country_to_continent}
        achievements = []

        if len(unique_continents) == 6:
            achievements.append('Continent!')

        if country_count > 100:
            achievements.append("Traveller 3")
        elif country_count > 50:
            achievements.append("Traveller 2")
        elif country_count > 25:
            achievements.append("Traveller 1")

        if language_count > 100:
            achievements.append("Linguist 3")
        elif language_count > 50:
            achievements.append("Linguist 2")
        elif language_count > 25:
            achievements.append("Linguist 1")

        if theme_count > 100:
            achievements.append("Theme Explorer 3")
        elif theme_count > 75:
            achievements.append("Theme Explorer 2")
        elif theme_count > 50:
            achievements.append("Theme Explorer 1")

        if genre_count == 20:
            achievements.append("Genre Master")

        if director_count > 100:
            achievements.append("Director Explorer 3")
        elif director_count > 50:
            achievements.append("Director Explorer 2")
        elif director_count > 25:
            achievements.append("Director Explorer 1")

        if reviewed_movie_count > 200:
            achievements.append("Reviewer 3")
        elif reviewed_movie_count > 100:
            achievements.append("Reviewer 2")
        elif reviewed_movie_count > 50:
            achievements.append("Reviewer 1")

        if decade_count > 10:
            achievements.append('Time Traveller 3')
        elif decade_count > 8:
            achievements.append('Time Traveller 2')
        elif decade_count > 5:
            achievements.append('Time Traveller 1')

        if top250_movie_count == 250:
            achievements.append("250 Master")
        elif top250_movie_count > 100:
            achievements.append('Popular 3')
        elif top250_movie_count > 100:
            achievements.append("Popular 2")
        elif top250_movie_count > 50:
            achievements.append("Popular 1")

        if obscure_movies > 75: 
            achievements.append('obscure 3')
        elif obscure_movies > 50:
            achievements.append('obscure 2')
        elif obscure_movies > 25:
            achievements.append("obscure 1")

        return achievements

    def get_user_type(self):

        df = self.df
        df = df[df['user_rating'] != 0]
        theme_scores = theme_score(df)
        diversity = self.diversity_score()
        obscurity = self.obscurity_score()
        date_difference = date_diff(df)
        popularity_score = popularity(df)
        review_score = review_counts(df)


        if diversity > 0.6 and obscurity > 0.1:
            return 'exp'

        elif theme_scores['personal_identity'] > 0.5:
            return 'idn'

        elif theme_scores['escapist'] > 0.4 and review_score < 0.4:
            return 'esc'

        elif (
            (date_difference > 0.1 or
            popularity_score > 0.1) and
            review_score > 0.4
            ):
            return 'scb'

        else:
            return 'csl'
        
    
    def main(self):
        self.preprocess_df()
        basic_info = self.basic_info()
        rating_diff = self.df[self.df['user_rating'] != 0]['rating_difference'].values.tolist()
        log_data = self.log_activity()
        likewatchdata = self.like_to_watch_movie()
        genre_theme_data = self.high_rated_genres_themes()
        monthly_sum = self.monthly_summary()
        div_score = self.diversity_score()
        obs_score = self.obscurity_score()
        wordcloud = self.word_cloud()
        user_type = self.get_user_type()
        achievement_list = self.achievements()

        return {
            'basic_info': basic_info,
            'rating_diff': rating_diff,
            'achievements': achievement_list,
            'log_activity': log_data,
            'like_to_watch': likewatchdata,
            'high_rated_genres_and_themes': genre_theme_data,
            'monthly_summary': monthly_sum,
            'diversity_score': div_score,
            'obscurity_score': obs_score,
            'word_cloud': wordcloud,
            'user_type': user_type
        }