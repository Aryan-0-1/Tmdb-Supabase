import os
import requests
from supabase import create_client, Client

# -------------------------
# CONFIG (from GitHub Secrets)
# -------------------------
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# HELPERS
# -------------------------
def fetch_movies(year, region, page=1):
    """Fetch movies for a given year and region"""
    url = (
        f"https://api.themoviedb.org/3/discover/movie"
        f"?api_key={TMDB_API_KEY}"
        f"&primary_release_year={year}"
        f"&region={region}"
        f"&with_original_language=hi|en"
        f"&page={page}"
    )
    res = requests.get(url).json()
    return res

def fetch_movie_details(movie_id):
    """Fetch full movie details, credits, videos, external IDs"""
    details = requests.get(
        f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}&append_to_response=credits,videos,external_ids,keywords"
    ).json()
    return details

def extract_data(movie):
    """Transform raw TMDb movie into our schema"""
    cast = [c["name"] for c in movie.get("credits", {}).get("cast", [])[:5]]
    genres = [g["name"] for g in movie.get("genres", [])]
    trailer = None
    for v in movie.get("videos", {}).get("results", []):
        if v["type"] == "Trailer" and v["site"] == "YouTube":
            trailer = f"https://www.youtube.com/watch?v={v['key']}"
            break

    return {
        "tmdb_id": movie.get("id"),
        "imdb_id": movie.get("external_ids", {}).get("imdb_id"),
        "title": movie.get("title"),
        "plot": movie.get("overview"),
        "year": movie.get("release_date", "").split("-")[0] if movie.get("release_date") else None,
        "length": movie.get("runtime"),
        "poster": f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}" if movie.get("poster_path") else None,
        "cast": cast,
        "genres": genres,
        "trailer": trailer,
        "tags": [k["name"] for k in movie.get("keywords", {}).get("keywords", [])],
        "awards": None,  # TMDb doesn’t provide awards data
    }

# -------------------------
# MAIN
# -------------------------
def main():
    year = 2024
    regions = ["US", "IN"]  # Hollywood (US), Bollywood (India)

    for region in regions:
        page = 1
        while True:
            data = fetch_movies(year, region, page)
            results = data.get("results", [])
            if not results:
                break

            for movie in results:
                details = fetch_movie_details(movie["id"])
                record = extract_data(details)

                # Insert or update into Supabase
                supabase.table("movies").upsert(record).execute()

            if page >= data.get("total_pages", 1):
                break
            page += 1

if __name__ == "__main__":
    main()
