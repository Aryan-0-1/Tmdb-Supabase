import os
import requests
import time
from supabase import create_client, Client

# -------------------------
# CONFIG (from GitHub Secrets)
# -------------------------
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# RETRY CONFIG
# -------------------------
MAX_RETRIES = 5        # Maximum number of retries per request
RETRY_DELAY = 5        # Seconds to wait before retrying

# -------------------------
# HELPER FUNCTION WITH RETRY
# -------------------------
def safe_request(url):
    """Perform GET request with retry if fails"""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Request failed (status {response.status_code}). Retrying in {RETRY_DELAY}s...")
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying in {RETRY_DELAY}s...")

        retries += 1
        time.sleep(RETRY_DELAY)

    print(f"Failed after {MAX_RETRIES} retries. Skipping URL: {url}")
    return None

# -------------------------
# FETCH FUNCTIONS
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
    return safe_request(url)

def fetch_movie_details(movie_id):
    """Fetch full movie details, credits, videos, external IDs"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}&append_to_response=credits,videos,external_ids,keywords"
    return safe_request(url)

# -------------------------
# DATA EXTRACTION
# -------------------------
def extract_data(movie):
    """Transform raw TMDb movie into our schema"""
    if not movie:
        return None

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
        "awards": None,  # TMDb does not provide awards
        "language": movie.get("original_language")  # NEW FIELD
    }

# -------------------------
# MAIN FUNCTION
# -------------------------
def main():
    year = 2024
    regions = ["US", "IN"]  # Hollywood (US), Bollywood (India)

    for region in regions:
        page = 1
        while True:
            data = fetch_movies(year, region, page)
            if not data or "results" not in data:
                break

            results = data.get("results", [])
            if not results:
                break

            for movie in results:
                details = fetch_movie_details(movie["id"])
                record = extract_data(details)
                if record:
                    supabase.table("movies").upsert(record).execute()
                    print(f"Inserted/Updated: {record['title']}")

            if page >= data.get("total_pages", 1):
                break
            page += 1

if __name__ == "__main__":
    main()
