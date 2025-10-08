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
BATCH_SIZE = 50        # Number of records per upsert batch

# -------------------------
# PROGRESS HELPERS
# -------------------------
def get_progress(supabase):
    res = supabase.table("fetch_progress").select("*").eq("id", 1).execute()
    if res.data:
        return res.data[0]["last_year"], res.data[0]["region"], res.data[0]["last_page"]
    else:
        # initialize if not exists
        supabase.table("fetch_progress").insert(
            {"id": 1, "last_year": 2000, "region": "US", "last_page": 0}
        ).execute()
        return 2000, "US", 0

def save_progress(supabase,year, region, page):
    supabase.table("fetch_progress").upsert({
        "id": 1,
            "last_year": year, "region": region, "last_page": page
        }, on_conflict=["id"]).execute()



# -------------------------
# HELPER FUNCTION WITH RETRY
# -------------------------
remaining_requests = None
daily_limit = None

def safe_request(url):
    """Perform GET request with retry if fails"""
    global remaining_requests, daily_limit
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url)
            # --------------------------
            # Handle rate-limiting (429)
            # --------------------------
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_DELAY))
                print(f"429 Too Many Requests. Waiting for {retry_after} seconds before retrying...")
                time.sleep(retry_after)
                retries += 1
                continue

            
            if response.status_code == 200:
                # Capture TMDb quota headers
                if "X-RateLimit-Remaining" in response.headers:
                    remaining_requests = int(response.headers["X-RateLimit-Remaining"])
                if "X-RateLimit-Limit" in response.headers:
                    daily_limit = int(response.headers["X-RateLimit-Limit"])
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
# BATCH UPSERT WITH RETRY
# -------------------------
def safe_upsert(table, records, batch_size=BATCH_SIZE, retries=MAX_RETRIES, delay=RETRY_DELAY):
    total = len(records)
    for start in range(0, total, batch_size):
        batch = records[start:start + batch_size]
        attempt = 0
        while attempt < retries:
            try:
                table.upsert(batch, on_conflict=["tmdb_id"]).execute()
                print(f"Upserted batch {start}-{start + len(batch) - 1}")
                break
            except Exception as e:
                attempt += 1
                print(f"Batch {start}-{start + len(batch) - 1} failed (attempt {attempt}/{retries}): {e}")
                time.sleep(delay)
        else:
            print(f"Failed to upsert batch {start}-{start + len(batch) - 1} after {retries} retries.")



# -------------------------
# MAIN FUNCTION
# -------------------------
def main():
    current_year, current_region, current_page = get_progress(supabase)
    regions = ["US", "IN"]  # Hollywood (US), Bollywood (India)
    requests_made = 0
    
    for year in range(current_year, 2024):  # fetch from 2000 → 2023
        for region in regions:
            start_page = current_page + 1 if (year == current_year and region == current_region) else 1
            page = start_page

            while True:
                # Stop if API quota exhausted
                if remaining_requests is not None and remaining_requests <= 1:
                    print("Reached TMDb daily API limit (from headers). Saving progress and exiting.")
                    save_progress(supabase, year, region, page - 1)
                    return
                
                data = fetch_movies(year, region, page)
                requests_made += 1
                if not data or "results" not in data:
                    print(f"No data returned for {year}-{region}, page {page}")
                    break

                results = data.get("results", [])
                if not results:
                    print(f"No more results for {year}-{region}")
                    break

                records_to_upsert = []
                for movie in results:
                    # Again check before details request
                    if remaining_requests is not None and remaining_requests <= 1:
                        print("Reached TMDb daily API limit inside details. Saving progress and exiting.")
                        save_progress(supabase, year, region, page)
                        return
                        
                    details = fetch_movie_details(movie["id"])
                    requests_made += 1
                    record = extract_data(details)
                    if record:
                        records_to_upsert.append(record)

                # Batch upsert
                if records_to_upsert:
                    safe_upsert(supabase.table("movies"), records_to_upsert)

                # ✅ Save progress after each page
                save_progress(supabase, year, region, page)
                print(f"Progress saved → Year: {year}, Region: {region}, Page: {page}")

                # Stop if last page
                if page >= data.get("total_pages", 1):
                    break

                page += 1

            # Reset current page after finishing a region
            current_page = 0

    print("All movies fetched successfully!")
    save_progress(supabase, 2025, "US", 0)


if __name__ == "__main__":
    main()
