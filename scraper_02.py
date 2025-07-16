from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
from transformers import pipeline
import pandas as pd
from urllib.parse import urlparse
import schedule
import time
from typing import Optional

# ─── Hard-coded credentials ────────────────────────────────────────────────────
project_id = "855529056135"
location = "global"
engine_id = "news-scraper_1752663065679"
api_key = "AIzaSyDWp934QAIP3PJeEL3tw4LeLSxheI-kbpg"
# ────────────────────────────────────────────────────────────────────────────────

MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

# Load sentiment analysis pipeline (once)
sentiment_pipeline = pipeline("sentiment-analysis")


def search_news(project_id, location, engine_id, api_key, query, page_token=""):
    client_options = ClientOptions(
        api_key=api_key,
        api_endpoint=(
            f"{location}-discoveryengine.googleapis.com"
            if location != "global"
            else None
        ),
    )
    client = discoveryengine.SearchServiceClient(client_options=client_options)
    serving_config = (
        f"projects/{project_id}/locations/{location}/collections/default_collection/"
        f"engines/{engine_id}/servingConfigs/default_config"
    )

    request = discoveryengine.SearchRequest(
        serving_config=serving_config,
        query=query,
        page_size=25,
        page_token=page_token,
    )

    retry_count = 0
    while retry_count <= MAX_RETRIES:
        try:
            response = client.search_lite(request)
            return response
        except Exception as e:
            retry_count += 1
            print(f"Error: {e} | Retrying {retry_count}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY)
    return None


def extract_metadata(result):
    data = result.document.derived_struct_data
    meta = {
        "title": None,
        "description": None,
        "date_published": None,
        "source": None,
        "url": None,
        "sentiment": None,
        "score": None,
    }

    if "pagemap" in data:
        pagemap = data["pagemap"]
        if metatags := pagemap.get("metatags"):
            tags = metatags[0]
            meta.update(
                {
                    "title": tags.get("og:title") or tags.get("title"),
                    "description": tags.get("og:description")
                    or tags.get("twitter:description"),
                    "date_published": tags.get("article:published_time"),
                    "source": tags.get("og:site_name"),
                    "url": tags.get("og:url"),
                }
            )

            if not meta["source"] and tags.get("og:url"):
                try:
                    meta["source"] = urlparse(tags["og:url"]).netloc
                except:
                    pass

    # Run sentiment if description or title is available
    text = meta["description"] or meta["title"]
    if text:
        sentiment = sentiment_pipeline(text[:512])[0]
        meta["sentiment"] = sentiment["label"]
        meta["score"] = round(sentiment["score"], 3)

    return meta


def scrape_news(query: str, total_results: int = 10):
    print(f"\n Searching: {query}")
    aggregated = []
    page_token = ""
    pages = 0

    while len(aggregated) < total_results and pages < 5:
        response = search_news(
            project_id, location, engine_id, api_key, query, page_token
        )
        if not response:
            break

        for result in response.results:
            metadata = extract_metadata(result)
            if metadata["title"]:  # filter out junk
                aggregated.append(metadata)
            if len(aggregated) >= total_results:
                break

        page_token = getattr(response, "next_page_token", "")
        if not page_token:
            break
        pages += 1

    print(f" Found {len(aggregated)} results.\n")
    return aggregated


def save_to_csv(data: list, filename: str = "news_results.csv"):
    if not data:
        print("No data to save.")
        return
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f" Saved to {filename}")


def display_results(results: list):
    for i, item in enumerate(results, start=1):
        print(f"\n🔹 Result {i}")
        print(f"    Title       : {item.get('title')}")
        print(f"    Description : {item.get('description')}")
        print(f"    Published   : {item.get('date_published') or 'N/A'}")
        print(f"    Source      : {item.get('source')}")
        print(f"    URL         : {item.get('url')}")
        print(f"    Sentiment   : {item.get('sentiment')} ({item.get('score')})")


def scheduled_job():
    query = "Warehouse in india"
    results = scrape_news(query, total_results=5)
    display_results(results)
    save_to_csv(results, "epack_news.csv")


if __name__ == "__main__":
    # One-time run
    scheduled_job()

    # Uncomment below to run every 10 minutes
    # schedule.every(10).minutes.do(scheduled_job)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
