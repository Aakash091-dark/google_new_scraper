from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
import os
import time
import json
from typing import Optional
from urllib.parse import urlparse
from datetime import datetime, timedelta
import re
import datefinder


# Hardcoded credentials
project_id = "855529056135"
location = "global"
engine_id = "news-scraper_1752663065679"
api_key = "AIzaSyDWp934QAIP3PJeEL3tw4LeLSxheI-kbpg"

MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds


def search(
    project_id: str,
    location: str,
    engine_id: str,
    api_key: str,
    search_query: str,
    page_token: str = "",
) -> Optional[discoveryengine.types.SearchResponse]:
    if not all([project_id, location, engine_id, api_key, search_query]):
        print("Error: Missing required parameters for search")
        return None

    retry_count = 0
    while retry_count <= MAX_RETRIES:
        try:
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
                f"projects/{project_id}/locations/{location}"
                f"/collections/default_collection/engines/{engine_id}"
                f"/servingConfigs/default_config"
            )
            request = discoveryengine.SearchRequest(
                serving_config=serving_config,
                query=search_query,
                page_size=25,
                page_token=page_token,
            )
            response = client.search_lite(request)
            return response

        except Exception as e:
            retry_count += 1
            if retry_count <= MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                print(f"Max retries reached: {e}")
                return None


def parse_date(date_string):
    if not date_string:
        return None

    date_string = date_string.strip().lower()

    # Handle relative times
    relative_patterns = [
        (
            r"(\d+)\s*minute[s]?\s*ago",
            lambda m: datetime.now() - timedelta(minutes=int(m.group(1))),
        ),
        (
            r"(\d+)\s*hour[s]?\s*ago",
            lambda m: datetime.now() - timedelta(hours=int(m.group(1))),
        ),
        (
            r"(\d+)\s*day[s]?\s*ago",
            lambda m: datetime.now() - timedelta(days=int(m.group(1))),
        ),
        (r"yesterday", lambda m: datetime.now() - timedelta(days=1)),
        (r"just now", lambda m: datetime.now()),
        (r"a day ago", lambda m: datetime.now() - timedelta(days=1)),
        (r"an hour ago", lambda m: datetime.now() - timedelta(hours=1)),
        (r"a minute ago", lambda m: datetime.now() - timedelta(minutes=1)),
    ]

    for pattern, func in relative_patterns:
        match = re.search(pattern, date_string)
        if match:
            parsed_date = func(match)
            return parsed_date.strftime("%Y-%m-%d %H:%M:%S")

    # Try common datetime formats
    date_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%B %d, %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%d %b %Y",
        "%Y/%m/%d",
    ]

    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

    # Try to find date in string with regex
    regex_patterns = [
        r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})",
        r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})",
        r"(\d{8})",  # YYYYMMDD
    ]
    for pattern in regex_patterns:
        match = re.search(pattern, date_string)
        if match:
            try:
                if len(match.group(0)) == 8:
                    # YYYYMMDD format
                    parsed_date = datetime.strptime(match.group(0), "%Y%m%d")
                elif pattern == r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})":
                    year, month, day = map(int, match.groups())
                    parsed_date = datetime(year, month, day)
                else:
                    day, month, year = map(int, match.groups())
                    parsed_date = datetime(year, month, day)
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

    return None


def parse_date_from_url(url):
    if not url:
        return None

    # Extract date patterns from URL
    url_date_patterns = [
        r"/(\d{4})/(\d{2})/(\d{2})/",  # /YYYY/MM/DD/
        r"/(\d{4})-(\d{2})-(\d{2})/",  # /YYYY-MM-DD/
        r"/(\d{8})/",  # /YYYYMMDD/
        r"(\d{4})(\d{2})(\d{2})",  # YYYYMMDD anywhere
    ]

    for pattern in url_date_patterns:
        match = re.search(pattern, url)
        if match:
            try:
                if len(match.group(0)) == 8 or len(match.groups()) == 3:
                    if len(match.group(0)) == 8:
                        parsed_date = datetime.strptime(match.group(0), "%Y%m%d")
                    else:
                        year, month, day = map(int, match.groups())
                        parsed_date = datetime(year, month, day)
                    return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue

    # Fallback to datefinder
    matches = list(datefinder.find_dates(url))
    if matches:
        return matches[0].strftime("%Y-%m-%d %H:%M:%S")

    return None


def extract_date_from_text(text):
    """Try to extract date from arbitrary text (like description or title)."""
    if not text:
        return None
    matches = list(datefinder.find_dates(text))
    if matches:
        return matches[0].strftime("%Y-%m-%d %H:%M:%S")
    return None


def save_to_json(data, filename="search_results.json"):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False, sort_keys=True)
        print(f"Results saved to {filename}")
    except Exception as e:
        print(f"Error saving JSON: {e}")


def main(total_results_needed: int, keyword: str):
    search_query = keyword
    aggregated_results = []
    current_page_token = ""
    pagination_attempt = 0
    max_pagination_attempts = 10

    while (
        len(aggregated_results) < total_results_needed
        and pagination_attempt < max_pagination_attempts
    ):
        pagination_attempt += 1
        response = search(
            project_id, location, engine_id, api_key, search_query, current_page_token
        )
        if not response:
            break

        for result in response.results:
            data = result.document.derived_struct_data
            meta = {
                "title": None,
                "description": None,
                "date_published": None,
                "source": None,
                "url": None,
            }

            # Basic URL
            meta["url"] = data.get("link") or data.get("url") or None

            # --- Title/Description extraction ---
            meta["title"] = data.get("title")
            meta["description"] = data.get("description")

            # --- Try pagemap/metatags ---
            if "pagemap" in data:
                pagemap = data["pagemap"]
                if metatags := pagemap.get("metatags"):
                    tags = metatags[0]
                    meta["title"] = (
                        meta["title"] or tags.get("og:title") or tags.get("title")
                    )
                    meta["description"] = (
                        meta["description"]
                        or tags.get("og:description")
                        or tags.get("twitter:description")
                    )
                    meta["source"] = tags.get("og:site_name") or meta["source"]

                    # Try all date fields in metatags
                    date_fields = [
                        "article:published_time",
                        "datePublished",
                        "publishedDate",
                        "article:published",
                        "pubdate",
                        "date",
                        "article:modified_time",
                        "dateModified",
                        "lastmod",
                        "dc.date.created",
                        "dc.date.issued",
                        "sailthru.date",
                        "article.published",
                        "parsely-pub-date",
                        "timestamp",
                    ]
                    for field in date_fields:
                        if tags.get(field):
                            meta["date_published"] = tags[field]
                            break

                    if not meta["url"]:
                        meta["url"] = (
                            tags.get("og:url")
                            or tags.get("canonical")
                            or tags.get("twitter:url")
                            or None
                        )
                    if not meta["source"] and meta["url"]:
                        try:
                            meta["source"] = urlparse(meta["url"]).netloc
                        except Exception:
                            pass

                # --- Try JSON-LD ---
                if "jsonld" in pagemap:
                    for item in pagemap["jsonld"]:
                        if isinstance(item, dict):
                            if not meta["date_published"]:
                                meta["date_published"] = (
                                    item.get("datePublished")
                                    or item.get("publishedDate")
                                    or item.get("dateCreated")
                                )
                            if not meta["url"]:
                                meta["url"] = item.get("url") or item.get("@id")

            # --- Try main data fields for date ---
            if not meta["date_published"]:
                for fallback_field in [
                    "datePublished",
                    "publishedDate",
                    "date",
                    "created",
                    "modified",
                ]:
                    if data.get(fallback_field):
                        meta["date_published"] = data[fallback_field]
                        break

            # --- Try to parse date from all possible sources ---
            parsed_date = None
            # 1. Try direct date field
            if meta["date_published"]:
                parsed_date = parse_date(str(meta["date_published"]))
            # 2. Try from URL
            if not parsed_date and meta["url"]:
                parsed_date = parse_date_from_url(meta["url"])
            # 3. Try from description
            if not parsed_date and meta.get("description"):
                parsed_date = extract_date_from_text(meta["description"])
            # 4. Try from title
            if not parsed_date and meta.get("title"):
                parsed_date = extract_date_from_text(meta["title"])
            # 5. Fallback: Use today's date if still not found
            if not parsed_date:
                parsed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            meta["date_published"] = parsed_date

            # --- Fallback for URL ---
            if not meta["url"]:
                for possible_url_field in ["link", "url", "href", "webpage"]:
                    val = data.get(possible_url_field)
                    if val and isinstance(val, str) and val.startswith("http"):
                        meta["url"] = val
                        break
            if not meta["url"] and getattr(result.document, "id", None):
                if result.document.id.startswith("http"):
                    meta["url"] = result.document.id

            if any(v for v in meta.values()):
                aggregated_results.append(meta)

            if len(aggregated_results) >= total_results_needed:
                break

        # Pagination token update
        current_page_token = getattr(response, "next_page_token", "")
        if not current_page_token:
            break

    if not aggregated_results:
        return None

    # Prepare summary counts
    titles = [r.get("title") for r in aggregated_results if r.get("title")]
    descriptions = [
        r.get("description") for r in aggregated_results if r.get("description")
    ]
    dates = [
        r.get("date_published") for r in aggregated_results if r.get("date_published")
    ]
    sources = [r.get("source") for r in aggregated_results if r.get("source")]
    urls = [r.get("url") for r in aggregated_results if r.get("url")]

    structured_results = []
    max_length = max(
        len(titles), len(descriptions), len(dates), len(sources), len(urls)
    )

    for i in range(max_length):
        result = {
            "title": titles[i] if i < len(titles) else None,
            "description": descriptions[i] if i < len(descriptions) else None,
            "date_published": dates[i] if i < len(dates) else None,
            "source": sources[i] if i < len(sources) else None,
            "url": urls[i] if i < len(urls) else None,
        }
        structured_results.append(result)

    return {
        "results": structured_results,
        "summary": {
            "total_results": len(structured_results),
            "titles_count": len(titles),
            "descriptions_count": len(descriptions),
            "dates_count": len(dates),
            "sources_count": len(sources),
            "urls_count": len(urls),
        },
    }


if __name__ == "__main__":
    try:
        total_results = int(input("Enter the number of results needed: "))
    except Exception:
        total_results = 2
    keyword = input("Enter the search keyword: ")

    final_result = main(total_results, keyword)

    if final_result:
        save_to_json(final_result)

        for idx, item in enumerate(final_result["results"], start=1):
            print(f"\nResult {idx}:")
            print(f"Title       : {item.get('title') or 'N/A'}")
            print(f"Description : {item.get('description') or 'N/A'}")
            print(f"Published   : {item.get('date_published') or 'N/A'}")
            print(f"Source      : {item.get('source') or 'N/A'}")
            print(f"URL         : {item.get('url') or 'N/A'}")
    else:
        print("No results found.")
