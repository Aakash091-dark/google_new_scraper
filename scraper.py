from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
import os
import time
import json
from typing import Optional
from urllib.parse import urlparse
from datetime import datetime
import re

# ─── Hard-coded credentials ────────────────────────────────────────────────────
project_id = "855529056135"
location = "global"
engine_id = "news-scraper_1752663065679"
api_key = "AIzaSyDWp934QAIP3PJeEL3tw4LeLSxheI-kbpg"
# ────────────────────────────────────────────────────────────────────────────────

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

    print(
        f"Attempting search with query: '{search_query}' and page token: '{page_token}'"
    )

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

            print("Sending search request to Discovery Engine API")
            response = client.search_lite(request)
            print(f"Search successful, received {len(response.results)} results")
            return response

        except Exception as e:
            retry_count += 1
            print(f"Error during search (attempt {retry_count}/{MAX_RETRIES}): {e}")
            if retry_count <= MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds…")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Max retries reached. Giving up.")
                return None


def parse_date(date_string):
    """Parse various date formats and return a standardized format."""
    if not date_string:
        return None

    # Common date formats to try
    date_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO format with microseconds
        "%Y-%m-%dT%H:%M:%SZ",  # ISO format
        "%Y-%m-%dT%H:%M:%S",  # ISO format without Z
        "%Y-%m-%d %H:%M:%S",  # Standard datetime
        "%Y-%m-%d",  # Standard date
        "%d/%m/%Y",  # DD/MM/YYYY
        "%m/%d/%Y",  # MM/DD/YYYY
        "%d-%m-%Y",  # DD-MM-YYYY
        "%m-%d-%Y",  # MM-DD-YYYY
        "%B %d, %Y",  # Month DD, YYYY
        "%d %B %Y",  # DD Month YYYY
        "%b %d, %Y",  # Mon DD, YYYY
        "%d %b %Y",  # DD Mon YYYY
        "%Y/%m/%d",  # YYYY/MM/DD
        "%d.%m.%Y",  # DD.MM.YYYY
        "%m.%d.%Y",  # MM.DD.YYYY
        "%Y.%m.%d",  # YYYY.MM.DD
    ]

    # Try to parse with different formats
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string.strip(), fmt)
            return {
                "original": date_string,
                "formatted": parsed_date.strftime("%Y-%m-%d %H:%M:%S"),
                "iso": parsed_date.isoformat(),
                "timestamp": int(parsed_date.timestamp()),
            }
        except ValueError:
            continue

    # If no format matches, try regex patterns for partial matches
    patterns = [
        r"(\d{4})-(\d{2})-(\d{2})",  # YYYY-MM-DD
        r"(\d{2})/(\d{2})/(\d{4})",  # DD/MM/YYYY or MM/DD/YYYY
        r"(\d{2})-(\d{2})-(\d{4})",  # DD-MM-YYYY or MM-DD-YYYY
    ]

    for pattern in patterns:
        match = re.search(pattern, date_string)
        if match:
            try:
                if pattern == r"(\d{4})-(\d{2})-(\d{2})":
                    year, month, day = match.groups()
                else:
                    part1, part2, year = match.groups()
                    # Assume DD/MM format for ambiguous cases
                    day, month = part1, part2

                parsed_date = datetime(int(year), int(month), int(day))
                return {
                    "original": date_string,
                    "formatted": parsed_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "iso": parsed_date.isoformat(),
                    "timestamp": int(parsed_date.timestamp()),
                }
            except (ValueError, TypeError):
                continue

    # If all parsing fails, return original with metadata
    return {
        "original": date_string,
        "formatted": None,
        "iso": None,
        "timestamp": None,
        "parse_error": True,
    }


def save_to_json(data, filename="search_results.json"):
    """Save the search results to a JSON file with proper formatting."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False, sort_keys=True)
        print(f"Results saved to {filename}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")


def main(total_results_needed: int, domain: str):
    search_query = f"{domain} Warehouse"
    print(f"Generated LinkedIn search query: {search_query}")

    aggregated_results = []
    pages_metadata = []
    current_page_token = ""
    pagination_attempt = 0
    max_pagination_attempts = 10

    while (
        len(aggregated_results) < total_results_needed
        and pagination_attempt < max_pagination_attempts
    ):
        pagination_attempt += 1
        print(
            f"Pagination attempt {pagination_attempt} with token '{current_page_token}'"
        )

        response = search(
            project_id, location, engine_id, api_key, search_query, current_page_token
        )
        if response is None:
            break

        pages_metadata.append(
            {
                "page_token_used": current_page_token,
                "results_in_page": len(response.results),
            }
        )
        print(f"Received {len(response.results)} results")

        for result in response.results:
            data = result.document.derived_struct_data
            meta = {
                "title": None,
                "description": None,
                "date_published": None,
                "source": None,
            }

            if "pagemap" in data:
                pagemap = data["pagemap"]
                if metatags := pagemap.get("metatags"):
                    tags = metatags[0]

                    # Try multiple date fields
                    date_published = None
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
                            date_published = tags[field]
                            break

                    meta.update(
                        {
                            "title": tags.get("og:title") or tags.get("title"),
                            "description": tags.get("og:description")
                            or tags.get("twitter:description"),
                            "date_published": date_published,
                            "source": tags.get("og:site_name"),
                        }
                    )

                    if not meta["source"] and tags.get("og:url"):
                        try:
                            meta["source"] = urlparse(tags["og:url"]).netloc
                        except:
                            pass

                # Also check for JSON-LD structured data
                if "jsonld" in pagemap:
                    jsonld_data = pagemap["jsonld"]
                    for item in jsonld_data:
                        if isinstance(item, dict):
                            if not meta["date_published"] and item.get("datePublished"):
                                meta["date_published"] = item["datePublished"]
                            elif not meta["date_published"] and item.get(
                                "publishedDate"
                            ):
                                meta["date_published"] = item["publishedDate"]
                            elif not meta["date_published"] and item.get("dateCreated"):
                                meta["date_published"] = item["dateCreated"]

                # Check in the main document data for additional date fields
                if not meta["date_published"]:
                    main_data_date_fields = [
                        "datePublished",
                        "publishedDate",
                        "date",
                        "created",
                        "modified",
                    ]
                    for field in main_data_date_fields:
                        if data.get(field):
                            meta["date_published"] = data[field]
                            break

            if any(meta.values()):
                aggregated_results.append(meta)

            if len(aggregated_results) >= total_results_needed:
                break

        if getattr(response, "next_page_token", ""):
            current_page_token = response.next_page_token
        else:
            break

    if not aggregated_results:
        return None

    titles = [r["title"] for r in aggregated_results if r.get("title")]
    descriptions = [
        r["description"] for r in aggregated_results if r.get("description")
    ]
    dates = [r["date_published"] for r in aggregated_results if r.get("date_published")]
    sources = [r["source"] for r in aggregated_results if r.get("source")]

    print(
        f"Search completed: {len(aggregated_results)} results over {len(pages_metadata)} pages"
    )
    # Create structured results for JSON output
    structured_results = []
    max_length = (
        max(len(titles), len(descriptions), len(dates), len(sources))
        if any([titles, descriptions, dates, sources])
        else 0
    )

    for i in range(max_length):
        result = {
            "title": titles[i] if i < len(titles) else None,
            "description": descriptions[i] if i < len(descriptions) else None,
            "date_published": dates[i] if i < len(dates) else None,
            "source": sources[i] if i < len(sources) else None,
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
        },
    }


if __name__ == "__main__":
    # Get user input
    try:
        total_results = int(input("Enter the number of results needed: "))
        domain = input("Enter the domain/search term: ")
    except ValueError:
        print("Invalid input for number of results. Using default value of 2.")
        total_results = 2
        domain = input("Enter the domain/search term: ")

    final_result = main(total_results, domain)

    print("\n Final Results:")
    if final_result:
        # Save to JSON
        save_to_json(final_result)

        results = final_result["results"]
        for i, result in enumerate(results):
            print(f"\n🔹 Result {i}")
            print(f"   Title       : {result['title'] or 'N/A'}")
            print(f"    Description : {result['description'] or 'N/A'}")
            print(f"     Published   : {result['date_published'] or 'N/A'}")
            print(f"    Source      : {result['source'] or 'N/A'}")
    else:
        print("No results found.")
