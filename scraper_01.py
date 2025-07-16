from google.api_core.client_options import ClientOptions
from google.cloud import discoveryengine_v1 as discoveryengine
import os
import time
from typing import Optional
from urllib.parse import urlparse

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
                    meta.update(
                        {
                            "title": tags.get("og:title") or tags.get("title"),
                            "description": tags.get("og:description")
                            or tags.get("twitter:description"),
                            "date_published": tags.get("article:published_time"),
                            "source": tags.get("og:site_name"),
                        }
                    )

                    if not meta["source"] and tags.get("og:url"):
                        try:
                            meta["source"] = urlparse(tags["og:url"]).netloc
                        except:
                            pass

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
    return {
        "titles": titles or [],
        "descriptions": descriptions or [],
        "dates": dates or [],
        "sources": sources or [],
    }

if __name__ == "__main__":
    final_result = main(2, "warehouses in India")

    print("\n Final Results:")
    if final_result:
        result_count = len(final_result["titles"])
        for i in range(result_count):
            print(f"\n🔹 Result {i}")
            print(f"   Title       : {final_result['titles'][i]}")
            print(
                f"    Description : {final_result['descriptions'][i] if i < len(final_result['descriptions']) else 'N/A'}"
            )
            print(
                f"     Published   : {final_result['dates'][i] if i < len(final_result['dates']) else 'N/A'}"
            )
            print(
                f"    Source      : {final_result['sources'][i] if i < len(final_result['sources']) else 'N/A'}"
            )
    else:
        print("No results found.")
