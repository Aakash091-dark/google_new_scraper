import json
import requests
from bs4 import BeautifulSoup
import time
import os
import re
import logging
from typing import Optional, Dict, List
import random
from urllib.parse import urlparse, urljoin
from datetime import datetime

# Configuration
OUTPUT_FOLDER = "scraped_news"
REQUEST_DELAY = (1, 3)  # Random delay between requests
TIMEOUT = 30
CONTENT_MIN_LENGTH = 200

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Create session with headers
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def remove_footer_lines(text: str) -> str:
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(
            phrase.lower() in line.lower()
            for phrase in [
                "download the",
                "log in",
                "sign up",
                "follow live",
                "subscribe",
                "read premium",
                "save your bookmarks",
            ]
        ):
            continue
        cleaned.append(line)
    return " ".join(cleaned)


def get_page_content(url: str) -> Optional[BeautifulSoup]:
    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        if "text/html" not in response.headers.get("content-type", ""):
            return None
        return BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None


def debug_page_structure(soup, url):
    """Debug function to analyze page structure"""
    logger.info(f"Debugging page structure for: {url}")

    # Look for common article containers
    potential_containers = [
        "article",
        "main",
        "[role='main']",
        ".article",
        ".story",
        ".content",
        "[data-testid*='article']",
        "[data-testid*='story']",
        "[data-testid*='content']",
        ".post-content",
        ".entry-content",
        ".article-content",
        ".story-content",
    ]

    for container in potential_containers:
        elements = soup.select(container)
        if elements:
            logger.info(f"Found {len(elements)} elements with selector: {container}")
            for i, elem in enumerate(elements[:2]):  # Check first 2
                text = clean_text(elem.get_text())
                logger.info(f"  Element {i+1} length: {len(text)} chars")
                if len(text) > 100:
                    logger.info(f"  Sample text: {text[:100]}...")

    # Look for paragraph content
    paragraphs = soup.find_all("p")
    total_p_text = " ".join([p.get_text() for p in paragraphs])
    logger.info(f"Total paragraph text length: {len(total_p_text)} chars")

    # Look for divs with substantial text
    divs = soup.find_all("div")
    text_divs = []
    for div in divs:
        text = clean_text(div.get_text())
        if len(text) > 200:
            classes = div.get("class", [])
            ids = div.get("id", "")
            text_divs.append(
                {
                    "classes": classes,
                    "id": ids,
                    "text_length": len(text),
                    "sample": text[:100],
                }
            )

    logger.info(f"Found {len(text_divs)} divs with substantial text")
    for div_info in text_divs[:5]:  # Show first 5
        logger.info(
            f"  Div - Classes: {div_info['classes']}, ID: {div_info['id']}, Length: {div_info['text_length']}"
        )
        logger.info(f"    Sample: {div_info['sample']}...")


def scrape_livemint_enhanced(soup, url):
    """Enhanced LiveMint scraper with more selectors and debugging"""
    logger.info("Using enhanced LiveMint scraper")

    # Updated selectors based on LiveMint's current structure
    selectors = [
        # Main article content selectors
        'div[data-vars-section="article"]',
        "div.contentSec",
        "div.mainArea",
        "div.textRubik",
        'div[class*="articleBody"]',
        "div.articlePage",
        "div.FirstEle",
        "div.paywall",
        "div.main-content",
        "div.article-content",
        "div.story-content",
        "div#content",
        "div.content",
        "article",
        "main",
        '[role="main"]',
        # More specific LiveMint selectors
        "div.storyContent",
        "div.storyDetails",
        "div.story-body",
        "div.article-body",
        "div.post-content",
        "div.entry-content",
        "section.article",
        "section.story",
        # Generic content selectors
        'div[class*="content"]',
        'div[class*="article"]',
        'div[class*="story"]',
        'div[class*="text"]',
        'div[id*="content"]',
        'div[id*="article"]',
        'div[id*="story"]',
    ]

    # Try each selector
    for selector in selectors:
        try:
            content = soup.select_one(selector)
            if content:
                text = clean_text(content.get_text())
                text = remove_footer_lines(text)
                if len(text) > CONTENT_MIN_LENGTH:
                    logger.info(f"Success with selector: {selector}")
                    return text
                else:
                    logger.debug(
                        f"Selector {selector} found content but too short: {len(text)} chars"
                    )
        except Exception as e:
            logger.debug(f"Error with selector {selector}: {e}")

    # If no selector works, try to find the largest text block
    logger.info("Trying to find largest text block...")

    # Look for divs with substantial text content
    divs = soup.find_all("div")
    best_content = ""
    best_length = 0

    for div in divs:
        try:
            text = clean_text(div.get_text())
            text = remove_footer_lines(text)
            if len(text) > best_length and len(text) > CONTENT_MIN_LENGTH:
                # Check if this looks like article content (not navigation, ads, etc.)
                if not any(
                    skip_word in text.lower()
                    for skip_word in [
                        "advertisement",
                        "sponsored",
                        "trending now",
                        "latest news",
                        "follow us",
                        "newsletter",
                        "subscription",
                        "terms of use",
                    ]
                ):
                    best_content = text
                    best_length = len(text)
        except Exception as e:
            continue

    if best_content:
        logger.info(
            f"Found content using largest text block method: {best_length} chars"
        )
        return best_content

    # Final fallback - try paragraph aggregation
    logger.info("Trying paragraph aggregation as final fallback...")
    paragraphs = soup.find_all("p")
    content = " ".join([p.get_text() for p in paragraphs])
    text = clean_text(content)
    text = remove_footer_lines(text)

    if len(text) > CONTENT_MIN_LENGTH:
        logger.info(f"Success with paragraph aggregation: {len(text)} chars")
        return text

    # If we still can't find content, run debug
    debug_page_structure(soup, url)

    return "Content not found"


def scrape_article_with_debug(url: str, source: str) -> Dict:
    """Enhanced article scraper with debugging"""
    soup = get_page_content(url)
    if not soup:
        return {"url": url, "source": source, "content": ""}

    if source == "LiveMint":
        content = scrape_livemint_enhanced(soup, url)
    else:
        content = get_scraper_for_source(source)(soup)

    return {"url": url, "source": source, "content": content}


# Test function for specific URLs
def test_livemint_urls():
    """Test the enhanced scraper on the problematic URLs"""
    test_urls = [
        "https://www.livemint.com/technology/can-nvidia-persuade-governments-to-pay-for-sovereign-ai-11752590985477.html",
        "https://www.livemint.com/news/world/nvidia-ceo-jensen-huang-says-china-s-military-unlikely-to-use-us-ai-chips-11752460408769.html",
        "https://www.livemint.com/market/stock-market-news/nvidia-google-tesla-shares-among-top-us-stocks-bought-by-indian-investors-in-q1fy26-11752729740100.html",
    ]

    results = []
    for url in test_urls:
        logger.info(f"Testing URL: {url}")
        result = scrape_article_with_debug(url, "LiveMint")
        results.append(result)
        print(f"URL: {url}")
        print(f"Content length: {len(result['content'])}")
        print(f"Content preview: {result['content'][:200]}...")
        print("-" * 80)
        time.sleep(2)

    return results


def get_page_content(url: str) -> Optional[BeautifulSoup]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        resp = session.get(url, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        if "text/html" not in resp.headers.get("content-type", ""):
            return None
        return BeautifulSoup(resp.content, "html.parser")
    except Exception as e:
        logger.error(f"Fetching {url} failed: {e}")
        return None


def scrape_pr_newswire(soup):
    logger.info("Using updated PR Newswire scraper")
    selectors = [
        "div.content-release",  # common main wrapper
        "div.release-body",  # classic format
        "div#main-content",  # fallback
        "div#newsContent",  # older version
        "article",  # just in case
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            text = clean_text(el.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                logger.info(f"Extracted with selector: {sel}")
                return text

    # Fallback: gather all paragraphs under release-body
    paragraphs = soup.select("div.release-body p")
    if paragraphs:
        combined = " ".join(p.get_text(strip=True) for p in paragraphs)
        text = clean_text(combined)
        if len(text) > CONTENT_MIN_LENGTH:
            logger.info("Extracted via paragraph fallback")
            return text

    logger.warning("PR Newswire scraper failed to extract meaningful content")
    return "Content not found"


def scrape_economic_times(soup):
    logger.info("Using Economic Times scraper")
    selectors = ["div.artText", "div.article_body", "div.Normal", "section.artText"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_livemint(soup):
    """Original LiveMint scraper - now calls enhanced version"""
    return scrape_livemint_enhanced(soup, "")


def scrape_thehindu(soup):
    logger.info("Using The Hindu scraper")
    selectors = [
        "div.articlebodycontent",
        'div[class*="article-body"]',
        'div[itemprop="articleBody"]',
    ]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_zee_news(soup):
    logger.info("Using Zee News scraper")
    selectors = [
        "div.article-content",
        "div#story",
        "div.articleText",
        "div.newsDetails",
    ]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_hindustan_times(soup):
    logger.info("Using Hindustan Times scraper")
    selectors = ["div.storyDetails", "div.htImport", 'div[class*="detail-body"]']
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_india_today(soup):
    logger.info("Using India Today scraper")
    selectors = ["div#story", "div.description", "div.article-body", "div#story-left"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_india_tv_news(soup):
    logger.info("Using India TV News scraper")
    selectors = ["div.article_content", "div#articleBody", "div.story-content"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_times_of_india(soup):
    logger.info("Using Times of India scraper")
    selectors = ["div.Normal", "div._s30J", "div.article_content"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_et_now(soup):
    logger.info("Using ET Now scraper")
    selectors = ["div.article__content", "div.content_box", "div.article_body"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_indian_express(soup):
    logger.info("Using Indian Express scraper")
    selectors = ["div.full-details", "div.ie-first-para", "div.article-details"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_news18(soup):
    logger.info("Using News18 scraper")
    selectors = ["div.article_container", "div#article_body", "div.storyContent"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_business_standard(soup):
    logger.info("Using Business Standard scraper")
    selectors = ["div.storyContent", "div#story-content", "div.article-content"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_deccan_herald(soup):
    logger.info("Using Deccan Herald scraper")
    selectors = ["div.article-main", "div.field-items", "div.article-content"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_firstpost(soup):
    logger.info("Using Firstpost scraper")
    selectors = ["div.text-copy", "div.article-full-content", "div.main-article"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_the_print(soup):
    logger.info("Using The Print scraper")
    selectors = [
        "div.tdb_single_post_content",
        "div.content-wrap",
        "div.td-post-content",
    ]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_the_wire(soup):
    logger.info("Using The Wire scraper")
    selectors = ["div.article-content", "div#article_content", "div.td-post-content"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_moneycontrol(soup):
    logger.info("Using Moneycontrol scraper")
    selectors = ["div#article-main", "div.content_wrapper", "div#story-main"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_free_press_journal(soup):
    logger.info("Using Free Press Journal scraper")
    selectors = ["div.article-detail", "div#story-content", "div.main-article-content"]
    for selector in selectors:
        content = soup.select_one(selector)
        if content:
            text = clean_text(content.get_text())
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                return text
    return "Content not found"


def scrape_generic(soup):
    logger.info("Using generic scraper")
    paragraphs = soup.find_all("p")
    content = " ".join([p.get_text() for p in paragraphs])
    text = clean_text(content)
    text = remove_footer_lines(text)
    if len(text) > CONTENT_MIN_LENGTH:
        return text
    return "Content not found"


def get_scraper_for_source(source: str):
    scrapers = {
        "PR Newswire": scrape_pr_newswire,
        "Economic Times": scrape_economic_times,
        "LiveMint": scrape_livemint,
        "The Hindu": scrape_thehindu,
        "Zee News": scrape_zee_news,
        "Hindustan Times": scrape_hindustan_times,
        "India Today": scrape_india_today,
        "India TV News": scrape_india_tv_news,
        "Times of India": scrape_times_of_india,
        "ET Now": scrape_et_now,
        "Indian Express": scrape_indian_express,
        "News18": scrape_news18,
        "Business Standard": scrape_business_standard,
        "Deccan Herald": scrape_deccan_herald,
        "Firstpost": scrape_firstpost,
        "The Print": scrape_the_print,
        "The Wire": scrape_the_wire,
        "Moneycontrol": scrape_moneycontrol,
        "Free Press Journal": scrape_free_press_journal,
    }
    return scrapers.get(source, scrape_generic)


def scrape_article(url: str, source: str) -> Dict:
    soup = get_page_content(url)
    if not soup:
        return {"url": url, "source": source, "content": ""}
    content = get_scraper_for_source(source)(soup)
    return {"url": url, "source": source, "content": content}


def save_to_json(articles: List[Dict], source: str, folder: str):
    filename = os.path.join(folder, f"{source.replace(' ', '_')}.json")
    os.makedirs(folder, exist_ok=True)
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except:
                existing = []
    else:
        existing = []
    urls = {a["url"] for a in existing}
    new_articles = [a for a in articles if a["url"] not in urls]
    if new_articles:
        existing.extend(new_articles)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(new_articles)} articles to {filename}")


def infer_source_name(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    domain = domain.replace("www.", "")
    mapping = {
        "prnewswire.com": "PR Newswire",
        "economictimes.indiatimes.com": "Economic Times",
        "livemint.com": "LiveMint",
        "thehindu.com": "The Hindu",
        "zeenews.india.com": "Zee News",
        "hindustantimes.com": "Hindustan Times",
        "indiatoday.in": "India Today",
        "indiatvnews.com": "India TV News",
        "timesofindia.indiatimes.com": "Times of India",
        "etnownews.com": "ET Now",
        "indianexpress.com": "Indian Express",
        "news18.com": "News18",
        "business-standard.com": "Business Standard",
        "deccanherald.com": "Deccan Herald",
        "firstpost.com": "Firstpost",
        "theprint.in": "The Print",
        "thewire.in": "The Wire",
        "moneycontrol.com": "Moneycontrol",
        "freepressjournal.in": "Free Press Journal",
    }
    return mapping.get(domain, domain.replace(".", "_"))


def main():
    logger.info("Reading URLs from search_results.json")
    try:
        with open("search_results.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            urls = [item["url"] for item in data.get("results", []) if "url" in item]
    except Exception as e:
        logger.error(f"Failed to load search_results.json: {e}")
        return

    grouped_articles = {}
    for i, url in enumerate(urls, 1):
        logger.info(f"[{i}/{len(urls)}] Processing: {url}")
        source = infer_source_name(url)
        article = scrape_article(url, source)
        grouped_articles.setdefault(source, []).append(article)
        time.sleep(random.uniform(*REQUEST_DELAY))

    for source, articles in grouped_articles.items():
        save_to_json(articles, source, OUTPUT_FOLDER)

    logger.info(" Done scraping all URLs.")


if __name__ == "__main__":

    main()
