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
    """Clean and normalize text content"""
    if not text:
        return ""

    # Remove extra whitespace and newlines
    text = re.sub(r"\s+", " ", text.strip())

    # Remove common unwanted patterns
    text = re.sub(
        r"(Advertisement|Subscribe|Login|Register|Share|Tweet|Like|Follow)",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"(Read More|Continue Reading|View Full Article)", "", text, flags=re.IGNORECASE
    )
    text = re.sub(r"(Photo:|Image:|Video:|Source:)", "", text, flags=re.IGNORECASE)

    return text.strip()


def remove_footer_lines(text: str) -> str:
    """Remove footer/copyright lines from text"""
    if not text:
        return ""

    lines = text.split("\n")
    filtered_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip footer-like content
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
                "copyright",
                "all rights reserved",
                "terms",
                "privacy",
            ]
        ):
            continue

        filtered_lines.append(line)

    return " ".join(filtered_lines)


def get_page_content(url: str) -> Optional[BeautifulSoup]:
    """Fetch and parse web page content"""
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


def extract_content_with_fallback(soup, selectors, site_name=""):
    """Extract content with multiple fallback strategies"""
    logger.info(f"Using {site_name} scraper")

    # Try primary selectors
    for selector in selectors:
        try:
            content = soup.select_one(selector)
            if content:
                text = clean_text(content.get_text())
                text = remove_footer_lines(text)
                if len(text) > CONTENT_MIN_LENGTH:
                    logger.info(f"Success with selector: {selector}")
                    return text
        except Exception as e:
            logger.debug(f"Selector '{selector}' failed: {e}")
            continue

    # Fallback strategies
    fallback_strategies = [
        # Strategy 1: Common article tags
        lambda: soup.find("article"),
        lambda: soup.find("main"),
        lambda: soup.find("div", {"role": "main"}),
        # Strategy 2: Common content containers
        lambda: soup.find(
            "div", class_=re.compile(r"(content|article|story|post|body)", re.I)
        ),
        lambda: soup.find(
            "div", id=re.compile(r"(content|article|story|post|body)", re.I)
        ),
        # Strategy 3: Structured data
        lambda: soup.find("div", {"itemprop": "articleBody"}),
        lambda: soup.find("div", {"itemtype": "http://schema.org/Article"}),
        # Strategy 4: Paragraph aggregation
        lambda: soup.find("div", lambda x: x and len(x.find_all("p")) > 3),
    ]

    for strategy in fallback_strategies:
        try:
            content = strategy()
            if content:
                text = clean_text(content.get_text())
                text = remove_footer_lines(text)
                if len(text) > CONTENT_MIN_LENGTH:
                    logger.info(
                        f"Content extracted using fallback strategy for {site_name}"
                    )
                    return text
        except Exception as e:
            logger.debug(f"Fallback strategy failed: {e}")
            continue

    # Final fallback - aggregate all paragraphs
    try:
        paragraphs = soup.find_all("p")
        if paragraphs:
            content = " ".join([p.get_text() for p in paragraphs])
            text = clean_text(content)
            text = remove_footer_lines(text)
            if len(text) > CONTENT_MIN_LENGTH:
                logger.info(f"Using paragraph aggregation for {site_name}")
                return text
    except Exception as e:
        logger.error(f"Paragraph aggregation failed for {site_name}: {e}")

    # Emergency fallback - extract readable text
    try:
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()

        # Get text from body or html
        body = soup.find("body") or soup
        text = clean_text(body.get_text())
        text = remove_footer_lines(text)

        if len(text) > CONTENT_MIN_LENGTH:
            logger.warning(f"Using emergency fallback for {site_name}")
            return text
    except Exception as e:
        logger.error(f"Emergency fallback failed for {site_name}: {e}")

    return "Content not found"


def scrape_pr_newswire(soup):
    """PR Newswire scraper"""
    selectors = [
        "div.content-release",
        "div.release-body",
        "div#main-content",
        "div#newsContent",
        "article",
    ]
    return extract_content_with_fallback(soup, selectors, "PR Newswire")


def scrape_economic_times(soup):
    """Economic Times scraper"""
    selectors = [
        "div.etArticleText",
        "section.artText",
        "div.artText",
        "div#articleBody",
        "div.article_body",
        "div.Normal",
    ]
    return extract_content_with_fallback(soup, selectors, "Economic Times")


def scrape_livemint(soup):
    """LiveMint scraper"""
    selectors = [
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
        "div.storyContent",
        "div.storyDetails",
        "div.story-body",
        "div.article-body",
        "div.post-content",
        "div.entry-content",
        "section.article",
        "section.story",
        'div[class*="content"]',
        'div[class*="article"]',
        'div[class*="story"]',
        'div[class*="text"]',
        'div[id*="content"]',
        'div[id*="article"]',
        'div[id*="story"]',
    ]
    return extract_content_with_fallback(soup, selectors, "LiveMint")


def scrape_thehindu(soup):
    """The Hindu scraper"""
    selectors = [
        "div.articlebodycontent",
        "div.paywall",
        "div.article-content",
        'div[class*="article-body"]',
        'div[class*="story-body"]',
        'div[itemprop="articleBody"]',
        "div.content-body",
        "div.story-element-text",
        "section.paywall",
        "div.articletext",
        "div.story-content",
    ]
    return extract_content_with_fallback(soup, selectors, "The Hindu")


def scrape_zee_news(soup):
    """Zee News scraper"""
    selectors = [
        "div.article-content",
        "div#story",
        "div.articleText",
        "div.newsDetails",
        "div.story-text",
        "div.content-wrapper",
        "div.article-body",
        "div.news-content",
        "div.story-content",
        "div.main-content",
    ]
    return extract_content_with_fallback(soup, selectors, "Zee News")


def scrape_hindustan_times(soup):
    """Hindustan Times scraper"""
    selectors = [
        "div.storyDetails",
        "div.htImport",
        'div[class*="detail-body"]',
        "div.story-details",
        "div.content-wrapper",
        "div.article-content",
        "div.story-content",
        "div.main-content",
        "div.detail-content",
        "div.story-element",
    ]
    return extract_content_with_fallback(soup, selectors, "Hindustan Times")


def scrape_india_today(soup):
    """India Today scraper"""
    selectors = [
        "div#story",
        "div.description",
        "div.article-body",
        "div#story-left",
        "div.story-content",
        "div.content-wrapper",
        "div.main-content",
        "div.article-content",
        "div.story-details",
        "div.post-content",
    ]
    return extract_content_with_fallback(soup, selectors, "India Today")


def scrape_india_tv_news(soup):
    """India TV News scraper"""
    selectors = [
        "div.article_content",
        "div#articleBody",
        "div.story-content",
        "div.content-wrapper",
        "div.article-body",
        "div.news-content",
        "div.main-content",
        "div.story-text",
        "div.article-container",
        "div.story-details",
    ]
    return extract_content_with_fallback(soup, selectors, "India TV News")


def scrape_times_of_india(soup):
    """Times of India scraper"""
    selectors = [
        "div.Normal",
        "div._s30J",
        "div.article_content",
        "div.ga-headlines",
        "div.content",
        "div.story-content",
        "div.article-body",
        "div.main-content",
        "div._3YYSt",
        "div.story-text",
        "div.content-wrapper",
    ]
    return extract_content_with_fallback(soup, selectors, "Times of India")


def scrape_et_now(soup):
    """ET Now scraper"""
    selectors = [
        "div.article__content",
        "div.content_box",
        "div.article_body",
        "div.story-content",
        "div.article-content",
        "div.content-wrapper",
        "div.main-content",
        "div.post-content",
        "div.story-text",
        "div.article-container",
    ]
    return extract_content_with_fallback(soup, selectors, "ET Now")


def scrape_indian_express(soup):
    """Indian Express scraper"""
    selectors = [
        "div.full-details",
        "div.ie-first-para",
        "div.article-details",
        "div.story-content",
        "div.content-wrapper",
        "div.article-content",
        "div.main-content",
        "div.story-text",
        "div.post-content",
        "div.story-element",
    ]
    return extract_content_with_fallback(soup, selectors, "Indian Express")


def scrape_news18(soup):
    """News18 scraper"""
    selectors = [
        "div.article_container",
        "div#article_body",
        "div.storyContent",
        "div.story-content",
        "div.article-content",
        "div.content-wrapper",
        "div.main-content",
        "div.news-content",
        "div.story-text",
        "div.post-content",
    ]
    return extract_content_with_fallback(soup, selectors, "News18")


def scrape_business_standard(soup):
    """Business Standard scraper"""
    selectors = [
        "div.storyContent",
        "div#story-content",
        "div.article-content",
        "div.story-text",
        "div.content-wrapper",
        "div.main-content",
        "div.story-content",
        "div.article-body",
        "div.post-content",
        "div.story-element",
    ]
    return extract_content_with_fallback(soup, selectors, "Business Standard")


def scrape_deccan_herald(soup):
    """Deccan Herald scraper"""
    selectors = [
        "div.article-main",
        "div.field-items",
        "div.article-content",
        "div.story-content",
        "div.content-wrapper",
        "div.main-content",
        "div.story-text",
        "div.article-body",
        "div.post-content",
        "div.story-element",
    ]
    return extract_content_with_fallback(soup, selectors, "Deccan Herald")


def scrape_firstpost(soup):
    """Firstpost scraper"""
    selectors = [
        "div.text-copy",
        "div.article-full-content",
        "div.main-article",
        "div.story-content",
        "div.article-content",
        "div.content-wrapper",
        "div.main-content",
        "div.story-text",
        "div.post-content",
        "div.article-body",
    ]
    return extract_content_with_fallback(soup, selectors, "Firstpost")


def scrape_the_print(soup):
    """The Print scraper"""
    selectors = [
        "div.tdb_single_post_content",
        "div.content-wrap",
        "div.td-post-content",
        "div.story-content",
        "div.article-content",
        "div.main-content",
        "div.content-wrapper",
        "div.story-text",
        "div.post-content",
        "div.article-body",
    ]
    return extract_content_with_fallback(soup, selectors, "The Print")


def scrape_the_wire(soup):
    """The Wire scraper"""
    selectors = [
        "div.article-content",
        "div#article_content",
        "div.td-post-content",
        "div.story-content",
        "div.content-wrapper",
        "div.main-content",
        "div.story-text",
        "div.post-content",
        "div.article-body",
        "div.story-element",
    ]
    return extract_content_with_fallback(soup, selectors, "The Wire")


def scrape_moneycontrol(soup):
    """Moneycontrol scraper"""
    selectors = [
        "div#article-main",
        "div.content_wrapper",
        "div#story-main",
        "div.story-content",
        "div.article-content",
        "div.main-content",
        "div.content-body",
        "div.story-text",
        "div.post-content",
        "div.article-body",
    ]
    return extract_content_with_fallback(soup, selectors, "Moneycontrol")


def scrape_free_press_journal(soup):
    """Free Press Journal scraper"""
    selectors = [
        "div.article-detail",
        "div#story-content",
        "div.main-article-content",
        "div.story-content",
        "div.article-content",
        "div.content-wrapper",
        "div.main-content",
        "div.story-text",
        "div.post-content",
        "div.article-body",
    ]
    return extract_content_with_fallback(soup, selectors, "Free Press Journal")


def scrape_generic(soup):
    """Enhanced generic scraper with multiple strategies"""
    selectors = [
        "article",
        "main",
        'div[role="main"]',
        'div[class*="content"]',
        'div[class*="article"]',
        'div[class*="story"]',
        'div[class*="post"]',
        'div[id*="content"]',
        'div[id*="article"]',
        'div[id*="story"]',
    ]
    return extract_content_with_fallback(soup, selectors, "Generic")


# Source-to-scraper mapping
SOURCE_SCRAPER_MAP = {
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


def get_scraper_for_source(source: str):
    """Get appropriate scraper function for a given source"""
    return SOURCE_SCRAPER_MAP.get(source, scrape_generic)


def scrape_article(url: str, source: str) -> Dict:
    """Main scraping function"""
    soup = get_page_content(url)
    if not soup:
        return {"url": url, "source": source, "content": ""}

    content = get_scraper_for_source(source)(soup)
    return {"url": url, "source": source, "content": content}


def save_to_json(articles: List[Dict], source: str, folder: str):
    """Save articles to JSON file"""
    filename = os.path.join(folder, f"{source.replace(' ', '_')}.json")
    os.makedirs(folder, exist_ok=True)

    # Load existing articles
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except:
                existing = []
    else:
        existing = []

    # Add new articles (avoid duplicates)
    urls = {a["url"] for a in existing}
    new_articles = [a for a in articles if a["url"] not in urls]

    if new_articles:
        existing.extend(new_articles)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(new_articles)} articles to {filename}")
    else:
        logger.info(f"No new articles to save for {source}")


def infer_source_name(url: str) -> str:
    """Infer source name from URL"""
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


def test_livemint_urls():
    """Test the enhanced scraper on specific LiveMint URLs"""
    test_urls = [
        "https://www.livemint.com/technology/can-nvidia-persuade-governments-to-pay-for-sovereign-ai-11752590985477.html",
        "https://www.livemint.com/news/world/nvidia-ceo-jensen-huang-says-china-s-military-unlikely-to-use-us-ai-chips-11752460408769.html",
        "https://www.livemint.com/market/stock-market-news/nvidia-google-tesla-shares-among-top-us-stocks-bought-by-indian-investors-in-q1fy26-11752729740100.html",
    ]

    results = []
    for url in test_urls:
        logger.info(f"Testing URL: {url}")
        result = scrape_article(url, "LiveMint")
        results.append(result)
        print(f"URL: {url}")
        print(f"Content length: {len(result['content'])}")
        print(f"Content preview: {result['content'][:200]}...")
        print("-" * 80)
        time.sleep(2)

    return results


def main():
    """Main function to scrape articles from search results"""
    logger.info("Reading URLs from search_results.json")

    try:
        with open("search_results.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            urls = [item["url"] for item in data.get("results", []) if "url" in item]
    except Exception as e:
        logger.error(f"Failed to load search_results.json: {e}")
        return

    if not urls:
        logger.error("No URLs found in search_results.json")
        return

    logger.info(f"Found {len(urls)} URLs to process")

    grouped_articles = {}
    for i, url in enumerate(urls, 1):
        logger.info(f"[{i}/{len(urls)}] Processing: {url}")
        source = infer_source_name(url)
        article = scrape_article(url, source)

        if article["content"] and len(article["content"]) > CONTENT_MIN_LENGTH:
            grouped_articles.setdefault(source, []).append(article)
            logger.info(f"Successfully scraped {len(article['content'])} characters")
        else:
            logger.warning(f"Failed to scrape meaningful content from {url}")

        # Random delay between requests
        time.sleep(random.uniform(*REQUEST_DELAY))

    # Save articles grouped by source
    for source, articles in grouped_articles.items():
        save_to_json(articles, source, OUTPUT_FOLDER)

    logger.info(
        f"Scraping completed. Processed {len(urls)} URLs, saved {sum(len(articles) for articles in grouped_articles.values())} articles"
    )


if __name__ == "__main__":
    main()
