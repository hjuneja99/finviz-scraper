from curl_cffi import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import re
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('PG_HOST', 'newsdb.ctws2qw08w49.eu-north-1.rds.amazonaws.com'),
    'port': os.getenv('PG_PORT', '5432'),
    'database': os.getenv('PG_DB', 'searchdb'),
    'user': os.getenv('PG_USER', 'Arash'),
    'password': os.getenv('PG_PASS', 'DCV0zBmL1!')  # Leave blank for trust auth
}

# Rotate user agents to avoid detection
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
]


def get_headers():
    """Get randomized headers"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    }


def get_db_connection(recreate=False):
    """Connect to the DB. If recreate=True, drop and recreate the DB."""
    dbname = DB_CONFIG['database']
    conn = None

    try:
        if recreate:
            # Connect to default 'postgres' DB to create target DB if missing
            tmp_config = DB_CONFIG.copy()
            tmp_config['database'] = 'postgres'
            conn = psycopg2.connect(**tmp_config)
            conn.autocommit = True
            cur = conn.cursor()

            print(f"Dropping and recreating database '{dbname}'...")
            cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s", (dbname,))
            cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
            cur.execute(f"CREATE DATABASE {dbname}")
            cur.close()
            conn.close()

        # Now connect to the target DB
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        return conn

    except Exception as e:
        print(f"Error connecting to or creating database '{dbname}': {e}")
        if conn:
            conn.close()
        raise


def init_db(conn):
    with conn.cursor() as cur:
        print("🗑️ Dropping and recreating tables...")
        cur.execute("DROP TABLE IF EXISTS stock_articles")
        cur.execute("DROP TABLE IF EXISTS news")
        cur.execute("DROP TABLE IF EXISTS stocks CASCADE")

        cur.execute("""
            CREATE TABLE stocks (
                id SERIAL PRIMARY KEY,
                ticker TEXT UNIQUE,
                company_name TEXT,
                market_cap NUMERIC,
                current_price NUMERIC,
                volume BIGINT,
                float_shares NUMERIC,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE news (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                title TEXT,
                url TEXT,
                datetime TIMESTAMP,
                UNIQUE(ticker, title, datetime)
            )
        """)

        cur.execute("""
            CREATE TABLE stock_articles (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                article_title TEXT,
                article_summary TEXT,
                article_url TEXT,
                article_date TIMESTAMP,
                UNIQUE(ticker, article_title, article_date)
            )
        """)
        conn.commit()


def parse_market_cap(market_cap_str):
    """Convert market cap string to number"""
    if not market_cap_str or market_cap_str == '-':
        return None

    # Remove $ and convert K, M, B to numbers
    value = market_cap_str.replace('$', '').replace(',', '')
    if 'B' in value:
        return float(value.replace('B', '')) * 1e9
    elif 'M' in value:
        return float(value.replace('M', '')) * 1e6
    elif 'K' in value:
        return float(value.replace('K', '')) * 1e3
    else:
        return float(value) if value else None


def parse_volume(volume_str):
    """Convert volume string to number"""
    if not volume_str or volume_str == '-':
        return None

    # Remove commas and convert K, M to numbers
    value = volume_str.replace(',', '')
    if 'M' in value:
        return int(float(value.replace('M', '')) * 1e6)
    elif 'K' in value:
        return int(float(value.replace('K', '')) * 1e3)
    else:
        return int(value) if value.isdigit() else None


def parse_float_millions(text):
    """Convert float‐share strings like '3.4M', '1.2B' to millions."""
    if not text or text == '-':
        return None
    text = text.replace(',', '')
    try:
        if text.endswith('B'):
            return float(text[:-1]) * 1000  # 1 B = 1 000 M
        if text.endswith('M'):
            return float(text[:-1])
        if text.endswith('K'):
            return float(text[:-1]) / 1000
        return float(text) / 1_000_000  # plain shares ⇒ millions
    except ValueError:
        return None


def fetch_float_from_quote(ticker):
    """Fetch float data for a single ticker"""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    max_retries = 4
    backoff_base = 3  # seconds

    for attempt in range(max_retries):
        try:
            r = requests.get(
                url,
                headers=get_headers(),
                timeout=15,
                impersonate="chrome110",
            )

            # Respect 429 / 503 responses with exponential back‑off
            if r.status_code in (429, 503):
                wait = backoff_base * (attempt + 1) + random.uniform(0, 2)
                print(f"      ↪ [{ticker}] HTTP {r.status_code} – retrying in {wait:.1f}s "
                      f"({attempt+1}/{max_retries})")
                time.sleep(wait)
                continue

            if r.status_code == 404:
                return None

            if r.status_code != 200:
                # Unexpected error; no more retries
                print(f"      ↪ [{ticker}] HTTP {r.status_code} – giving up.")
                return None

            # --- Parse HTML for “Shs Float” ---
            soup = BeautifulSoup(r.text, 'html.parser')

            # Method 1: snapshot tables (snapshot-table*, snapshot-td*)
            tables = soup.find_all('table', class_=re.compile(r'^snapshot-table'))
            for table in tables:
                cells = table.find_all(['td', 'th'])
                for i, cell in enumerate(cells):
                    if 'Shs Float' in cell.get_text(strip=True):
                        if i + 1 < len(cells):
                            float_text = cells[i + 1].get_text(strip=True)
                            result = parse_float_millions(float_text)
                            if result:
                                # Light delay to ease load
                                time.sleep(random.uniform(0.2, 0.4))
                                return result

            # Method 2: linear scan of all <td>
            all_tds = soup.find_all('td')
            for i, td in enumerate(all_tds):
                if 'Shs Float' in td.get_text(strip=True):
                    if i + 1 < len(all_tds):
                        float_text = all_tds[i + 1].get_text(strip=True)
                        result = parse_float_millions(float_text)
                        if result:
                            time.sleep(random.uniform(0.2, 0.4))
                            return result

            # Method 3: regex on raw HTML
            match = re.search(
                r'Shs\s+Float[^<]*</td>\s*<td[^>]*>\s*([\d\.,]+[BMK]?)',
                r.text,
                re.I,
            )
            if match:
                float_text = match.group(1).strip()
                result = parse_float_millions(float_text)
                if result is not None:
                    time.sleep(random.uniform(0.2, 0.4))
                    return result

            # If we get here, parsing failed – break loop, no point retrying
            print(f"      ↪ [{ticker}] Couldn’t parse float.")
            return None

        except Exception as e:
            wait = backoff_base * (attempt + 1) + random.uniform(0, 2)
            print(f"      ↪ [{ticker}] Error {e!s} – retrying in {wait:.1f}s "
                  f"({attempt+1}/{max_retries})")
            time.sleep(wait)
            continue

    return None

def fetch_float_and_price(ticker):
    """
    Return (float_in_millions, current_price) for a ticker by parsing its
    Finviz quote page. Retries & back-off logic is identical to the old
    fetch_float_from_quote().
    """
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    max_retries = 4
    backoff_base = 3

    for attempt in range(max_retries):
        try:
            r = requests.get(
                url,
                headers=get_headers(),
                timeout=15,
                impersonate="chrome110",
            )

            if r.status_code in (429, 503):
                wait = backoff_base * (attempt + 1) + random.uniform(0, 2)
                print(f"      ↪ [{ticker}] HTTP {r.status_code} – retry {attempt+1}/{max_retries} in {wait:.1f}s")
                time.sleep(wait)
                continue

            if r.status_code != 200:
                return (None, None)

            soup = BeautifulSoup(r.text, "html.parser")

            # --- Price ---
            price_val = None
            price_tag = soup.find("strong", class_="quote-price_wrapper_price")
            if price_tag:
                txt = price_tag.get_text(strip=True).replace(",", "").replace("$", "")
                try:
                    price_val = float(txt)
                except ValueError:
                    pass

            # --- Float (three-method cascade exactly like before) ---
            float_val = None

            # M-1: snapshot tables
            tables = soup.find_all("table", class_=re.compile(r"^snapshot-table"))
            for table in tables:
                cells = table.find_all(["td", "th"])
                for i, cell in enumerate(cells):
                    if "Shs Float" in cell.get_text(strip=True):
                        if i + 1 < len(cells):
                            float_text = cells[i + 1].get_text(strip=True)
                            float_val = parse_float_millions(float_text)
                            if float_val is not None:
                                break
                if float_val is not None:
                    break

            # M-2: linear scan if still not found
            if float_val is None:
                for i, td in enumerate(soup.find_all("td")):
                    if "Shs Float" in td.get_text(strip=True):
                        if i + 1 < len(soup.find_all("td")):
                            float_text = soup.find_all("td")[i + 1].get_text(strip=True)
                            float_val = parse_float_millions(float_text)
                        break

            # M-3: regex on raw HTML
            if float_val is None:
                m = re.search(
                    r"Shs\s+Float[^<]*</td>\s*<td[^>]*>\s*([\d\.,]+[BMK]?)",
                    r.text,
                    re.I,
                )
                if m:
                    float_val = parse_float_millions(m.group(1).strip())

            return (float_val, price_val)

        except Exception as e:
            wait = backoff_base * (attempt + 1) + random.uniform(0, 2)
            print(f"      ↪ [{ticker}] {e} – retry {attempt+1}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)

    return (None, None)

def _parse_screener_rows(rows):
    """Parse rows from the screener table"""
    parsed = []

    for idx, row in enumerate(rows):
        cells = row.find_all('td')

        if len(cells) < 11:
            continue

        # Find ticker
        ticker_elem = None
        ticker = None

        # Look for ticker in first few cells
        for i, cell in enumerate(cells[:5]):
            link = cell.find('a', class_='screener-link-primary')
            if link:
                text = link.text.strip()
                if text and text.isupper() and 1 <= len(text) <= 5:
                    ticker_elem = link
                    ticker = text
                    break

        # Alternative: look for uppercase link
        if not ticker_elem:
            for i, cell in enumerate(cells[:3]):
                link = cell.find('a')
                if link:
                    text = link.text.strip()
                    if text and text.isupper() and 1 <= len(text) <= 5:
                        ticker_elem = link
                        ticker = text
                        break

        if not ticker:
            continue

        # Extract other fields
        try:
            # Find company name
            company_name = None
            for i in range(1, min(len(cells), 5)):
                text = cells[i].text.strip()
                if text and len(text) > 3 and not text.replace('.', '').replace(',', '').replace('-', '').isdigit():
                    company_name = text
                    break

            if not company_name:
                company_name = ticker

            # Look for market cap
            market_cap = None
            for i in range(5, min(len(cells), 9)):
                text = cells[i].text.strip()
                if '$' in text or 'B' in text or 'M' in text:
                    market_cap = parse_market_cap(text)
                    if market_cap:
                        break

            # Look for volume
            volume = None
            for i in range(max(8, len(cells) - 3), len(cells)):
                text = cells[i].text.strip()
                vol = parse_volume(text)
                if vol and vol > 100:
                    volume = vol
                    break

            parsed.append({
                'ticker': ticker,
                'company_name': company_name,
                'market_cap': market_cap,
                'volume': volume,
                'float_shares': None
            })

        except Exception as e:
            continue

    return parsed


def get_stocks_from_screener(max_float=10, workers=5):
    """Get all stocks with float under max_float millions"""
    base_url = "https://finviz.com/screener.ashx?v=111&f=sh_float_u10"
    all_stocks = []
    start = 1

    print("📊 Fetching stocks from screener...")

    while start <= 1261:  # Safety limit
        url = base_url if start == 1 else f"{base_url}&r={start}"
        print(f"  Page {((start - 1) // 20) + 1}: Fetching stocks {start} to {start + 19}...")

        try:
            response = requests.get(
                url,
                headers=get_headers(),
                impersonate="chrome110",
                timeout=30
            )

            if response.status_code != 200:
                print(f"  ❌ HTTP {response.status_code}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the screener table
            table = None
            for t in soup.find_all('table'):
                if t.find('a', class_='screener-link-primary'):
                    table = t
                    break

            if not table:
                # Alternative: find by ticker pattern
                for t in soup.find_all('table'):
                    links = t.find_all('a')
                    ticker_count = sum(
                        1 for link in links if link.text.strip().isupper() and 1 <= len(link.text.strip()) <= 5)
                    if ticker_count > 5:
                        table = t
                        break

            if not table:
                print("  ❌ No more data")
                break

            # Parse rows
            rows = table.find_all('tr')
            data_rows = []
            for row in rows:
                if row.find('th') or 'Ticker' in row.text:
                    continue
                if len(row.find_all('td')) >= 10:
                    data_rows.append(row)

            if not data_rows:
                print("  ❌ No data rows found")
                break

            parsed = _parse_screener_rows(data_rows)
            if not parsed:
                print("  ❌ No stocks parsed on this page, continuing...")
            else:
                all_stocks.extend(parsed)
                print(f"  ✅ Found {len(parsed)} stocks (total: {len(all_stocks)})")

            # If fewer than 20 rows were **parsed**, it may just mean some rows
            # were skipped (e.g., SPACs with no ticker link). We continue paging
            # until the safety limit is hit or no table is returned.

            start += 20
            time.sleep(random.uniform(1, 2))  # Be polite

        except Exception as e:
            print(f"  ❌ Error: {e}")
            break

    if not all_stocks:
        return []

    print(f"\n⏳ Fetching float data for {len(all_stocks)} stocks...")

    # Create a map for easy lookup
    stock_map = {s['ticker']: s for s in all_stocks}

    # Fetch float data with thread pool
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_float_and_price, ticker): ticker for ticker in stock_map}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                float_val, price_val = future.result()                # Log the float value we just fetched
                print(
                    f"    • {ticker}: price {price_val if price_val is not None else 'N/A'}  | "
                    f"float {float_val if float_val is not None else 'N/A'} M"
                )
                stock_map[ticker]['float_shares'] = float_val
                stock_map[ticker]["current_price"] = price_val
                completed += 1

                # Progress update every 10 stocks
                if completed % 10 == 0:
                    print(f"  Progress: {completed}/{len(all_stocks)} stocks processed")

            except Exception as e:
                print(f"  Error with {ticker}: {e}")

    # Filter by float
    filtered = [s for s in all_stocks if s['float_shares'] and s['float_shares'] <= max_float]
    return filtered


def get_stock_news(ticker, max_retries=3):
    """Fetch news for a ticker"""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    news_items = []

    for attempt in range(max_retries):
        try:
            r = requests.get(
                url,
                headers=get_headers(),
                timeout=15,
                impersonate="chrome110",
            )

            if r.status_code == 404:
                return []

            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')

                # Find news table
                news_table = soup.find('table', class_='fullview-news-outer')
                if not news_table:
                    # Alternative: find table containing news links
                    for table in soup.find_all('table'):
                        if table.find('a', href=re.compile(r'https?://')):
                            links = table.find_all('a')
                            if len(links) > 3 and any('news' in str(link).lower() for link in links):
                                news_table = table
                                break

                if news_table:
                    rows = news_table.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            date_text = cols[0].text.strip()
                            link = cols[1].find('a')

                            if link:
                                title = link.text.strip()
                                news_url = link.get('href', '')
                                date = parse_news_date(date_text)

                                news_items.append({
                                    "ticker": ticker,
                                    "title": title,
                                    "summary": None,
                                    "url": news_url,
                                    "date": date,
                                })

                break  # Success

            elif r.status_code == 429:
                wait = random.uniform(5, 10) * (attempt + 1)
                print(f"  💤 [{ticker}] 429 Too Many Requests – sleeping {wait:.1f}s before retry {attempt+1}/{max_retries}")
                time.sleep(wait)
                continue

        except Exception:
            pass

    time.sleep(random.uniform(0.5, 1.5))
    return news_items


def parse_news_date(date_str):
    """Parse Finviz news date format"""
    if not date_str:
        return datetime.now()

    try:
        now = datetime.now()

        # Handle time-only format (e.g., "09:49AM")
        if re.match(r'^\d{1,2}:\d{2}[AP]M$', date_str):
            return datetime.strptime(f"{now.strftime('%Y-%m-%d')} {date_str}", "%Y-%m-%d %I:%M%p")

        # Handle "Today" format
        if 'Today' in date_str:
            time_part = date_str.replace('Today', '').strip()
            if time_part:
                return datetime.strptime(f"{now.strftime('%Y-%m-%d')} {time_part}", "%Y-%m-%d %I:%M%p")
            return now

        # Handle "Yesterday" format
        if 'Yesterday' in date_str:
            time_part = date_str.replace('Yesterday', '').strip()
            yesterday = now - timedelta(days=1)
            if time_part:
                return datetime.strptime(f"{yesterday.strftime('%Y-%m-%d')} {time_part}", "%Y-%m-%d %I:%M%p")
            return yesterday

        # Handle full date format (e.g., "Dec-28-24 08:00PM")
        if re.match(r'^[A-Za-z]{3}-\d{2}-\d{2}', date_str):
            return datetime.strptime(date_str, "%b-%d-%y %I:%M%p")

        # Handle month-day format (e.g., "Dec-28 08:00PM")
        if re.match(r'^[A-Za-z]{3}-\d{1,2}', date_str):
            parts = date_str.split(' ')
            if len(parts) >= 2:
                date_part = parts[0]
                time_part = parts[1]
                return datetime.strptime(f"{date_part}-{now.year} {time_part}", "%b-%d-%Y %I:%M%p")

    except Exception:
        pass

    return datetime.now()


def save_stock_to_db(conn, stock_data):
    """Save or update stock in database"""
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO stocks (
                ticker, company_name, market_cap, current_price, volume, float_shares
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                market_cap = EXCLUDED.market_cap,
                current_price = EXCLUDED.current_price,
                volume = EXCLUDED.volume,
                float_shares = EXCLUDED.float_shares,
                last_updated = CURRENT_TIMESTAMP
            RETURNING ticker
        """, (
            stock_data['ticker'],
            stock_data['company_name'],
            stock_data['market_cap'],
            stock_data['current_price'],
            stock_data['volume'],
            stock_data['float_shares']
        ))

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        print(f"Error saving stock {stock_data['ticker']}: {e}")
        return False
    finally:
        cur.close()


def save_news_to_db(conn, news_items):
    """Save news articles to database"""
    cur = conn.cursor()
    saved_count = 0

    for news in news_items:
        try:
            # Check if article already exists
            cur.execute("""
                SELECT id FROM stock_articles 
                WHERE ticker = %s AND article_title = %s
            """, (news['ticker'], news['title']))

            if cur.fetchone():
                continue  # Skip if already exists

            # Insert new article
            cur.execute("""
                INSERT INTO stock_articles (
                    ticker, article_title, article_summary, article_url, article_date
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                news['ticker'],
                news['title'],
                news['summary'],
                news['url'],
                news['date']
            ))

            saved_count += 1

        except Exception:
            continue

    conn.commit()
    cur.close()
    return saved_count


def fetch_and_save_news(stock):
    """Fetch news for a stock and save to database"""
    try:
        news_items = get_stock_news(stock['ticker'])
        if not news_items:
            return 0

        conn = get_db_connection()
        saved = save_news_to_db(conn, news_items)
        conn.close()
        return saved

    except Exception:
        return 0


def main():
    """Main function to run the scraper"""
    print("🚀 Starting Finviz scraper...")
    print(f"🔄 Connecting to database '{DB_CONFIG['database']}' at {DB_CONFIG['host']}")

    # Connect to database and recreate it
    conn = get_db_connection(recreate=True)
    init_db(conn)

    try:
        # Get all stocks with float under 10M
        print("\n📈 STEP 1: Fetching stocks with float < 10M...")
        stocks = get_stocks_from_screener(max_float=10, workers=5)

        if not stocks:
            print("\n❌ No stocks found.")
            return

        print(f"\n✅ Found {len(stocks)} stocks with float < 10M")
        print(f"📊 Sample tickers: {[s['ticker'] for s in stocks[:10]]}")

        # Save stocks to database
        print("\n💾 STEP 2: Saving stocks to database...")
        saved_stocks = 0
        for stock in stocks:
            if save_stock_to_db(conn, stock):
                saved_stocks += 1
        print(f"✅ Saved {saved_stocks} stocks to database")

        # Fetch news for each stock
        print(f"\n📰 STEP 3: Fetching news for {len(stocks)} stocks...")
        print("This may take a while. Logging each ticker as it's processed.")

        total_news = 0
        for i, stock in enumerate(stocks, 1):
            print(f"🔎 [{i}/{len(stocks)}] {stock['ticker']}: fetching news...", end='', flush=True)
            saved = fetch_and_save_news(stock)
            total_news += saved
            print(f" done – {saved} new article{'s' if saved != 1 else ''}.")

            # Light, randomised delay to avoid hammering Finviz
            time.sleep(random.uniform(1, 2))

            # Longer pause every 50 tickers to respect rate limits
            if i % 50 == 0 and i != len(stocks):
                print("  🚨 50 tickers reached – pausing for 10 s to avoid rate limits...")
                time.sleep(10)

        print(f"\n🎉 SCRAPING COMPLETE!")
        print(f"\n📊 FINAL SUMMARY:")
        print(f"   • Total stocks found: {len(stocks)}")
        print(f"   • Stocks saved to DB: {saved_stocks}")
        print(f"   • News articles saved: {total_news}")
        print(f"   • Average articles per stock: {total_news / len(stocks):.1f}")

    except Exception as e:
        print(f"\n❌ Error in main: {e}")
        import traceback
        traceback.print_exc()

    finally:
        conn.close()
        print("\n👋 Done!")


if __name__ == "__main__":
    main()
