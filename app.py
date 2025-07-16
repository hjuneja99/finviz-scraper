from flask import Flask, request, jsonify,render_template
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import threading
import datetime
import pytz

load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for local dev

DB_CONFIG = {
    'host': os.getenv('PG_HOST', 'localhost'),
    'port': os.getenv('PG_PORT', '5432'),
    'database': os.getenv('PG_DB', 'searchdb'),
    'user': os.getenv('PG_USER', 'hiteshjuneja'),
    'password': os.getenv('PG_PASS', '')
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)



@app.route('/api/search', methods=['GET'])
def search_articles():
    # ---------------------------------------
    # 1) Parse query-string parameters
    # ---------------------------------------
    raw_q      = request.args.get('q', '').strip()
    float_max  = request.args.get('float_max', '10').strip()

    if not raw_q:
        return jsonify({'error': 'Missing search query'}), 400

    keywords = [kw.strip() for kw in raw_q.split(',') if kw.strip()]
    if not keywords:
        return jsonify({'error': 'No valid keywords supplied'}), 400

    try:
        float_limit = float(float_max)
    except ValueError:
        return jsonify({'error': 'Invalid float value'}), 400

    # ---------------------------------------
    # 2) Build dynamic SQL for many keywords
    #    a.article_title ILIKE ANY(%s) etc.
    # ---------------------------------------
    # Create a list like ['%merger%', '%split%']
    like_patterns = [f'%{kw}%' for kw in keywords]

    sql = """
        SELECT
            s.ticker,
            s.company_name,
            s.current_price,
            s.market_cap,
            s.volume,
            s.float_shares,
            -- JSON array of articles (newest-to-oldest)
            json_agg(
                json_build_object(
                    'title',   a.article_title,
                    'summary', a.article_summary,
                    'url',     a.article_url,
                    'date',    a.article_date
                )
                ORDER BY a.article_date DESC
            )               AS articles,
            MAX(a.article_date) AS latest_news     -- used for sorting stocks
        FROM stocks s
        JOIN stock_articles a USING (ticker)
        WHERE (
            a.article_title   ILIKE ANY (%s) OR
            a.article_summary ILIKE ANY (%s)
        )
          AND s.float_shares <= %s
        GROUP BY
            s.ticker,
            s.company_name,
            s.current_price,
            s.market_cap,
            s.volume,
            s.float_shares
        ORDER BY latest_news DESC;                  -- newest-news first
    """

    conn = get_db_connection()
    cur  = conn.cursor()

    try:
        cur.execute(sql, (like_patterns, like_patterns, float_limit))
        rows = cur.fetchall()
        return jsonify({'results': rows}), 200
    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/stocks/<ticker>/articles', methods=['GET'])
def get_stock_articles(ticker):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT article_title, article_summary, article_url, article_date
            FROM stock_articles
            WHERE ticker = %s
            ORDER BY article_date DESC
        """, (ticker.upper(),))

        articles = cur.fetchall()
        return jsonify({'ticker': ticker, 'articles': articles})

    except Exception as e:
        print(f"Article fetch error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

    finally:
        cur.close()
        conn.close()

scrape_status = {
    'status': 'idle',  # 'idle', 'scraping', 'done', 'error'
    'last_scraped': None,
    'error': None
}

def run_scraper():
    global scrape_status
    try:
        scrape_status['status'] = 'scraping'
        scrape_status['error'] = None
        from finviz_scraper import main as run_main
        run_main()
        scrape_status['status'] = 'done'
        scrape_status['last_scraped'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    except Exception as e:
        scrape_status['status'] = 'error'
        scrape_status['error'] = str(e)

@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    global scrape_status
    if scrape_status['status'] == 'scraping':
        return jsonify({'status': 'scraping', 'message': 'Scraping already in progress.'}), 409
    t = threading.Thread(target=run_scraper)
    t.start()
    return jsonify({'status': 'started'})

@app.route('/api/scrape_status', methods=['GET'])
def get_scrape_status():
    global scrape_status
    status = scrape_status.copy()
    if status['last_scraped']:
        try:
            utc_dt = datetime.datetime.fromisoformat(status['last_scraped'])
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=datetime.timezone.utc)
            dk_tz = pytz.timezone('Europe/Copenhagen')
            dk_dt = utc_dt.astimezone(dk_tz)
            status['last_scraped'] = dk_dt.strftime('%Y-%m-%dT%H:%M:%S%z')
        except Exception:
            pass
    return jsonify(status)


if __name__ == '__main__':
    app.run(port=5001, debug=True)