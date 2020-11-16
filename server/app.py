import logging
import redis
from decouple import config
import yfinance as yf

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level="INFO")
logger = logging.getLogger(__name__)

REDIS_HOST = config("REDIS_HOST", default="localhost")
REDIS_PORT = config("REDIS_PORT", default="6379")
REDIS_PASSWORD = config("REDIS_PASSWORD", default="Redis2019!")

SYMBOLS_FILE = config("SYMBOLSERVER_SYMBOL_FILE", default=None)
SYMBOL_KEY = "MT5_SYMBOLSERVER_SYMBOLS"

def remove_stock_exchange_name(symbols):
    return {key.split(".")[0]:symbols[key] for key in symbols }

def set_prices_on_redis(redis_conn, prices):
    redis_symbols = remove_stock_exchange_name(prices)
    redis_conn.hset(SYMBOL_KEY, mapping=redis_symbols)

def initialize_symbols(redis_conn):

    logger.info("Loading symbols from file...")

    symbols = {}
    with open(SYMBOLS_FILE) as f:
        for symbol in f.read().splitlines():
            symbols[symbol] = 0.0

    set_prices_on_redis(redis_conn, symbols)

    return symbols

def get_prices_from_yahoo(symbols):
    # for Bovespa stock market
    symbols_plain_text = " ".join(symbols)

    data = yf.download(
            tickers = symbols_plain_text,
            period = "1d",
            interval = "5m",
            group_by = 'ticker')

    filled_data = data.fillna(0).round(decimals=2)    
    prices = {key:filled_data[key].Close[-1] for key in symbols}

    return prices

try:
    logger.info("Yahoo Finance client is starting...")

    if not REDIS_HOST:
        raise ValueError("Please configure your Redis connection enviromment variables")

    redis_conn = redis.Redis(host=REDIS_HOST,
                             port=REDIS_PORT,
                             db=0,
                             password=REDIS_PASSWORD,
                             decode_responses=True)

    symbols = initialize_symbols(redis_conn)
    
    prices = get_prices_from_yahoo(symbols)

    set_prices_on_redis(redis_conn, prices)    

except ConnectionRefusedError as e:
    logger.error(f"Connection could not been established: {str(e)}")
except Exception as e:
    logger.error(f"Unknown error: {str(e)}")
    logger.error(f"Service is shutting down...")
    raise e
