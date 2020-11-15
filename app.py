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

def set_symbols_from_file(redis_conn):
    symbols = {}
    with open(SYMBOLS_FILE) as f:
        for symbol in f.readlines():
            symbols[symbol.replace("\n", "")] = 0.0

    redis_conn.hset(SYMBOL_KEY, mapping=symbols)
    return symbols


def get_symbols(redis_conn):
    symbols = redis_conn.hgetall(SYMBOL_KEY)
    if symbols:
        return symbols

    logger.info("Redis não inicializado. Buscando simbolos no banco de dados")

    if set_symbols_from_file(redis_conn):
        symbols = redis_conn.hgetall(SYMBOL_KEY)

    if not symbols:
        raise ValueError(f"Nenhum simbolo foi encontrado para busca")

    return symbols

def get_prices_from_yahoo(symbols):
    symbols_keys = [f"{key}.SA" for key in symbols]
    symbols_plain_text = " ".join(symbols_keys)

    
    data = yf.download(
            tickers = symbols_plain_text,
            period = "1d",
            interval = "5m",
            group_by = 'ticker')

    filled_data = data.fillna(0).round(decimals=2)    
    prices = {key:filled_data[f"{key}.SA"].Close[-1] for key in symbols}

    return prices

try:
    logger.info("Yahoo Finance client is starting...")

    if not REDIS_HOST:
        raise ValueError("É necessário informar o endereço do REDIS")

    redis_conn = redis.Redis(host=REDIS_HOST,
                             port=REDIS_PORT,
                             db=0,
                             password=REDIS_PASSWORD,
                             decode_responses=True)
    # warmup
    try:
        symbols = get_symbols(redis_conn)
    except Exception as e:
        logger.error("Warm-up: " + str(e))
    
    prices = get_prices_from_yahoo(symbols)
    redis_conn.hset(SYMBOL_KEY, mapping=prices)

except ConnectionRefusedError as e:
    logger.error(f"A conexão com o redis não pode ser efetuada: {str(e)}")
except Exception as e:
    logger.error(f"Erro desconhecido: {str(e)}")
    logger.error(f"Service is shutting down... {str(e)}")
    raise e


"""
data = yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers = "PETR4.SA",

        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        period = "1d",
        interval = "5m",
        group_by = 'ticker')

print(data)        
"""