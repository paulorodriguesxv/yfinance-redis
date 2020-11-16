
# Yahho Finance Redis

Yahoo Finance Redis

Get data from Yahoo Finance through yfinance lib and push it to Redis.

# Install

pip install -r requirements.txt


## Environment Vars

Create a .env file with the following variables:

    REDIS_HOST=YourRedisHost
    REDIS_PORT=YorRedisPort
    REDIS_PASSWORD=YourRedisPasswd!
    SYMBOLSERVER_SYMBOL_FILE=symbols.txt

## Running

```bash
python ./server/app.py
```

## Result

On your Redis database '0', a key named MT5_SYMBOLSERVER_SYMBOLS will be created. 

Star if you like it.
---------------------
If you like or use this project, consider showing your support by starring it.