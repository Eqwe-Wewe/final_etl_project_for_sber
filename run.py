from threading import Thread
from project import ETL
from config_db import config_db


def start(config):
    etl = ETL(config)
    etl.main()
    etl.pool.close()


if __name__ == '__main__':
    stream = Thread(target=start, args=[config_db])
    stream.start()
