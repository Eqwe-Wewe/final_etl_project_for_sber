from project import ETL
from config_db import config_db

if __name__ == '__main__':
    etl = ETL(config_db)
    etl.main()
    # etl.pool.close()
