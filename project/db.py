import cx_Oracle
import logging
import logging.config


logging.config.fileConfig('project/logging.conf')
logger = logging.getLogger('logger')


class DataBase:
    def __init__(self, config):
        logging.info('Инициализация cx_Oracle')
        self.__config = config
        self.open_conn()

    def open_conn(self):
        logging.info('Создание пула соединения')
        config = self.__config
        try:
            dsn = cx_Oracle.makedsn(
                config['host'], config['port'], config['SID']
            )
            self.pool = cx_Oracle.SessionPool(
                config['user'], config['password'], dsn, min=5,
                max=5, increment=1
            )
        except cx_Oracle.Error:
            logging.exception('Ошибка соединения с базой данных')
            print('Аварийное завершение работы приложения')
            raise SystemExit

    def get_report(self, dt):
        logging.info('Загрузка отчетов из базы данных', exc_info=True)
        with self.pool.acquire() as connect:
            try:
                cursor = connect.cursor()
                date = dt.strftime('%d.%m.%Y')
                res = cursor.execute(
                    f"""
                        SELECT *
                        FROM report
                        WHERE TRUNC(fraud_dt) = to_date('{date}', 'dd.mm.yyyy')
                        ORDER BY 1
                    """
                )
            except cx_Oracle.DatabaseError:
                logging.exception('Ошибка загрузки отчетов из базы данных')
            else:
                return res.fetchall()

    def insert_into_stg_trans(self, values, data):
        logging.info('Загрузка данных в STG_TRANSACTIONS')
        sql_query = f"""
                        INSERT INTO STG_TRANSACTIONS
                        VALUES({values})
                     """
        with self.pool.acquire() as connect:
            try:
                cursor = connect.cursor()
                cursor.executemany(sql_query, data)
            except cx_Oracle.DatabaseError:
                logging.exception('Ошибка загрузки данных в STG_TRANSACTIONS')
            else:
                connect.commit()

    def create_report(self):
        logging.info('Создание отчета в базе данных')
        try:
            with self.pool.acquire() as connect:
                cursor = connect.cursor()
                cursor.callproc('project.create_report')
        except cx_Oracle.DatabaseError:
            logging.exception('Ошибка создания отчета')
            return False
        else:
            return True

    def close(self):
        logging.info('Закрытие пула соединений', exc_info=True)
        self.pool.close()
