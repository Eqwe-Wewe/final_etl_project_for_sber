from datetime import datetime
from project.db import DataBase
from project.pdf import html_to_pdf
import pandas
import os
import json
import logging
import logging.config


logging.config.fileConfig('project/logging.conf')
logger = logging.getLogger('logger')


class ETL:
    __col_report = [
        'Fraud date', 'Passport', 'FIO',
        'Phone', 'Fraud type', 'Report date',
    ]

    def __init__(self, config):
        self.__get_conf()
        self.pool = DataBase(config)
        self.__user = config['user']

    def __get_conf(self):
        logging.info('Получение конфигурации по транзакциям')
        json_data = json.load(open('project/config.json', encoding='utf-8'))
        self.__last_xlsx = json_data['last_xlsx']
        self.__cur_last_id = json_data['last_id']

        logging.debug(f'{self.__last_xlsx = } ,{self.__cur_last_id = }')

    def __set_conf(self):
        logging.info('Актуализация данных по тразакциям')
        json.dump(
            {
                'last_xlsx': self.__last_xlsx,
                'last_id': self.__cur_last_id
            },
            open('project/config.json', 'w', encoding='utf-8'),
            indent=3
        )
        logging.debug(f'{self.__last_xlsx = } ,{self.__cur_last_id = }')

    def __reset_conf(self):
        logging.info('Сброс конфигурации по тразакциям')
        json.dump(
            {
                'last_xlsx': None,
                'last_id': -1
            },
            open('project/config.json', 'w')
        )
        self.__get_conf()

    def __get_xlsx(self):
        try:
            lst = [
                'transactions/' + i for i in os.listdir("transactions")
                if i.endswith(".xlsx")
            ]

            logging.debug(f'список транзакций в папке transactions = {lst}')
        except FileNotFoundError:
            logging.exception('Ошибка в работе метода ETL._ETL__get_xlsx')
        else:
            lst = sorted(lst)
            cur_xlsx = self.__last_xlsx
            self.__last_xlsx = lst[-1]

            # при первом запуске скрипта возвращает все файлы
            if cur_xlsx is None:
                lst_xlsx = lst

            # вернет неограниченное количество новых файлов
            else:
                lst_xlsx = lst[lst.index(cur_xlsx) + 1:]
            return lst_xlsx

        logging.debug(
            f'список переданных xlsx для дальнейшей обработки {lst_xlsx}'
        )

    def __get_report(self, dt: 'datetime'):
        logging.info('Начало работы метода ETL._ETL__get_report')
        resp_db = self.pool.get_report(dt)
        try:
            resp_db = pandas.DataFrame(resp_db)
            resp_db.columns = self.__col_report
        except ValueError:
            logging.exception('Ошибка в работе метода ETL._ETL__get_report')
        else:
            resp_db['Fraud date'] = resp_db['Fraud date']\
                                    .dt.strftime('%d.%m.%Y  %H:%M:%S')
            resp_db['Report date'] = resp_db['Report date']\
                                     .dt.strftime('%d.%m.%Y')
            html_to_pdf(
                'temp.html', resp_db.values.tolist(), self.__col_report,
                self.__user, dt.strftime('%d.%m.%Y'),
                datetime.today().strftime('%d.%m.%Y')
            )
            resp_db.set_index(self.__col_report[0], inplace=True)
            date_report = dt.strftime('%d%m%Y')
            resp_db.to_csv(f'reports/csv/report_{date_report}.csv', sep=';')
            resp_db.to_excel(f'reports/xlsx/report_{date_report}.xlsx')
        logging.info('Оконачние работы метода ETL._ETL__get_report')

    def main(self):
        logging.info('Начало работы ETL.main.')

        files = self.__get_xlsx()

        logging.debug(f'Список файлов xlsx для дальнейшей итерации = {files}')
        if files:
            for file in files:
                logging.debug(f'Файл в очереди = {file}')
                data_frame = pandas.read_excel(file)
                data_frame.rename(columns={'date': 'trans_date'}, inplace=True)
                last_id = data_frame.last_valid_index()
                date_report = data_frame.tail(1)['trans_date']\
                        .dt.to_pydatetime()[0]
                data_frame = data_frame.loc[self.__cur_last_id + 1:]
                self.__cur_last_id = last_id

                values = f":{', :'.join(data_frame.columns)}"
                data = data_frame.values.tolist()
                self.pool.insert_into_stg_trans(values, data)

                resp_db = self.pool.create_report()
                if not resp_db:
                    try:
                        raise StopIteration
                    except StopIteration:
                        logging.exception('Ошибка в цикле for метода ETL.main')
                        raise
                else:
                    self.__get_report(date_report)
                    self.__set_conf()
        else:
            print('Отчеты по последним данным уже составлены')

        logging.info('Окончание работы ETL.main.')
