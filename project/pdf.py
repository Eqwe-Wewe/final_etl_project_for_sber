from jinja2 import FileSystemLoader, Environment
import pdfkit
import logging
import logging.config


logging.config.fileConfig('project/logging.conf')
logger = logging.getLogger('logger')


def html_to_pdf(template: 'str', value: list, columns: list,
                author: str, date_report: str, date_create: str):
    logging.info('Запуск конвертации html в pdf', exc_info=True)
    file_loader = FileSystemLoader('project/templates')
    env = Environment(loader=file_loader)
    tm = env.get_template(template)  # 'temp.html'
    t = tm.render(
        date_report=date_report,
        title=columns,
        rows=value,
        author=author,
        date_create=date_create
    )
    pdfkit.from_string(
        t, f'reports/pdf/report_{date_report.replace(".","")}.pdf'
    )
    logging.info('Окончание конвертации', exc_info=True)
