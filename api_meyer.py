"""
API поставщика Meyer
"""

import argparse
import datetime
import logging
import os

import requests

import constants
import database
from items import MeyerAPIInformationItem
from suppliers import Meyer


class Program:
    args = None  # type:argparse.Namespace

    @classmethod
    def run(cls):
        # Парсинг входных параметров программы
        cls.parse_program_arguments()

        # Настройка логирования
        cls.setup_logging()

        # Подключение к базе данных
        db = database.Database(
            option_files=cls.args.db_config
        )

        # Создание директории для бэкапа
        if cls.args.backup:
            cls.make_dirs()

        # Инициализация API
        # noinspection SpellCheckingInspection
        api = MeyerAPI(
            db=db,
            username="*******",
            password="*******",
            apitest=cls.args.test,
            backup=cls.args.backup,
            limit=cls.args.limit,
        )

        # Запросы к API
        if 1 and cls.args.item_information:
            api.write_item_information_to_db()

        print('Finished.')

    @classmethod
    def parse_program_arguments(cls) -> None:
        """
        Парсит входные параметры программы
        """
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            description='Collect data to database with Meyer API'
                        ' (http://*******/http/default/ProdAPI/v2/)'
        )
        parser.add_argument(
            '--db-config',
            help='database config file (default: %(default)s)',
            dest='db_config',
            action='store',
            default=constants.DATABASE_DB_CONFIG_FILE
        )
        parser.add_argument(
            '--item-information',
            help='get data from /ItemInformation?ItemNumber={itemNumber},{itemNumber}',
            dest='item_information',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '-t', '--test',
            help='use sandbox (http://*******/http/default/TestAPI/v2/)',
            dest='test',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '-b', '--backup',
            help='backup collected data to files',
            dest='backup',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '--limit',
            help='limit number of requests (for testing)',
            dest='limit',
            type=int,
            action='store',
            default=None
        )
        parser.add_argument(
            '--logfile',
            help='log file (default: stderr)',
            dest='logfile',
            action='store',
            default=None
        )
        parser.add_argument(
            '--loglevel',
            help='logging level (default: %(default)s)',
            dest='loglevel',
            action='store',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO'
        )
        cls.args = parser.parse_args()

    @classmethod
    def setup_logging(cls) -> None:
        """
        Настраивает формат и уровень строгости для логирование
        """
        numeric_level = getattr(logging, cls.args.loglevel.upper(), None)

        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: {}'.format(cls.args.loglevel))
        logging.basicConfig(
            format='%(asctime)s %(levelname)s:%(message)s',
            level=numeric_level,
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(
                    filename=cls.args.logfile,
                    mode='w',
                    encoding='utf-8'
                ) if cls.args.logfile else logging.StreamHandler()]
        )
        logging.debug('Логирование настроено.')

    @classmethod
    def make_dirs(cls) -> None:
        """
        Создаёт директорию для временных файлов поставщиков
        """
        logging.info('{} Создание директории {}.'.format(constants.LOGGING_START,
                                                         constants.API_MEYER_BACKUP_DIR))
        os.makedirs(constants.API_MEYER_BACKUP_DIR, exist_ok=True)
        logging.info('{} Создание директории {}.'.format(constants.LOGGING_FINISH,
                                                         constants.API_MEYER_BACKUP_DIR))


class MeyerAPI:

    def __init__(
            self,
            *,
            db: database.Database,
            username: str,
            password: str,
            apitest: bool = False,
            backup: bool = False,
            limit: int = None
    ) -> None:
        """
        Инициализирует экземпляр входными параметрами и на их основе получает authorization_header
        :param db: экземпляр для работы з базой данных
        :param username: получил от Дениса
        :param password: получил от Дениса
        :param apitest: флаг тестового режима
            True - обращение идёт к  http://*******/http/default/ProdAPI/v2/
            False - обращение идёт к http://*******test/http/default/TestAPI/v2/
        :param backup: сохранять ли данные в файлы отдельно от базы
        :param limit: ограничивает количество исходящих запросов (для тестирования)
        """
        self.db = db
        self.username = username
        self.password = password
        self.apitest = apitest
        self.backup = backup
        self.limit = limit
        self._make_authorization_header()
        self.meyer_items_ids = self.db.get_supplier_number_2_supplier_item_id(Meyer.id_in_db)

    def _make_authorization_header(self) -> None:
        """Создаёт self.authorization_header"""
        # noinspection SpellCheckingInspection
        try:
            logging.info('{} Getting authorization_header.'.format(constants.LOGGING_START))
            r = self.get_access_token()
            if r.status_code != 200:
                raise requests.RequestException(r.json(), response=r)
            d = r.json()
            logging.debug(d)
            self.authorization_header = {'Authorization': f'Espresso {d["apikey"]}:1'}

        finally:
            logging.info('{} Getting authorization_header.'.format(constants.LOGGING_FINISH))

    def _url(self, path):
        return ('http://*******/http/default/TestAPI/v2/' if self.apitest
                else 'http://*******/http/default/ProdAPI/v2/') + path

    def getting_writing_loop(
            self,
            *,
            numbers_iterator,
            item_type,
            get_data_api_function,
            item_to_dict_function,
            write_to_db_function,
            f_out
    ) -> None:
        """
        Основной цикл "Запрос к API"-"Запись в базу"
        :param numbers_iterator: итератор, который возвращает определённую порцию (список) с номерами Meyer,
            для которых производится запрос с помощью API
        :param item_type: тип элемента получаемый по API (item из модуля items)
        :param get_data_api_function: функция запроса по API
        :param item_to_dict_function: функция преобразования элемента в словарь для записи в базу
        :param write_to_db_function: функция записи в базу данных
        :param f_out: файл бэкапа
        """
        retrying_to_authorize = False
        numbers = None
        request_no = 0
        while True:
            request_no += 1
            if self.limit:  # Ограничение для тестирования
                if request_no > self.limit:
                    break

            if not retrying_to_authorize:  # Не нужно переавторизоваться
                try:
                    numbers = next(numbers_iterator)  # Получаем следующую порцию номеров
                except StopIteration:
                    break  # Выход из цикла

            try:
                r = get_data_api_function(numbers)  # Запрос API с номерами
            except requests.exceptions.Timeout as e:
                logging.warning(e)
            else:
                if r.status_code == 500:
                    if not retrying_to_authorize:
                        retrying_to_authorize = True
                        self._make_authorization_header()  # Для неавторизованных запросов повторяем авторизацию
                        continue
                    else:
                        raise requests.RequestException(r.json(), response=r)
                retrying_to_authorize = False

                if r.status_code == 200:
                    data = []
                    rows = r.json()
                    # если rows - словарь, значит не найдены номера
                    if type(rows) == dict:
                        if rows['statusCode'] == 500:
                            continue
                    elif type(rows) == list:
                        for row in rows:
                            if f_out:  # todo: убрать, оставить и раскомментировать print что ниже
                                print(row, file=f_out)
                            item = item_type(row)  # Для успешных запросов разбираем данные на item'ы
                            try:
                                d = item_to_dict_function(item)
                            except KeyError:
                                continue
                            else:
                                data.append(d)
                        write_to_db_function(data)  # И записываем в базу
                    else:
                        logging.warning(rows)
                else:  # Для других неуспешных запросов
                    logging.warning('Error: {} while {}'.format(r.text, r.url))

    def get_access_token(self) -> requests.Response:
        # noinspection SpellCheckingInspection
        """
        Возвращает response с sessionToken
        Пример response.json:
            {
                'apikey': '*******',
                'expiration': '2019-02-14T12:06:21.468Z',
                'lastLoginTs': '2019-01-10T05:43:57.000Z',
                'lastLoginIP': '*******'
            }

        """
        return requests.post(
            url=self._url('Authentication'),
            json={
                "username": self.username,
                "password": self.password
            },
            timeout=15
        )

    def get_item_information(self, numbers) -> requests.Response:
        # noinspection SpellCheckingInspection,SpellCheckingInspection
        """
        Примеры:
        Response[200] - успешный:
        [
            {
                'Additional Handling Charge': 'No',
                'CustomerPrice': 317.88,
                'Height': 16,
                'ItemDescription': '11-14 MUSTANG V8-5.0L AIR INTAKE SYSTEM P5R; (POL)',
                'ItemNumber': 'AFE54-11982-P',
                'JobberPrice': 382.99,
                'Kit': 'No',
                'Kit Only': 'No',
                'LTL Required': 'No',
                'Length': 23,
                'ManufacturerID': 'AFE',
                'ManufacturerName': 'aFe Power',
                'MinAdvertisedPrice': 363.84,
                'Oversize': 'No',
                'PartStatus': 'Active',
                'QtyAvailable': 0,
                'SuggestedRetailPrice': None,
                'UPC': '802959503720',
                'Weight': 12,
                'Width': 13
            },
            {
                'Additional Handling Charge': 'No',
                'CustomerPrice': 275.55,
                'Height': 13,
                'ItemDescription': 'AIS P5R; DODGE TRUCKS 03-08 V8-5.7L',
                'ItemNumber': 'AFE54-11992',
                'JobberPrice': 331.99,
                'Kit': 'No',
                'Kit Only': 'No',
                'LTL Required': 'No',
                'Length': 23,
                'ManufacturerID': 'AFE',
                'ManufacturerName': 'aFe Power',
                'MinAdvertisedPrice': 315.39,
                'Oversize': 'No',
                'PartStatus': 'Active',
                'QtyAvailable': 0,
                'SuggestedRetailPrice': None,
                'UPC': '802959503737',
                'Weight': 14,
                'Width': 16
            }
        ]

        Response[200] - Не найдена информация по запрашиваемым номерам:
        {
            'statusCode': 500,
            'errorCode': 40501,
            'errorMessage': 'No results found'
        }

        Response[500] - неверный token
        b'{\n  "statusCode": 401,\n  "errorCode": 4012,\n  "errorMessage":
           "Auth Token cannot be accepted: Auth Token not found"\n} (401)\n'
        """
        logging.debug('Requesting item information for {}...'.format(numbers))
        return requests.get(
            url=self._url('ItemInformation'),
            headers=self.authorization_header,
            params={'ItemNumber': ','.join(numbers)},
            timeout=60
        )

    def _meyer_numbers(self, portion_size=50):
        """
        Итератор, возвращающий из базы список номеров premier_number в количестве по portion_size штук
        """
        if portion_size <= 0:
            raise ValueError
        numbers = sorted(self.meyer_items_ids)

        # print(numbers.index('CPC33-320-4'))
        # raise SystemExit
        # numbers = numbers[147750:]  # todo: убрать это

        i = 0
        while True:
            if i >= len(numbers):
                break
            logging.debug('{}-{} of {}'.format(i, i + portion_size, len(numbers)))
            yield numbers[i:i + portion_size]
            i += portion_size

    def _dict_from_meyer_api_information_item(self, item: MeyerAPIInformationItem) -> dict:
        """
        Трансформирует item типа PremierAPIPricingItem в словарь с необходимыми для вставки в базу полями
        """
        supplier_item_id = self.meyer_items_ids[item.ItemNumber]
        return {
            'supplier_item_id': supplier_item_id,
            'Customer_Price': item.CustomerPrice,
            'Height': item.Height,
            'Description': item.ItemDescription,
            'Jobber_Price': item.JobberPrice,
            'Kit': item.Kit,
            'Kit_Only': item.Kit_Only,
            'LTL_Required': item.LTL_Required,
            'Length': item.Length,
            'MAP': item.MinAdvertisedPrice,
            'Oversize': item.Oversize,
            'Discontinued': item.Discontinued,
            'QtyAvailable': item.QtyAvailable,
            'SuggestedRetailPrice': item.SuggestedRetailPrice,
            'UPC': item.UPC,
            'Weight': item.Weight,
            'Width': item.Width
        }

    def write_item_information_to_db(self) -> None:
        try:
            logging.info(
                '{} Сбор с помощью {}'.format(
                    constants.LOGGING_START,
                    'ItemInformation?ItemNumber={itemNumber},{itemNumber}...'
                )
            )
            if self.backup:
                name = constants.API_MEYER_ITEM_INFORMATION_BACKUP_FILE_T.format(
                    datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                )
                logging.info('Writing {}...'.format(name))
                f_out = open(file=name, mode='w', encoding='utf8')
                del name
            else:
                f_out = None

            # Сначала для всех номеров
            numbers_iterator = self._meyer_numbers()
            self.getting_writing_loop(
                numbers_iterator=numbers_iterator,
                item_type=MeyerAPIInformationItem,
                get_data_api_function=self.get_item_information,
                item_to_dict_function=self._dict_from_meyer_api_information_item,
                write_to_db_function=self.db.insert_into_meyer_item__item_information,
                f_out=f_out
            )
        finally:
            logging.info(
                '{} Сбор с помощью {}'.format(
                    constants.LOGGING_FINISH,
                    'ItemInformation?ItemNumber={itemNumber},{itemNumber}...'
                )
            )


if __name__ == '__main__':
    Program.run()
