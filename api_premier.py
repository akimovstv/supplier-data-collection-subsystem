"""
API поставщика Premier
"""

import argparse
import datetime
import logging
import os

import requests

import constants
import database
from items import PremierAPIPricingItem, PremierAPIInventoryItem
from suppliers import Premier


class Program:
    args = None  # type:argparse.Namespace

    @classmethod
    def run(cls):
        # Парсинг входных параметров программы
        cls.parse_program_arguments()

        print('Started.')
        print('Working...')

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
        api = PremierAPI(
            db=db,
            api_key='*******',
            apitest=cls.args.test,
            backup=cls.args.backup,
            limit=cls.args.limit,
        )

        # Запросы к API
        if cls.args.pricing:
            api.write_pricing_to_db()
        if cls.args.inventory:
            api.write_inventory_to_db()

        print('Finished.')

    @classmethod
    def parse_program_arguments(cls) -> None:
        """
        Парсит входные параметры программы
        """
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            description='Collect data to database with Premier API (https://*******/api/v5/)'
        )
        parser.add_argument(
            '--db-config',
            help='database config file (default: %(default)s)',
            dest='db_config',
            action='store',
            default=constants.DATABASE_DB_CONFIG_FILE
        )
        parser.add_argument(
            '--pricing',
            help='get data from /pricing?itemNumbers={itemNumber},{itemNumber}',
            dest='pricing',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '--inventory',
            help='get data from /inventory?itemNumbers={itemNumber},{itemNumber}',
            dest='inventory',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '-t', '--test',
            help='use sandbox (http://*******/api/v5/)',
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
                                                         constants.API_PREMIER_BACKUP_DIR))
        os.makedirs(constants.API_PREMIER_BACKUP_DIR, exist_ok=True)
        logging.info('{} Создание директории {}.'.format(constants.LOGGING_FINISH,
                                                         constants.API_PREMIER_BACKUP_DIR))


class PremierAPI:

    def __init__(
            self,
            *,
            db: database.Database,
            api_key: str,
            apitest: bool = False,
            backup: bool = False,
            limit: int = None
    ) -> None:
        """
        Инициализирует экземпляр входными параметрами и на их основе получает authorization_header
        :param db: экземпляр для работы з базой данных
        :param api_key: получил от Дениса
        :param apitest: флаг тестового режима
                    True - обращение идёт к http:/*******/api/v5/
                    False - обращение идёт к https://*******/api/v5/
        :param backup: сохранять ли данные в файлы отдельно от базы
        :param limit: ограничивает количество исходящих запросов (для тестирования)
        """
        self.db = db
        self.api_key = api_key
        self.apitest = apitest
        self.backup = backup
        self.limit = limit
        self._make_authorization_header()

        self.numbers_with_which_request_for_pricing_failed = None
        self.numbers_with_which_request_for_inventory_failed = None

        self.premier_items_ids = self.db.get_supplier_number_2_supplier_item_id(Premier.id_in_db)

    def _make_authorization_header(self) -> None:
        """Создаёт self.authorization_header"""
        try:
            logging.info('{} Getting authorization_header.'.format(constants.LOGGING_START))
            r = self.get_access_token()
            if r.status_code != 200:
                raise requests.RequestException(r.json(), response=r)
            d = r.json()
            logging.debug(d)
            self.authorization_header = {'Authorization': 'Bearer {}'.format(d['sessionToken'])}
        finally:
            logging.info('{} Getting authorization_header.'.format(constants.LOGGING_FINISH))

    def _url(self, path):
        return ('http://*******/api/v5/' if self.apitest
                else 'https://*******/api/v5/') + path

    def getting_writing_loop(
            self,
            *,
            numbers_iterator,
            item_type,
            get_data_api_function,
            item_to_dict_function,
            write_to_db_function,
            append_failed_numbers,
            failed_numbers,
            f_out
    ) -> None:
        """
        Основной цикл "Запрос к API"-"Запись в базу"
        :param numbers_iterator: итератор, который возвращает определённую порцию (список) с номерами Premier,
            для которых производится запрос с помощью API
        :param item_type: тип элемента получаемый по API (item из модуля items)
        :param get_data_api_function: функция запроса по API
        :param item_to_dict_function: функция преобразования элемента в словарь для записи в базу
        :param write_to_db_function: функция записи в базу данных
        :param append_failed_numbers: флаг - добавлять ли номера из неуспешных запросов в список для дальнейшей
            обработки
        :param failed_numbers: список для номеров из неуспешных запросов
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
                for number in numbers:  # В случае таймаута добавляем по одному в failed_numbers
                    failed_numbers.append([number])
                    continue
            else:
                if r.status_code == 401 and r.reason == 'Unauthorized':
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
                    for row in rows:
                        if f_out:
                            print(row, file=f_out)
                        item = item_type(row)  # Для успешных запросов разбираем данные на item'ы
                        try:
                            d = item_to_dict_function(item)
                        except KeyError:
                            continue
                        else:
                            data.append(d)
                    write_to_db_function(data)  # И записываем в базу
                else:  # Для других неуспешных запросов
                    logging.warning('Error: {} while {}'.format(r.text, r.url))
                    if append_failed_numbers:  # если нужно заносить номера в failed_numbers
                        for number in numbers:
                            failed_numbers.append([number])  # добавляем номера по одному в failed_numbers

    def get_access_token(self) -> requests.Response:
        # noinspection SpellCheckingInspection
        """
        Возвращает response с sessionToken
        Пример response.content:
            {
                "sessionToken":"*******"
            }
        """
        return requests.get(
            url=self._url('authenticate'),
            params={'apiKey': self.api_key},
            timeout=15
        )

    def get_pricing(self, numbers) -> requests.Response:
        # noinspection SpellCheckingInspection,SpellCheckingInspection
        """
        Пример response.content:
        [
            {
                'id': 152168,
                'itemNumber': 'YUKYPC1-GM7.5',
                'pricing': [{'cost': 26.999, 'jobber': 37.87, 'map': 40.6, 'retail': 40.6, 'currency': 'USD'},
                            {'cost': 36.791, 'jobber': 50.367, 'map': 53.998, 'retail': 53.998, 'currency': 'CAD'}]},
            {
                'id': 92799,
                'itemNumber': 'SCT2900',
                'pricing': [{'cost': 40.0, 'jobber': 40.0, 'map': 45.0, 'retail': 50.0, 'currency': 'USD'},
                            {'cost': 53.2, 'jobber': 53.2, 'map': 59.85, 'retail': 66.5, 'currency': 'CAD'}]
            },
            {
                'id': 644827,
                'itemNumber': 'AFWAFTH463D22-14',
                'pricing': [{'cost': 900.827, 'jobber': 1085.0, 'map': 1030.75, 'retail': 1085.0, 'currency': 'USD'},
                            {'cost': 1198.1, 'jobber': 1443.05, 'map': 1370.898, 'retail': 1443.05, 'currency': 'CAD'}]
            }
        ]
        """
        logging.debug('Requesting for pricing for {}...'.format(numbers))
        return requests.get(
            url=self._url('pricing'),
            headers=self.authorization_header,
            params={'itemNumbers': ','.join(numbers)},
            timeout=60
        )

    def get_inventory(self, numbers) -> requests.Response:
        # noinspection SpellCheckingInspection,SpellCheckingInspection
        """
        Пример response.content:
        {'itemNumber': 'A1T6-12VALVEHDSTD17-22A', 'inventory': [{'warehouseCode': 'UT-1-US', 'quantityAvailable': 0.0},
                                                                {'warehouseCode': 'KY-1-US', 'quantityAvailable': 0.0},
                                                                {'warehouseCode': 'TX-1-US', 'quantityAvailable': 6.0},
                                                                {'warehouseCode': 'CA-1-US', 'quantityAvailable': 7.0},
                                                                {'warehouseCode': 'AB-1-CA', 'quantityAvailable': 0.0},
                                                                {'warehouseCode': 'WA-1-US', 'quantityAvailable': 0.0},
                                                                {'warehouseCode': 'CO-1-US', 'quantityAvailable': 0.0},
                                                                {'warehouseCode': 'PO-1-CA', 'quantityAvailable': 0.0}]}
        """
        logging.debug('Requesting for inventory for {}...'.format(numbers))
        return requests.get(
            url=self._url('inventory'),
            headers=self.authorization_header,
            params={'itemNumbers': ','.join(numbers)},
            timeout=60
        )

    def _premier_numbers(self, portion_size=50):
        """
        Итератор, возвращающий из базы список номеров premier_number в количестве по portion_size штук
        """
        if portion_size <= 0:
            raise ValueError
        numbers = sorted(self.premier_items_ids)

        # print(numbers.index('CPC33-320-4'))
        # raise SystemExit
        numbers = numbers[147750:]  # todo: убрать это

        i = 0
        while True:
            if i >= len(numbers):
                break
            logging.debug('{}-{} of {}'.format(i, i + portion_size, len(numbers)))
            yield numbers[i:i + portion_size]
            i += portion_size

    def _dict_from_premier_api_pricing_item(self, item: PremierAPIPricingItem) -> dict:
        """
        Трансформирует item типа PremierAPIPricingItem в словарь с необходимыми для вставки в базу полями
        """
        supplier_item_id = self.premier_items_ids[item.premier_number]
        return {
            'supplier_item_id': supplier_item_id,
            'cost_usd': item.cost_usd,
            'jobber_usd': item.jobber_usd,
            'map_usd': item.map_usd,
            'retail_usd': item.retail_usd,
            'cost_cad': item.cost_cad,
            'jobber_cad': item.jobber_cad,
            'map_cad': item.map_cad,
            'retail_cad': item.retail_cad
        }

    def _dict_from_premier_api_inventory_item(self, item: PremierAPIInventoryItem) -> dict:
        """
        Трансформирует item типа PremierAPIInventoryItem в словарь с необходимыми для вставки в базу полями
        """
        supplier_item_id = self.premier_items_ids[item.premier_number]
        return {
            'supplier_item_id': supplier_item_id,
            'Qty_UT_1_US': item.Qty_UT_1_US,
            'Qty_KY_1_US': item.Qty_KY_1_US,
            'Qty_TX_1_US': item.Qty_TX_1_US,
            'Qty_CA_1_US': item.Qty_CA_1_US,
            'Qty_AB_1_CA': item.Qty_AB_1_CA,
            'Qty_WA_1_US': item.Qty_WA_1_US,
            'Qty_CO_1_US': item.Qty_CO_1_US,
            'Qty_PO_1_CA': item.Qty_PO_1_CA
        }

    def write_pricing_to_db(self) -> None:
        try:
            logging.info(
                '{} Сбор с помощью {}'.format(
                    constants.LOGGING_START,
                    '/pricing?itemNumbers={itemNumber},{itemNumber}'
                )
            )
            if self.backup:
                name = constants.API_PREMIER_PRICING_BACKUP_FILE_T.format(
                    datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                )
                logging.info('Writing {}...'.format(name))
                f_out = open(file=name, mode='w', encoding='utf8')
                del name
            else:
                f_out = None

            # Сначала для всех номеров
            self.numbers_with_which_request_for_pricing_failed = []
            numbers_iterator = self._premier_numbers()
            self.getting_writing_loop(
                numbers_iterator=numbers_iterator,
                item_type=PremierAPIPricingItem,
                get_data_api_function=self.get_pricing,
                item_to_dict_function=self._dict_from_premier_api_pricing_item,
                write_to_db_function=self.db.insert_into_premier_item__pricing,
                append_failed_numbers=True,
                failed_numbers=self.numbers_with_which_request_for_pricing_failed,
                f_out=f_out,
            )
            # Потом для каждого из номеров с ошибками в запросах
            numbers_iterator = iter(self.numbers_with_which_request_for_pricing_failed)
            self.getting_writing_loop(
                numbers_iterator=numbers_iterator,
                item_type=PremierAPIPricingItem,
                get_data_api_function=self.get_pricing,
                item_to_dict_function=self._dict_from_premier_api_pricing_item,
                write_to_db_function=self.db.insert_into_premier_item__pricing,
                append_failed_numbers=False,
                failed_numbers=self.numbers_with_which_request_for_pricing_failed,
                f_out=f_out,
            )
        finally:
            logging.info(
                '{} Сбор с помощью {}'.format(
                    constants.LOGGING_FINISH,
                    '/pricing?itemNumbers={itemNumber},{itemNumber}'
                )
            )

    def write_inventory_to_db(self) -> None:
        try:
            logging.info(
                '{} Сбор с помощью {}'.format(
                    constants.LOGGING_START,
                    '/inventory?itemNumbers={itemNumber},{itemNumber}'
                )
            )
            if self.backup:
                name = constants.API_PREMIER_INVENTORY_BACKUP_FILE_T.format(
                    datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                )
                logging.info('Writing {}...'.format(name))
                f_out = open(file=name, mode='w', encoding='utf8')
                del name
            else:
                f_out = None

            # Сначала для всех номеров
            self.numbers_with_which_request_for_inventory_failed = []
            numbers_iterator = self._premier_numbers()
            self.getting_writing_loop(
                numbers_iterator=numbers_iterator,
                item_type=PremierAPIInventoryItem,
                get_data_api_function=self.get_inventory,
                item_to_dict_function=self._dict_from_premier_api_inventory_item,
                write_to_db_function=self.db.insert_into_premier_item__inventory,
                append_failed_numbers=True,
                failed_numbers=self.numbers_with_which_request_for_inventory_failed,
                f_out=f_out,
            )
            # Потом для каждого из номеров с ошибками в запросах
            numbers_iterator = iter(self.numbers_with_which_request_for_inventory_failed)
            self.getting_writing_loop(
                numbers_iterator=numbers_iterator,
                item_type=PremierAPIInventoryItem,
                get_data_api_function=self.get_inventory,
                item_to_dict_function=self._dict_from_premier_api_inventory_item,
                write_to_db_function=self.db.insert_into_premier_item__inventory,
                append_failed_numbers=False,
                failed_numbers=self.numbers_with_which_request_for_inventory_failed,
                f_out=f_out,
            )
        finally:
            logging.info(
                '{} Сбор с помощью {}'.format(
                    constants.LOGGING_FINISH,
                    '/inventory?itemNumbers={itemNumber},{itemNumber}'
                )
            )


if __name__ == '__main__':
    Program.run()
