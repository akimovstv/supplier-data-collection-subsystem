"""
API поставщика Turn14
"""

import argparse
import datetime
import logging
import os

import requests

import constants
import database
from items import Turn14APIAllItemDataItem, Turn14APIAllItemsItem
from suppliers import Turn14


class Program:
    args = None  # type:argparse.Namespace

    @classmethod
    def run(cls):
        # Парсинг входных параметров программы
        cls.parse_program_arguments()

        print(cls.args)  # todo
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
        api = Turn14API(
            db=db,
            client_id="*******",
            client_secret="*******",
            apitest=cls.args.test,
            backup=cls.args.backup,
            limit=cls.args.limit
        )

        # Запросы к API
        if cls.args.all_items:
            api.write_all_items_to_db()
        if cls.args.all_item_data:
            api.write_all_item_data_to_db()

        print('Finished.')

    @classmethod
    def parse_program_arguments(cls) -> None:
        """
        Парсит входные параметры программы
        """
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            description='Collect data to database with Turn14 API (https://*******/)'
        )
        parser.add_argument(
            '--db-config',
            help='database config file (default: %(default)s)',
            dest='db_config',
            action='store',
            default=constants.DATABASE_DB_CONFIG_FILE
        )
        parser.add_argument(
            '--all-items',
            help='get data from /v1/items?page={page}',
            dest='all_items',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '--all-item-data',
            help='get data from /v1/items/data?page={page}',
            dest='all_item_data',
            action='store_true',
            default=False
        )
        parser.add_argument(
            '-t', '--test',
            help='use sandbox (https://*******/)',
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
        logging.info('{} Создание директории {}.'.format(constants.LOGGING_START, constants.API_TURN14_BACKUP_DIR))
        os.makedirs(constants.API_TURN14_BACKUP_DIR, exist_ok=True)
        logging.info('{} Создание директории {}.'.format(constants.LOGGING_FINISH, constants.API_TURN14_BACKUP_DIR))


class Turn14API:

    def __init__(
            self,
            *,
            db: database.Database,
            client_id: str,
            client_secret: str,
            apitest: bool = False,
            backup: bool = False,
            limit: int = None

    ) -> None:
        """
        Инициализирует экземпляр входными параметрами и на их основе получает authorization_header
        :param db: экземпляр для работы з базой данных
        :param client_id: Client ID со страницы https://*******/api_settings.php (раздел API Credentials)
        :param client_secret: Client Secret со страницы https://*******/api_settings.php (раздел API Credentials)
        :param apitest: флаг тестового режима
                    True - обращение идёт к https://*******test
                    False - обращение идёт к https://*******
        :param backup: сохранять ли данные в файлы отдельно от базы
        :param limit: ограничивает количество исходящих запросов (для тестирования)
        """
        self.db = db
        self.client_id = client_id
        self.client_secret = client_secret
        self.apitest = apitest
        self.backup = backup
        self.limit = limit
        self._make_authorization_header()

        self.turn14_items_ids = self.db.get_supplier_number_2_supplier_item_id(Turn14.id_in_db)

    def _make_authorization_header(self) -> None:
        """Создаёт self.authorization_header"""
        try:
            logging.info('{} Getting authorization_header.'.format(constants.LOGGING_START))
            r = self.get_access_token()
            if r.status_code != 200:
                raise requests.RequestException(r.json(), response=r)
            d = r.json()
            logging.debug(d)  # todo: убрать
            self.authorization_header = {'Authorization': '{} {}'.format(d['token_type'], d['access_token'])}
        finally:
            logging.info('{} Getting authorization_header.'.format(constants.LOGGING_FINISH))

    def _url(self, path):
        return ('https://*******test' if self.apitest else 'https:/*******') + path

    def getting_writing_loop(
            self,
            *,
            item_type,
            get_data_api_function,
            write_to_db_function,
            f_out
    ) -> None:
        path_to_page = None
        retrying_to_authorize_flag = False
        request_no = 0
        timeout_error_counter = 0
        not_status_200_counter = 0
        while True:
            request_no += 1
            if self.limit:  # Ограничение для тестирования
                if request_no > self.limit:
                    break
            try:
                r = get_data_api_function(path_to_page)
            except requests.exceptions.Timeout as e:
                timeout_error_counter += 1
                logging.warning(e)
                if timeout_error_counter > 5:
                    break
                continue
            else:
                timeout_error_counter = 0
                if r.status_code == 401:
                    if not retrying_to_authorize_flag:
                        retrying_to_authorize_flag = True
                        self._make_authorization_header()
                        continue
                    else:
                        raise requests.RequestException(r.json(), response=r)
                retrying_to_authorize_flag = False
                if r.status_code != 200:
                    not_status_200_counter += 1
                    logging.warning(r.text)
                    if not_status_200_counter > 5:
                        break
                    continue
                else:
                    not_status_200_counter = 0
                    data = r.json()
                    for data_row in data.get('data', []):
                        if f_out:
                            print(data_row, file=f_out)
                        item = item_type(data_row)

                        write_to_db_function(item)
                    try:
                        path_to_page = data['links']['next']
                    except KeyError:
                        break  # Дальше идти некуда, выходим из цикла

    def get_access_token(self) -> requests.Response:
        # noinspection SpellCheckingInspection
        """
        Возвращает response с access_token
        Подробнее см. https:/*******/api/#oauth-2
        Пример response.body для Valid Credentials:
            {
              "grant_type": "client_credentials",
              "client_id": "testclient",
              "client_secret": "testpass"
            }
        """
        return requests.post(
            url=self._url('/v1/token'),
            json={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            },
            timeout=15
        )

    def get_items_page(self, path_to_page: str = None) -> requests.Response:
        # noinspection SpellCheckingInspection
        """
        Возвращает response со страницей заданной в path_to_page item'ов
        Подробнее см. https://*******/api/#items-all-items-get
        Пример response.body для Valid Token:
            {
              "meta": {
                "total_pages": 10
              },
              "data": [
                {
                  "id": "10030",
                  "type": "Item",
                  "attributes": {
                    "product_name": "DBA 4000 Slot&Drill Rotors",
                    "part_number": "DBA4583XS",
                    "mfr_part_number": "4583XS",
                    "part_description": "DBA 92-95 MR-2 Turbo Rear Drilled & Slotted 4000 Series Rotor",
                    "category": "Brake",
                    "subcategory": "Drums and Rotors",
                    "dimensions": [
                      {
                        "box_number": 1,
                        "length": 15,
                        "width": 15,
                        "height": 4,
                        "weight": 13
                      }
                    ],
                    "brand_id": 18,
                    "brand": "DBA",
                    "price_group_id": 106,
                    "price_group": "DBA",
                    "active": true,
                    "regular_stock": false,
                    "dropship_controller_id": 12,
                    "air_freight_prohibited": false,
                    "not_carb_approved": false,
                    "warehouse_availability": [
                      {
                        "location_id": 1,
                        "can_place_order": true
                      }
                    ],
                    "thumbnail": "https://d5otzd52uv6zz.cloudfront.net/be0798de",
                    "barcode": "12345678765432",
                    "alternate_part_number": "1234567",
                    "prop_65": "Y"
                  }
                }
              ],
              "links": {
                "self": "/v1/items?page=1",
                "first": "/v1/items?page=1",
                "prev": "/v1/items?page=3",
                "next": "/v1/items?page=5",
                "last": "/v1/items?page=10"
              }
            }
        """
        return requests.get(
            url=self._url(path_to_page) if path_to_page else self._url('/v1/items?page=1'),
            headers=self.authorization_header,
            timeout=60
        )

    def get_items_data_page(self, path_to_page: str = None) -> requests.Response:
        """
        Возвращает response со страницей заданной в path_to_page item data
        Подробнее см. https://*******/api/#items-all-items-get
        Пример response.body для Valid Token:
        {
          "meta": {
            "total_pages": 10
          },
          "data": [
            {
              "id": "123456",
              "type": "ProductData",
              "files": [
                {
                  "id": 123456780,
                  "type": "Image",
                  "file_extension": "JPG",
                  "media_content": "Photo - Primary",
                  "links": [
                    {
                      "url": "https://d32vzsop7y1h3k.cloudfront.net/40df3ff8361b556415ce2df87ac9c9ee.JPG",
                      "height": 66,
                      "width": 100,
                      "asset_size": "S"
                    },
                    {
                      "url": "https://d32vzsop7y1h3k.cloudfront.net/40df3ff8361b556415ce2df87ac9c9ee.JPG",
                      "height": 132,
                      "width": 200,
                      "asset_size": "M"
                    }
                  ]
                },
                {
                  "id": 123456781,
                  "type": "Other",
                  "file_extension": "PDF",
                  "media_content": "Instruction Manual",
                  "links": [
                    {
                      "url": "https://d32vzsop7y1h3k.cloudfront.net/40df3ff8361b556415ce2df87ac9c9ee.PDF"
                    }
                  ]
                }
              ],
              "vehicle_fitments": [
                {
                  "vehicle_id": 5
                }
              ]
            }
          ],
          "links": {
            "self": "/v1/items/data?page=4",
            "first": "/v1/items/data?page=1",
            "prev": "/v1/items/data?page=3",
            "next": "/v1/items/data?page=5",
            "last": "/v1/items/data?&page=10"
          }
        }
        """
        return requests.get(
            url=self._url(path_to_page) if path_to_page else self._url('/v1/items/data?page=1'),
            headers=self.authorization_header,
            timeout=60
        )

    def write_all_items_to_db(self) -> None:
        try:
            logging.info('{} Сбор с помощью {}'.format(constants.LOGGING_START, '/v1/items?page={page}'))
            if self.backup:
                name = constants.API_TURN14_ALL_ITEMS_BACKUP_FILE_T.format(
                    datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                )
                logging.info('Writing {}...'.format(name))
                f_out = open(file=name, mode='w', encoding='utf8')
                del name
            else:
                f_out = None
            self.getting_writing_loop(
                item_type=Turn14APIAllItemsItem,
                get_data_api_function=self.get_items_page,
                write_to_db_function=self._write_to_db_all_items,
                f_out=f_out
            )
        finally:
            logging.info('{} Сбор с помощью {}'.format(constants.LOGGING_FINISH, '/v1/items?page={page}'))

    def write_all_item_data_to_db(self) -> None:
        try:
            logging.info('{} Сбор с помощью {}'.format(constants.LOGGING_START, '/v1/items/data?page={page}'))
            if self.backup:
                name = constants.API_TURN14_ALL_ITEM_DATA_BACKUP_FILE_T.format(
                    datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
                logging.info('Writing {}...'.format(name))
                f_out = open(file=name, mode='w', encoding='utf8')
                del name
            else:
                f_out = None
            self.getting_writing_loop(
                item_type=Turn14APIAllItemDataItem,
                get_data_api_function=self.get_items_data_page,
                write_to_db_function=self._write_to_db_all_item_data,
                f_out=f_out
            )
        finally:
            logging.info('{} Сбор с помощью {}'.format(constants.LOGGING_FINISH, '/v1/items/data?page={page}'))

    def _write_to_db_all_items(self, item):
        supplier_item_id = self.turn14_items_ids.get(item.number)
        if supplier_item_id:
            self.db.write_turn14_item_info_from_get_items_api(
                supplier_item_id,
                item.item_id_in_api,
                item.product_name,
                item.category,
                item.subcategory,
                item.dimensions,
                item.thumbnail,
                item.barcode
            )

    def _write_to_db_all_item_data(self, item):
        for file in item.files:
            self.db.write_turn14_item_info_from_get_items_data_api(
                item.item_id_in_api,
                file['url'],
                file['media_content'],
                file['height'],
                file['width']
            )
        if item.vehicle_fitments_ids:
            self.db.write_turn14_item_info_from_get_items_data_fitment_api(
                item.item_id_in_api,
                item.vehicle_fitments_ids
            )


if __name__ == '__main__':
    Program.run()
