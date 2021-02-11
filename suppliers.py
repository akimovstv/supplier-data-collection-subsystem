"""
Классы для обработки файлов поставщиков.
Под обработкой подразумевается загрузка по ftp (или http) и нормализация.
Под нормализацией подразумевается приведение файлов к формату csv и запись в унифицированном диалекте.
"""

import abc
import csv
import logging
import os
import re
import shutil
import zipfile
from typing import List

import requests
import pycurl

import constants
from items import BaseItem, KeystoneItem, MeyerItem, PremierItem, TransItem, Turn14Item


class BasicSupplier(abc.ABC):
    """
    Базовый абстрактный класс поставщика.
    Определяет базовые методы загрузки и нормализации файлов поставщика.
    """
    item = BaseItem
    INPUT_FILE_NECESSARY_FIELDS_LIST = None  # необходимые поля во входном файле
    SUPPLIER_NAME = None  # название поставщика
    DOWNLOAD_FILE = None  # имя загруженного файла
    INPUT_FILE = None  # имя входного файла
    DOWNLOAD_URL = None  # url для загрузки '*******'
    DOWNLOAD_USERPASSWORD = None  # e.g., '*******:*******'
    DOWNLOAD_URL_ALTERNATIVE = None  # ссылка на файл поставщика на альтернативном сервере

    def __repr__(self):
        return '{} object with __dict__: {}'.format(self.__class__, self.__dict__)

    @staticmethod
    def _put_into_temp_dir(file: str) -> str:
        return os.path.join(constants.PARSE_SUPPLIERS_FILES_TEMP_DIR, file)

    @staticmethod
    def _download_curl(file: str, url: str, userpassword: str = None) -> None:
        """
        Загружает с помощью pycurl содержимое по ссылке url с опциональным паролем userpassword в файл file
        """
        try:
            logging.info('Загрузка {}'.format(url))
            try:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_START, file))
                with open(
                        file=file,
                        mode='wb'  # открываем выходной файл для бинарной записи
                ) as f_out:
                    curl = pycurl.Curl()  # создаём экземпляр Curl
                    curl.setopt(curl.URL, url)  # установка опции загрузки URL=download_url
                    if userpassword:
                        curl.setopt(
                            curl.USERPWD,
                            userpassword
                        )  # установка опции загрузки USERPWD=download_userpwd
                    curl.setopt(curl.SSL_VERIFYPEER, 0)  # данная опция необходима для загрузки через FTP
                    curl.setopt(curl.SSL_VERIFYHOST, 0)  # данная опция необходима для загрузки через FTP
                    curl.setopt(curl.WRITEDATA, f_out)  # данная опция необходима для записи в выходной файл
                    try:
                        curl.perform()  # запускаем загрузку
                    except pycurl.error:
                        logging.error('Ошибка {} при загрузке {}'.format(repr(curl.errstr()), url))
                        raise
                    finally:
                        curl.close()  # закрываем Curl
            finally:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_FINISH, file))
        finally:
            logging.info('Файл загружен и сохранён как {}.'.format(file))

    @classmethod
    def download(cls) -> None:
        """
        Загрузка с помощью pycurl
        """
        cls._download_curl(
            file=cls.DOWNLOAD_FILE,
            url=cls.DOWNLOAD_URL,
            userpassword=cls.DOWNLOAD_USERPASSWORD
        )

    @staticmethod
    def _download_requests(file: str, url: str) -> None:
        """
        Загружает с помощью requests содержимое по ссылке url в файл file
        """
        try:
            logging.info('Загрузка {}'.format(url))
            try:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_START, file))
                with open(
                        file=file,
                        mode='wb'
                ) as f_out:
                    try:
                        response = requests.get(url, timeout=20)
                    except Exception as err:
                        logging.error('Возникла ошибка {} при загрузке: {}'.format(err, url))
                        raise
                    else:
                        f_out.write(response.content)
            finally:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_FINISH, file))
        finally:
            logging.info('Файл загружен и сохранён как {}.'.format(file))

    @classmethod
    def download_alternative(cls) -> None:
        """
        Загрузка с помощью requests
        """
        cls._download_requests(
            file=cls.DOWNLOAD_FILE,
            url=cls.DOWNLOAD_URL_ALTERNATIVE
        )

    @classmethod
    def normalize_download_file(cls) -> None:
        """
        Преобразует загруженный файл во входной файл
        """
        cls._rewrite_in_standard_dialect(
            file_in=cls.DOWNLOAD_FILE,
            file_out=cls.INPUT_FILE,
            fieldnames=cls.INPUT_FILE_NECESSARY_FIELDS_LIST
        )

    @staticmethod
    def _rewrite_in_standard_dialect(
            file_in: str,
            file_out: str,
            fieldnames: List[str],
            dialect: csv.Dialect = None
    ) -> None:
        """
        Перезаписывает csv-файл file_in в csv-файл file_out
        используя стандартный диалект constants.PROJECT_STANDARD_DIALECT
        :param file_in: входной файл (путь и имя)
        :param file_out: выходной файл (путь и имя)
        :param fieldnames: предполагаемые заголовки в файле file_in
        :param dialect: диалект csv-файла задаваемый пользователем (если заранее известен)
        """
        try:
            logging.debug('{} Чтение {}.'.format(constants.LOGGING_START, file_in))
            with open(
                    file=file_in,
                    mode='r',
                    newline='',
                    encoding='utf8'
            ) as f_in:
                if not dialect:  # если заранее неизвестен диалект - разнюхиваем
                    dialect = csv.Sniffer().sniff(f_in.read(20000))  # Определяем диалект в файле
                    f_in.seek(0)  # Восстанавливаем курсор в файле в начальную позицию
                reader = csv.DictReader(
                    f_in,
                    dialect=dialect
                )
                assert set(fieldnames).issubset(set(reader.fieldnames)), (fieldnames, reader.fieldnames)
                try:
                    logging.debug('{} Запись {}.'.format(constants.LOGGING_START, file_out))
                    with open(
                            file=file_out,
                            mode='w',
                            newline='',
                            encoding='utf8'
                    ) as f_out:
                        writer = csv.DictWriter(
                            f_out,
                            fieldnames=fieldnames,
                            extrasaction='ignore',
                            dialect=constants.PROJECT_STANDARD_DIALECT
                        )
                        writer.writeheader()
                        for row in reader:
                            writer.writerow(row)
                finally:
                    logging.debug('{} Запись {}.'.format(constants.LOGGING_FINISH, file_out))
        finally:
            logging.debug('{} Чтение {}.'.format(constants.LOGGING_FINISH, file_in))


class Keystone(BasicSupplier):
    item = KeystoneItem
    id_in_db = 1
    INPUT_FILE_NECESSARY_FIELDS_LIST = [
        'VenCode', 'PartNumber', 'LongDescription', 'JobberPrice', 'Cost', 'Fedexable', 'ExeterQty', 'MidWestQty',
        'SouthEastQty', 'TexasQty', 'PacificNWQty', 'GreatLakesQty', 'CaliforniaQty', 'TotalQty', 'VendorName',
        'UPCCode', 'Prop65Toxicity', 'HazardousMaterial'
    ]
    SUPPLIER_NAME = 'Keystone'
    DOWNLOAD_FILE = BasicSupplier._put_into_temp_dir('downloaded__{}.csv'.format(SUPPLIER_NAME))
    INPUT_FILE = BasicSupplier._put_into_temp_dir('input__{}.csv'.format(SUPPLIER_NAME))
    DOWNLOAD_URL = '*******'
    DOWNLOAD_USERPASSWORD = '*******'
    DOWNLOAD_URL_ALTERNATIVE = '*******'


class Meyer(BasicSupplier):
    """
    Особенности:
    1. Для Meyer необходимо загружать 2 файла (inventory и pricing)
    """
    item = MeyerItem
    id_in_db = 2

    INPUT_FILE_NECESSARY_FIELDS_LIST = [
        'Manufacturer Name', 'Manufacturer Part Number', 'Meyer SKU', 'Description', 'Jobber Price', 'Customer Price',
        'UPC', 'MAP', 'Length', 'Width', 'Height', 'Weight', 'Category', 'Sub-Category', 'LTL Eligible', 'Discontinued'
    ]

    INPUT_FILE_INVENTORY_NECESSARY_FIELDS_LIST = [
        'Meyer SKU', 'Item Description', '008 Qty', '032 Qty', '041 Qty', '044 Qty', '053 Qty', '062 Qty', '063 Qty',
        '065 Qty', '068 Qty', '069 Qty', '070 Qty', '071 Qty', '072 Qty', '077 Qty', '093 Qty', '094 Qty', '098 Qty',
        'Discontinued'
    ]

    SUPPLIER_NAME = 'Meyer'
    DOWNLOAD_FILE = BasicSupplier._put_into_temp_dir('downloaded__{}_Pricing.csv'.format(SUPPLIER_NAME))
    DOWNLOAD_FILE_INVENTORY = BasicSupplier._put_into_temp_dir('downloaded__{}_Inventory.csv'.format(SUPPLIER_NAME))
    INPUT_FILE = BasicSupplier._put_into_temp_dir('input__{}_Pricing.csv'.format(SUPPLIER_NAME))
    INPUT_FILE_INVENTORY = BasicSupplier._put_into_temp_dir('input__{}_Inventory.csv'.format(SUPPLIER_NAME))

    DOWNLOAD_URL = '*******'
    DOWNLOAD_URL_INVENTORY = '*******'
    DOWNLOAD_USERPASSWORD = '*******'

    DOWNLOAD_URL_ALTERNATIVE = '*******'
    DOWNLOAD_URL_ALTERNATIVE_INVENTORY = '*******'

    @classmethod
    def normalize_download_file(cls) -> None:
        """
        Преобразует загруженный файл во входной файл
        """
        cls._rewrite_with_triple_quotes(cls.DOWNLOAD_FILE)

        class MeyerDialect(csv.Dialect):
            delimiter = ','
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = '\r\n'
            quoting = csv.QUOTE_MINIMAL

        cls._rewrite_in_standard_dialect(
            file_in=cls.DOWNLOAD_FILE,
            file_out=cls.INPUT_FILE,
            fieldnames=cls.INPUT_FILE_NECESSARY_FIELDS_LIST,
            dialect=MeyerDialect()
        )

        cls._rewrite_in_standard_dialect(
            file_in=cls.DOWNLOAD_FILE_INVENTORY,
            file_out=cls.INPUT_FILE_INVENTORY,
            fieldnames=cls.INPUT_FILE_INVENTORY_NECESSARY_FIELDS_LIST,
            dialect=MeyerDialect()
        )

    @staticmethod
    def _rewrite_with_triple_quotes(file):
        temp = file + '.temp'
        shutil.move(file, temp)
        p = re.compile(r'([^"])"",')
        with open(
                file=temp,
                mode='r',
                encoding='utf8'
        ) as f_in:
            with open(
                    file=file,
                    mode='w',
                    encoding='utf8'
            ) as f_out:
                for row in f_in:
                    f_out.write(p.sub(r'\g<1>""",', row))
        os.remove(temp)

    @classmethod
    def download(cls) -> None:
        super(Meyer, cls).download()

        cls._download_curl(
            file=cls.DOWNLOAD_FILE_INVENTORY,
            url=cls.DOWNLOAD_URL_INVENTORY,
            userpassword=cls.DOWNLOAD_USERPASSWORD
        )

    @classmethod
    def download_alternative(cls) -> None:
        super(Meyer, cls).download_alternative()

        cls._download_requests(
            file=cls.DOWNLOAD_FILE_INVENTORY,
            url=cls.DOWNLOAD_URL_ALTERNATIVE_INVENTORY
        )


class Premier(BasicSupplier):
    """
    Особенности:
    1. Загружается архив, из которого извлекаем файл
    """
    item = PremierItem
    id_in_db = 3
    INPUT_FILE_NECESSARY_FIELDS_LIST = [
        'Brand', 'Line Code', 'Part Number', 'SKU', 'Distributor Cost', 'Package Quantity', 'Core Price', 'UPC',
        'Part Description', 'Inventory Count', 'Inventory Type'
    ]

    SUPPLIER_NAME = 'Premier'
    DOWNLOAD_FILE = BasicSupplier._put_into_temp_dir('downloaded__{}.zip'.format(SUPPLIER_NAME))
    TEMP_INPUT_FILE_NAME = BasicSupplier._put_into_temp_dir('temp_input__{}.csv'.format(SUPPLIER_NAME))
    INPUT_FILE = BasicSupplier._put_into_temp_dir('input__{}.csv'.format(SUPPLIER_NAME))

    DOWNLOAD_URL = '*******'
    DOWNLOAD_USERPASSWORD = '*******'

    DOWNLOAD_URL_ALTERNATIVE = '*******'

    @classmethod
    def normalize_download_file(cls) -> None:
        """
        Преобразует загруженный файл во входной файл
        """
        cls._extract_downloaded_zip()
        cls._remove_null_bytes()

        class PremierDialect(csv.Dialect):
            delimiter = '|'
            quotechar = None
            escapechar = None
            doublequote = False
            skipinitialspace = False
            lineterminator = '\r\n'
            quoting = csv.QUOTE_NONE

        cls._rewrite_in_standard_dialect(
            file_in=cls.TEMP_INPUT_FILE_NAME,
            file_out=cls.INPUT_FILE,
            fieldnames=cls.INPUT_FILE_NECESSARY_FIELDS_LIST,
            dialect=PremierDialect()
        )

        os.remove(cls.TEMP_INPUT_FILE_NAME)

    @classmethod
    def _extract_downloaded_zip(cls) -> None:
        """
        Извлекает необходимый файл из архива в директории загруженных файлов.
        Сохраняет извлеченный файл в директории входных файлов.
        """
        member = 'AmazonExport.csv'
        try:
            logging.debug(
                '{} Извлечение из архива {} файла {} и сохранение его как {}.'.format(
                    constants.LOGGING_START,
                    cls.DOWNLOAD_FILE,
                    member,
                    cls.TEMP_INPUT_FILE_NAME
                )
            )
            zip_file = zipfile.ZipFile(cls.DOWNLOAD_FILE)
            zip_file.extract(member=member, path=constants.PARSE_SUPPLIERS_FILES_TEMP_DIR)
            shutil.move(os.path.join(constants.PARSE_SUPPLIERS_FILES_TEMP_DIR, member), cls.TEMP_INPUT_FILE_NAME)
        finally:
            logging.debug(
                '{} Извлечение из архива {} файла {} и сохранение его как {}.'.format(
                    constants.LOGGING_FINISH,
                    cls.DOWNLOAD_FILE,
                    member,
                    cls.TEMP_INPUT_FILE_NAME
                )
            )

    @classmethod
    def _remove_null_bytes(cls) -> None:
        """
        Удаляет NULL-байты в извлеченном из архива файле AmazonExport.csv (Необходимо для работы модуля csv)
        Перезаписывает файл с уже удаленными NULL-байтами в директории входных файлов
        """
        try:
            logging.debug(
                '{} Перезапись файла {} в кодировке UTF-8 без NULL-bytes.'.format(
                    constants.LOGGING_START,
                    cls.TEMP_INPUT_FILE_NAME
                )
            )
            try:
                logging.debug('{} Чтение {}.'.format(constants.LOGGING_START, cls.TEMP_INPUT_FILE_NAME))
                with open(
                        file=cls.TEMP_INPUT_FILE_NAME,
                        mode='r',
                        encoding="cp1251"
                ) as f_in:
                    data = f_in.read()
                    if '\0' in data:
                        data = data.replace('\0', '')
            finally:
                logging.debug('{} Чтение {}.'.format(constants.LOGGING_FINISH, cls.TEMP_INPUT_FILE_NAME))
            try:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_START, cls.TEMP_INPUT_FILE_NAME))
                with open(
                        file=cls.TEMP_INPUT_FILE_NAME,
                        mode='w',
                        encoding='utf8'
                ) as f_out:
                    f_out.write(data)
            finally:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_FINISH, cls.TEMP_INPUT_FILE_NAME))
        finally:
            logging.debug(
                '{} Перезапись файла {} в кодировке UTF-8 без NULL-bytes.'.format(
                    constants.LOGGING_FINISH,
                    cls.TEMP_INPUT_FILE_NAME
                )
            )


class Trans(BasicSupplier):
    """
    Особенности:
    1. Бренды в загружаемом файле отсутствуют.
    2. Бренды получаем из файла соответствия префиксов брендам.
    """
    item = TransItem
    id_in_db = 4
    INPUT_FILE_NECESSARY_FIELDS_LIST = [
        'LINE', 'CLASS', 'PART_NUMBER_FULL', 'CA', 'TX', 'FL', 'CO', 'OH', 'ID', 'PA', 'LIST_PRICE', 'JOBBER_PRICE',
        'TOTAL', 'STATUS'
    ]
    SUPPLIER_NAME = 'Trans'
    DOWNLOAD_FILE = BasicSupplier._put_into_temp_dir('downloaded__{}.txt'.format(SUPPLIER_NAME))
    INPUT_FILE = BasicSupplier._put_into_temp_dir('input__{}.csv'.format(SUPPLIER_NAME))
    DOWNLOAD_URL = '*******'
    DOWNLOAD_USERPASSWORD = '*******'
    DOWNLOAD_URL_ALTERNATIVE = '*******'


class Turn14(BasicSupplier):
    item = Turn14Item
    id_in_db = 5
    INPUT_FILE_NECESSARY_FIELDS_LIST = [
        'PrimaryVendor', 'PricingGroup', 'InternalPartNumber', 'PartNumber', 'Description', 'Cost', 'Retail', 'Jobber',
        'CoreCharge', 'Map', 'Other', 'OtherName', 'EastStock', 'WestStock', 'CentralStock', 'Stock', 'MfrStock',
        'MfrStockDate', 'DropShip', 'DSFee', 'Weight'
    ]
    SUPPLIER_NAME = 'Turn14'
    DOWNLOAD_FILE = BasicSupplier._put_into_temp_dir('downloaded__{}.csv'.format(SUPPLIER_NAME))
    INPUT_FILE = BasicSupplier._put_into_temp_dir('input__{}.csv'.format(SUPPLIER_NAME))
    DOWNLOAD_URL = DOWNLOAD_URL_ALTERNATIVE = '*******'
    DOWNLOAD_USERPASSWORD = '*******'

    @classmethod
    def download(cls) -> None:
        # файл данного поставщика скачиваем по HTTP с помощью requests в любом случае
        cls.download_alternative()

    @classmethod
    def normalize_download_file(cls) -> None:
        """
        Преобразует загруженный файл во входной файл
        """

        class Turn14Dialect(csv.Dialect):
            delimiter = ','
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = '\r\n'
            quoting = csv.QUOTE_MINIMAL

        cls._rewrite_in_standard_dialect(
            file_in=cls.DOWNLOAD_FILE,
            file_out=cls.INPUT_FILE,
            fieldnames=cls.INPUT_FILE_NECESSARY_FIELDS_LIST,
            dialect=Turn14Dialect()
        )
