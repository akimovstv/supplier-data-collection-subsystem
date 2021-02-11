"""
Работа с файлами поставщиков получаемых по ftp, http

Именно эту программу нужно запускать для сбора данных из файлов поставщиков.

Запуск (из виртуального окружения):
python parse_suppliers_files.py

Также доступны флаги, модифицирующие запуск, которые можно посмотреть так:
python parse_suppliers_files.py -h

Данный модуль состоит из одного класса Program.

В самом конце модуля мы видим запуск Program.run(). Отсюда и нужно разбирать алгоритм.
"""

import argparse
import csv
import datetime
import logging
import os
import pycurl
import shutil
import zipfile
from collections import namedtuple
from typing import Tuple, Dict, FrozenSet

import constants
import database
from suppliers import Keystone, Meyer, Premier, Trans, Turn14


# noinspection SpellCheckingInspection
class Program:
    """
    Класс, который управляет потоком программы
    """

    suppliers = Keystone, Meyer, Premier, Trans, Turn14  # это поставщики, с которыми программа работает
    args = None  # type:argparse.Namespace # это входные параметры программы
    db = None  # type:database.Database # это связь с базой данных

    @classmethod
    def run(cls) -> None:
        """
        Запускает алгоритм программы
        """

        # ШАГ 1. Разбор входных параметров программы
        cls.parse_program_arguments()

        # ВСПОМОГАТЕЛЬНЫЙ ШАГ. Вывод вспомагательных сообщений о начале работы
        print('Started.')
        print('Working...')

        # ШАГ 2. Настройка логирования
        cls.setup_logging()

        # ВСПОМОГАТЕЛЬНЫЙ ШАГ. Начало логирования
        logging.info('{} РАБОТА АЛГОРИТМА'.format(constants.LOGGING_START))

        # ШАГ 3. Создание директорий для загружаемых файлов и файлов резервной копии
        cls.make_dirs()

        if 1:
            # ШАГ 4. Загрузка файлов поставщиков
            cls.download_suppliers_files(cls.args.download_http)
            # здесь уже имеюются загруженные файлы поставщиков

        if 1:
            # ШАГ 5. Нормализация файлов поставщиков
            cls.normalize_suppliers_files()
            # здесь уже имеются входные файлы поставщиков

        # ШАГ 6. Подключение к базе данных
        # при этом если нужно создаётся новая база данных
        cls.db = database.Database(create=True, option_files=cls.args.db_config)

        if 1:
            # ШАГ 7. Запись в базу данных названий брендов из файлов поставщиков
            cls.insert_into_supplier_brand()

        if 1:
            # ШАГ 8. Запись в базу данных брендов из файлов ручной модерации брендов
            cls.make_brands_from_checked_brands_of_suppliers()

        if 1:
            # ШАГ 9. Запись в базу данных brand, mpn, prefix item-ов
            cls.insert_into_supplier_item()

        if 1:
            # ШАГ 10. Запись в базу данных остальной информации об item-ах
            cls.insert_into_specific_supplier_item()

        if 1:
            # ШАГ 11. Обновление category и subcategory в meyer_item
            cls.update_meyer_item__category_subcategory()

        if 1:
            # ШАГ 12. Запись в базу данных остальной информации об item-ах поставщика Meyer
            cls.update_meyer_item__inventory()

        # ШАГ 13. Создание архива с резервной копией файлов поставщиков
        if cls.args.backup:
            cls.make_backup()

        # ШАГ 14. Очистка временных каталогов
        if cls.args.clear:
            cls.clear_temp_dirs()

        # ВСПОМОГАТЕЛЬНЫЙ ШАГ. Конец логирования
        logging.info('{} РАБОТА АЛГОРИТМА'.format(constants.LOGGING_FINISH))

        # ВСПОМОГАТЕЛЬНЫЙ ШАГ. Вывод вспомагательного сообщения о завершении работы
        print('Finished.')

    @classmethod
    def parse_program_arguments(cls) -> None:
        """
        Парсит входные параметры программы.
        Выделяет следующие поля (если не заданы получаются аргументы по-умолчанию (указаны в скобках)):
        cls.args.db_config - файл конфигурации подключения к базе данных (constants.DATABASE_DB_CONFIG_FILE)
        cls.args.download_http - флаг загрузки с https://******* (False)
        cls.args.clear - флаг удаления загруженных файлов после отработки алгоритма (False)
        cls.args.backup - флаг сохранения резервной копии загруженных файлов (False)
        cls.args.logfile - файл с результатами логирования (sys.stderr)
        cls.args.loglevel - уровень логирования (INFO)
        """
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            description="Create and/or update suppliers database.\nSuppliers to be processed: {}".format(
                ', '.join(sup.SUPPLIER_NAME for sup in cls.suppliers)
            )
        )

        parser.add_argument(
            '--db-config',
            dest='db_config',
            action='store',
            default=constants.DATABASE_DB_CONFIG_FILE,
            help='database config file (default: %(default)s)'
        )

        parser.add_argument(
            '--download-http',
            dest='download_http',
            action='store_true',
            default=False,
            help="download suppliers' files from https://*******/",
        )

        parser.add_argument(
            '-c', '--clear',
            dest='clear',
            action='store_true',
            default=False,
            help='delete temporary files and directories',
        )
        parser.add_argument(
            '--backup',
            dest='backup',
            action='store_true',
            default=False,
            help='backup input files as zip archive to {}'.format(constants.PARSE_SUPPLIERS_FILES_BACKUP_DIR),

        )
        parser.add_argument(
            '--logfile',
            dest='logfile',
            action='store',
            default='',
            help='log file (default: stderr)'
        )
        parser.add_argument(
            '--loglevel',
            dest='loglevel',
            action='store',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            help='logging level (default: %(default)s)'
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
        Создаёт директорию для загружаемых файлов поставщиков и для их резервной копии
        """
        logging.info('{} Создание необходимых директорий.'.format(constants.LOGGING_START))
        os.makedirs(constants.PARSE_SUPPLIERS_FILES_BACKUP_DIR, exist_ok=True)
        os.makedirs(constants.PARSE_SUPPLIERS_FILES_TEMP_DIR, exist_ok=True)
        logging.info('{} Создание необходимых директорий.'.format(constants.LOGGING_FINISH))

    @classmethod
    def make_backup(cls) -> None:
        """
        Создаёт файлы резервной копии
        """
        file = os.path.join(constants.PARSE_SUPPLIERS_FILES_BACKUP_DIR,
                            '{}.zip'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')))

        try:
            logging.info(
                '{} Создание резервной копии входных файлов поставщиков {}.'.format(constants.LOGGING_START, file)
            )
            with zipfile.ZipFile(
                    file=file,
                    mode='w',
                    compression=zipfile.ZIP_DEFLATED

            ) as zf:
                for supplier in cls.suppliers:
                    zf.write(
                        filename=supplier.INPUT_FILE
                    )
                zf.write(
                    filename=Meyer.INPUT_FILE_INVENTORY
                )
        finally:
            logging.info(
                '{} Создание резервной копии входных файлов поставщиков {}.'.format(constants.LOGGING_FINISH, file)
            )

    @classmethod
    def clear_temp_dirs(cls) -> None:
        if cls.args.clear:
            logging.info(
                '{} Удаление директории {} для временных файлов поставщиков.'.format(
                    constants.LOGGING_START,
                    constants.PARSE_SUPPLIERS_FILES_TEMP_DIR
                )
            )

            shutil.rmtree(constants.PARSE_SUPPLIERS_FILES_TEMP_DIR, ignore_errors=True)

            logging.info(
                '{} Удаление директории {} для временных файлов поставщиков.'.format(
                    constants.LOGGING_FINISH,
                    constants.PARSE_SUPPLIERS_FILES_TEMP_DIR
                )
            )

    @classmethod
    def download_suppliers_files(cls, alternative: bool = False) -> None:
        """
        Вызывает методы классов поставщиков по загрузке файлов поставщиков
        :param alternative: если False, то загрузка в основном с помощью pycurl (ftp),
                            иначе - загрузка с помощью requests с ******* по HTTP
        """
        try:
            logging.info('{} Загрузка файлов поставщиков.'.format(constants.LOGGING_START))
            for supplier in cls.suppliers:
                if not alternative:
                    try:
                        supplier.download()
                    except pycurl.error:
                        supplier.download_alternative()  # при ошибке загружаем с *******
                else:
                    supplier.download_alternative()
        finally:
            logging.info('{} Загрузка файлов поставщиков.'.format(constants.LOGGING_FINISH))

    @classmethod
    def normalize_suppliers_files(cls) -> None:
        """
        Вызывает методы классов поставщиков по нормализации загруженных файлов поставщиков, т.е. преобразовывает
        ЗАГРУЖЕННЫЕ файлы поставщиков в нормализованные ВХОДНЫЕ файлы
        Цель -  нормализовать загруженные файлы поставщиков, т. е. сохранить в одинаковом диалекте как csv
                при этом если нужно разархивировать, удалить NULL-bytes и подобное
        """
        try:
            logging.info(
                '{} Нормализация загруженных файлов поставщиков.'.format(
                    constants.LOGGING_START
                )
            )

            for supplier in cls.suppliers:
                try:
                    logging.info(
                        '{} Нормализация файлов поставщика {}.'.format(
                            constants.LOGGING_START,
                            supplier.SUPPLIER_NAME
                        )
                    )
                    supplier.normalize_download_file()
                finally:
                    logging.info(
                        '{} Нормализация файлов поставщика {}.'.format(
                            constants.LOGGING_FINISH,
                            supplier.SUPPLIER_NAME
                        )
                    )
        finally:
            logging.info(
                '{} Нормализация загруженных файлов поставщиков.'.format(
                    constants.LOGGING_FINISH
                )
            )

    @classmethod
    def insert_into_supplier_brand(cls) -> None:
        try:
            logging.info(
                '{} Запись брендов из файлов поставщиков в таблицу brand базы данных.'.format(
                    constants.LOGGING_START
                )
            )
            brands = set()
            for supplier in cls.suppliers:
                try:
                    logging.debug(
                        '{} Чтение {}.'.format(constants.LOGGING_START, supplier.INPUT_FILE)
                    )
                    with open(
                            file=supplier.INPUT_FILE,
                            newline='',
                            encoding='utf8'
                    ) as f_in:
                        csv_reader = csv.DictReader(f_in, dialect=constants.PROJECT_STANDARD_DIALECT)
                        assert set(csv_reader.fieldnames) == set(supplier.INPUT_FILE_NECESSARY_FIELDS_LIST)
                        for row in csv_reader:
                            item = supplier.item(row)
                            if item.norm_brand and item.norm_mpn:
                                brands.add((supplier.id_in_db, item.brand))
                finally:
                    logging.debug(
                        '{} Чтение {}.'.format(constants.LOGGING_FINISH, supplier.INPUT_FILE)
                    )
            data = []
            for supplier_id, name in sorted(brands, key=lambda x: x[-1].casefold()):
                data.append({'supplier_id': supplier_id, 'name': name})
            cls.db.insert_into_supplier_brand(data)
        finally:
            logging.info(
                '{} Запись брендов из файлов поставщиков в таблицу brand базы данных.'.format(
                    constants.LOGGING_FINISH
                )
            )

    @classmethod
    def insert_into_supplier_item(cls) -> None:

        cls.db.update_supplier_item_with_available()

        try:
            logging.info('{} Запись номеров в таблицу item базы данных'.format(constants.LOGGING_START))

            supplier_brand_ids = cls.db.get_from_supplier_brand__name_and_supplier_id_2_supplier_brand_id()

            data = []  # список database.Item для записи в таблицу item базы данных
            for supplier in cls.suppliers:
                try:
                    logging.debug(
                        '{} Чтение {}.'.format(constants.LOGGING_START, supplier.INPUT_FILE)
                    )

                    with open(
                            file=supplier.INPUT_FILE,
                            newline='',
                            encoding='utf8'
                    ) as f_in:
                        csv_reader = csv.DictReader(f_in, dialect=constants.PROJECT_STANDARD_DIALECT)
                        assert set(supplier.INPUT_FILE_NECESSARY_FIELDS_LIST).issubset(set(csv_reader.fieldnames)), \
                            'INPUT FILE NECESSARY FIELDS LIST: {}\nINPUT FILE ACTUAL FIELDS LIST:    {}'.format(
                                sorted(supplier.INPUT_FILE_NECESSARY_FIELDS_LIST),
                                sorted(csv_reader.fieldnames)
                            )
                        for row in csv_reader:
                            item = supplier.item(row)
                            if item.norm_brand and item.norm_mpn:
                                key = (item.brand, supplier.id_in_db)
                                try:
                                    supplier_brand_id = supplier_brand_ids[key]
                                except KeyError:
                                    logging.error('Error key {} in supplier_brand_ids'.format(key))
                                    continue

                                data.append(
                                    dict(
                                        supplier_brand_id=supplier_brand_id,
                                        norm_mpn=item.norm_mpn,
                                        available=True,
                                        prefix=item.prefix,
                                        mpn=item.mpn,
                                        number=item.number
                                    )
                                )

                                if len(data) == 5000:
                                    # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                                    cls.db.insert_into_supplier_item(data)
                                    data.clear()

                finally:
                    logging.debug(
                        '{} Чтение {}.'.format(constants.LOGGING_FINISH, supplier.INPUT_FILE)
                    )

            if data:
                # дописываем оствавшиеся item'ы
                cls.db.insert_into_supplier_item(data)
                data.clear()
        finally:
            logging.info('{} Запись номеров в таблицу item базы данных'.format(constants.LOGGING_FINISH))

    @classmethod
    def _get_supplier_brands(cls) -> Tuple[
        Dict[int, constants.SupplierBrandKey], Dict[constants.SupplierBrandKey, int]
    ]:
        """
        Получает из базы данных данные из таблицы supplier_brand
        Возвращает кортеж из двух словарей вида:
        (
            {supplier_brand_id: SupplierBrandKey(supplier_brand_name, supplier_id)},
            {SupplierBrandKey(supplier_brand_name, supplier_id): supplier_brand_id}
        )
        """
        statement = """
            SELECT
              supplier_brand_id AS supplier_brand_id, 
              name              AS supplier_brand_name,
              supplier_id       AS supplier_id
            FROM supplier_brand
        """
        cursor = cls.db.execute_with_results(statement=statement)
        supplier_brand_id_2_supplier_brand_key = {}
        supplier_brand_key_2_supplier_brand_id = {}
        try:
            for row in cursor:
                supplier_brand_key = constants.SupplierBrandKey(
                    name=row.supplier_brand_name,
                    supplier_id=row.supplier_id
                )
                supplier_brand_id = row.supplier_brand_id
                supplier_brand_id_2_supplier_brand_key[supplier_brand_id] = supplier_brand_key
                supplier_brand_key_2_supplier_brand_id[supplier_brand_key] = supplier_brand_id
        finally:
            cursor.close()
        return supplier_brand_id_2_supplier_brand_key, supplier_brand_key_2_supplier_brand_id

    @classmethod
    def _get_synonym_pairs_checked(cls) -> Dict[FrozenSet[constants.SupplierBrandKey], str]:
        """
        Получает из файла constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE данные.
        Возвращает словарь: frozenset({supplier_brand_key_1, supplier_brand_key_2}): check
        """
        checked = {}
        try:
            logging.debug(
                '{} Чтение {}'.format(
                    constants.LOGGING_START, constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE
                )
            )
            with open(
                    file=constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE,
                    mode='r',
                    encoding='utf8',
                    newline=''
            ) as f_in:
                csv.field_size_limit(1000000)
                dialect = csv.Sniffer().sniff(f_in.read(20000))
                f_in.seek(0)
                reader = csv.DictReader(f_in, dialect=dialect)
                assert constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE_NECESSARY_FIELDS_SET.issubset(
                    set(reader.fieldnames)
                )
                for row in reader:
                    check = row['check']
                    if check:
                        supplier_brand_key_1 = constants.SupplierBrandKey(
                            name=row['brand_1__name'],
                            supplier_id=int(row['brand_1__supplier_id'])
                        )
                        supplier_brand_key_2 = constants.SupplierBrandKey(
                            name=row['brand_2__name'],
                            supplier_id=int(row['brand_2__supplier_id'])
                        )
                        key = frozenset((supplier_brand_key_1, supplier_brand_key_2))
                        checked[key] = check
        except FileNotFoundError as e:
            logging.error(e)
            raise
        finally:
            logging.debug(
                '{} Чтение {}'.format(
                    constants.LOGGING_FINISH, constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE
                )
            )

        return checked

    @classmethod
    def _get_brands(cls) -> Tuple[Dict[int, str], Dict[str, int]]:
        """
        Получает из базы данных данные из таблицы brand
        Возвращает кортеж из двух словарей:
        (
            {brand_id: brand_name},
            {brand_name: brand_id}
        )
        """
        statement = """
            SELECT
              brand_id,
              name AS brand_name
            FROM brand;
        """
        cursor = cls.db.execute_with_results(statement=statement)
        brand_id_2_brand_name = {}
        brand_name_2_brand_id = {}
        try:
            for row in cursor:
                brand_id = row.brand_id
                brand_name = row.brand_name
                brand_id_2_brand_name[brand_id] = brand_name
                brand_name_2_brand_id[brand_name] = brand_id
        finally:
            cursor.close()
        return brand_id_2_brand_name, brand_name_2_brand_id

    @classmethod
    def _get_previous_names(cls) -> Dict[FrozenSet[constants.SupplierBrandKey], constants.BrandCheck]:
        previous_data = {}
        try:
            logging.debug(
                '{} Чтение {}'.format(
                    constants.LOGGING_START,
                    constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE
                )
            )
            with open(
                    file=constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE,
                    mode='r',
                    encoding='utf8',
                    newline=''
            ) as f_in:
                csv.field_size_limit(1000000)
                dialect = csv.Sniffer().sniff(f_in.read(20000))
                f_in.seek(0)
                reader = csv.DictReader(f_in, dialect=dialect)
                assert constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE_NECESSARY_FIELDS_SET.issubset(
                    set(reader.fieldnames))
                for row in reader:
                    brand_key = constants.BrandCheck(name=row['name'], check=row['check'])
                    supplier_brand_keys = constants.split_chain(row['chain'])
                    previous_data[supplier_brand_keys] = brand_key

        except FileNotFoundError as e:
            logging.error(e)
            return {}  # В случае если файла нет возвращаем пустой словарть
        finally:
            logging.debug(
                '{} Чтение {}'.format(
                    constants.LOGGING_FINISH,
                    constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE
                )
            )

        return previous_data

    # noinspection SpellCheckingInspection
    @classmethod
    def make_brands_from_checked_brands_of_suppliers(cls) -> None:
        """

        Сопоставляет бренды по признаку идентичности (синонимичности) с записью данных сопоставлений в базу данных.

        Для работы данной функции необходимо:
        1. Файл constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE, формируемый программой
            special_make_file_with_brand_pairs.py
            Данный файл должен иметь следующие колонки (поля): constants.BRAND_SYNONYM_CHECKED_FILE_NECESSARY_FIELDS_SET
            Расшифровка колонок:
                brand_1__name - название первого бренда пары в том виде, как оно есть у поставщика (не нормализованное)
                brand_1__supplier_id - код поставщика первого бренда
                brand_2__name - название второго бренда пары в том виде, как оно есть у поставщика (не нормализованное)
                brand_2__supplier_id - код поставщика второго бренда
                (brand_1__name+brand_1__supplier_id и brand_2__name+brand_2__supplier_id - это ключи в таблице
                supplier_brand базы данных)
                check - метка идентичтоности (синонимичности) брендов, т.е. что первый бренд является по сути вторым
                        брендом, хотя названия брендов могут отличаться
                        Т.о. метки в колонке check:
                            "+" - первый бренд и второй бренд пары брендов точно синонимы
                            "-" - первый бренд и второй бренд пары брендов точно не синонимы
                            "?" - идентичность (синонимичность) не удалось проверить
                            ""  - пара брендов не проверялась на идентичность

        2. Файл constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE, формируемый прогрммой
            special_make_file_with_name_of_brand_chains.py
            Данный файл должен содердать следующие колонки (поля):
            constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE_NECESSARY_FIELDS_SET
            Расшифровка колонок:
                name - реальное название бренда для всей цепочки брендов (проверяется вручную)
                check - метка проверки вручную: если "+" - значит проверка была проведена, иначе - не была проведена
                chain - цепочка брендов-синонимов в формате:
                    "name_A (supplier_id_A) | name_B (supplier_id_B) | name_C (supplier_id_C)", где
                    name_A, name_B, name_C - supplier_brand.name
                    supplier_id_A, supplier_id_B, supplier_id_C - supplier_brand.supplier_id

        Алгоритм приблизительно такой:
        - Получаем из базы данных все бренды поставщиков
        - Получаем из файла constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE пометки об идентичности пар
          брендов
        - Т.к. мы имеем пометки только для ПАР брендов, то находим максимально длинные цепочки брендов-синонимов
          с помощью поиска в глубину
        - Получаем из файла constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE реальные названия бреднов для
          цепочек брендов
        - Сопоставляем полученные цепочки с цепочками из файла constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE
        - Записываем в базу реальные названия цепочек брендов и вновь полученные цепочки брендов (последнее с помощью
          сопоставления brand_id и supplier_brand_id в таблице brand_supplier_brand)

        """

        # Сначала получаем некоторые данные из базы данных и из файла
        # constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE

        # supplier_brand_id_2_supplier_brand_key имеет вид {supplier_brand_id: SupplierBrandKey}
        # supplier_brand_key_2_supplier_brand_id имеет вид {SupplierBrandKey: supplier_brand_id}
        # Пример:
        # supplier_brand_id_2_supplier_brand_key = {
        #   1: SupplierBrandKey(name='100+/COYOTE', supplier_id=1),
        #   2: SupplierBrandKey(name='303 PRODUCTS', supplier_id=1),
        #   3: SupplierBrandKey(name='303 Products, Inc.', supplier_id=2),
        #   ...
        # }
        # supplier_brand_key_2_supplier_brand_id = {
        #   SupplierBrandKey(name='100+/COYOTE', supplier_id=1): 1,
        #   SupplierBrandKey(name='303 PRODUCTS', supplier_id=1): 2,
        #   SupplierBrandKey(name='303 Products, Inc.', supplier_id=2): 3,
        #   ...
        # }
        supplier_brand_id_2_supplier_brand_key, supplier_brand_key_2_supplier_brand_id = cls._get_supplier_brands()

        # synonym_pairs_checked имеет вид {frozenset({SupplierBrandKey1, SupplierBrandKey2}): check}
        # Пример:
        # synonym_pairs_checked = {
        #   frozenset({SupplierBrandKey(name='303 Products, Inc.', supplier_id=2),
        #              SupplierBrandKey(name='303 PRODUCTS', supplier_id=1)}): '+',
        #   frozenset({SupplierBrandKey(name='OCTANE BOOST', supplier_id=1),
        #              SupplierBrandKey(name='303 PRODUCTS', supplier_id=1)}): '+',
        #   frozenset({SupplierBrandKey(name='303 Products, Inc.', supplier_id=2),
        #              SupplierBrandKey(name='Eastern Catalytic Converters', supplier_id=2)}): '-',
        #   ...
        # }
        synonym_pairs_checked = cls._get_synonym_pairs_checked()

        # каждому supplier_brand_id имеющемуся в базе будет сопоставлена следующая структура
        MyStructure = namedtuple(
            'MyStructure',
            [
                'synonym_supplier_brand_ids_from_checked',
                'chain_of_synonym_supplier_brand_ids_after_dfs'
            ]
        )
        # вот так с помощью словаря my_synonyms
        my_synonyms = {
            supplier_brand_id: MyStructure(
                # synonym_supplier_brand_ids_from_checked - это set, состоящий из supplier_brand_ids,
                # пары которых с данным supplier_brand_id (т.е. ключом) в synonym_pairs_checked имеет пометку check == +
                # Например, данные будут следующий вид:
                # my_synonyms[2] = MyStructure(synonym_supplier_brand_ids_from_checked={3, 2076}, ...}
                # my_synonyms[3] = MyStructure(synonym_supplier_brand_ids_from_checked={2}, ...}
                # my_synonyms[2076] = MyStructure(synonym_supplier_brand_ids_from_checked={2}, ...}
                synonym_supplier_brand_ids_from_checked=set(),
                # chain_of_synonym_supplier_brand_ids_after_dfs - это set, состоящий из цепочки всех
                # синонимомов supplier_brand_ids в том числе и ключевого supplier_brand_id
                chain_of_synonym_supplier_brand_ids_after_dfs=set()
            )
            for supplier_brand_id in supplier_brand_id_2_supplier_brand_key
        }
        # дальше переносим в synonym_supplier_brand_ids_from_checked каждого из supplier_brand_id их синонимы
        # т.е. другие supplier_brand_ids где в synonym_pairs_checked есть +
        # todo: учитывать ещё и минусы
        for pair_of_keys in synonym_pairs_checked:  # pair_of_keys это frozenset({SupplierBrandKey1, SupplierBrandKey2}
            if synonym_pairs_checked[pair_of_keys] == '+':
                supplier_brand_key1, supplier_brand_key2 = pair_of_keys
                try:
                    supplier_brand_id1 = supplier_brand_key_2_supplier_brand_id[supplier_brand_key1]
                    supplier_brand_id2 = supplier_brand_key_2_supplier_brand_id[supplier_brand_key2]
                except KeyError:
                    continue
                else:
                    my_synonyms[supplier_brand_id1].synonym_supplier_brand_ids_from_checked.add(supplier_brand_id2)
                    my_synonyms[supplier_brand_id2].synonym_supplier_brand_ids_from_checked.add(supplier_brand_id1)

        # это вспомгательная рекурсивная функция поиска в глубину
        # (цепочек синонимов)
        def dfs(supplier_brand_id: int) -> None:
            """
            Поиск в глубину цепочки брендов-синонимов
            """
            nonlocal visited_synonym_supplier_brand_ids
            nonlocal chain_of_synonym_supplier_brand_ids_in_dfs
            nonlocal my_synonyms
            visited_synonym_supplier_brand_ids[supplier_brand_id] = True
            chain_of_synonym_supplier_brand_ids_in_dfs.add(supplier_brand_id)
            for synonym_supplier_brand_id in my_synonyms[supplier_brand_id].synonym_supplier_brand_ids_from_checked:
                if not visited_synonym_supplier_brand_ids[synonym_supplier_brand_id]:
                    dfs(synonym_supplier_brand_id)

        # это цикл перебора supplier_brand_id c вызовами dfs для каждого из них
        for supplier_brand_id in my_synonyms:
            # chain_of_synonym_supplier_brand_ids_in_dfs - по сути ссылка на элемент
            # chain_of_synonym_supplier_brand_ids_after_dfs структуры MyStructure текущего supplier_brand_id.
            # Она нужна для заполнения этого элемента в функции dfs и передаётся в функцию как нелокальная переменная
            chain_of_synonym_supplier_brand_ids_in_dfs = \
                my_synonyms[supplier_brand_id].chain_of_synonym_supplier_brand_ids_after_dfs
            # visited_synonym_supplier_brand_ids - список посещённых вершин графа.
            # Тоже передаётся в функцию dfs как нелокальная переменная
            visited_synonym_supplier_brand_ids = {
                synonym_supplier_brand_id: False
                for synonym_supplier_brand_id in my_synonyms
            }
            dfs(supplier_brand_id)

        # в конце концов получаем все уникальные цепочки синонимов
        # Пример all_chain_of_synonym_supplier_brand_ids_after_dfs:
        # {
        #   frozenset({1621, 1622}),
        #   frozenset({4451}),
        #   frozenset({1704, 1775}),
        #   frozenset({1495}),
        #   frozenset({3013, 3014, 3015}),
        #   ...
        # }
        all_chain_of_synonym_supplier_brand_ids_after_dfs = {
            frozenset(my_synonyms[supplier_brand_id].chain_of_synonym_supplier_brand_ids_after_dfs)
            for supplier_brand_id in my_synonyms
        }

        # Получаем предыдущие названия цепочек брендов
        previous_brand_names = cls._get_previous_names()

        # Теперь мы получаем название цепочки синонимов из файла
        # todo: формирование словаря brand_chain

        brand_2_chain = {}
        for chain in all_chain_of_synonym_supplier_brand_ids_after_dfs:
            set_of_supplier_brand_keys_from_chain = frozenset(
                {
                    supplier_brand_id_2_supplier_brand_key[supplier_brand_id]
                    for supplier_brand_id in chain
                }
            )
            sorted_chain = sorted(set_of_supplier_brand_keys_from_chain, key=lambda x: (x.name, x.supplier_id))

            brand_name = constants.NOT_CHECKED_BRAND_P.format(
                ' | '.join(
                    '{} ({})'.format(
                        supplier_brand_key.name,
                        supplier_brand_key.supplier_id)
                    for supplier_brand_key in sorted_chain
                )
            )
            try:
                brand_check = previous_brand_names[set_of_supplier_brand_keys_from_chain]
            except KeyError:
                pass
            else:
                if brand_check.check == '+':
                    brand_name = brand_check.name

            brand_2_chain[brand_name] = chain
        brand_names = sorted(brand_2_chain)

        # Работа с базой: установка active в False в таблицe brand
        # noinspection SqlWithoutWhere
        cls.db.execute_without_results(
            statement="""UPDATE brand SET active = FALSE;""",
            many=False,
            commit=True
        )

        # Работа с базой: запись name, active в таблицу brand
        data = [(brand_name, True) for brand_name in brand_names]
        cls.db.execute_without_results(
            statement="""
                INSERT INTO brand (name, active)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE active = values(active);
            """,
            data=data,
            many=True,
            commit=True
        )

        # Работа с базой: очистка таблицы brand_supplier_brand
        cls.db.execute_without_results(
            statement="""TRUNCATE brand_supplier_brand;""",
            many=False,
            commit=True
        )

        # Работа с базой: запись brand_id, supplier_brand_id в таблицу brand_supplier_brand
        brand_id_2_brand_name, brand_name_2_brand_id = cls._get_brands()
        data = []
        for brand_name in brand_2_chain:
            brand_id = brand_name_2_brand_id[brand_name]
            for supplier_brand_id in brand_2_chain[brand_name]:
                data.append(
                    {
                        'brand_id': brand_id,
                        'supplier_brand_id': supplier_brand_id
                    }
                )
        cls.db.execute_without_results(
            statement="""
                INSERT INTO brand_supplier_brand (brand_id, supplier_brand_id)
                VALUES
                  (%(brand_id)s, %(supplier_brand_id)s);
            """,
            data=data,
            many=True,
            commit=True
        )

        # try:
        #     logging.info(
        #         '{} Обработка результатов ручной модерации брендов с записью в базу.'.format(constants.LOGGING_START)
        #     )
        #
        #     # Для дальнейшей работы получаем 2 отображения: brand_id_2_norm_name и norm_name_2_brand_id
        #     try:
        #         logging.debug(
        #             '{} Получение отображений brand_id_2_norm_name и norm_name_2_brand_id из базы данных.'.format(
        #                 constants.LOGGING_START
        #             )
        #         )
        #         brand_id_2_norm_name, norm_name_2_brand_id = \
        #             cls.db.get_from_dbtable_brand__brand_id_2_norm_name__and__norm_name_2_brand_id()
        #     finally:
        #         logging.debug(
        #             '{} Получение отображений brand_id_2_norm_name и norm_name_2_brand_id из базы данных.'.format(
        #                 constants.LOGGING_FINISH
        #             )
        #         )
        #
        #     # brand_2_set_of_synonyms представляет собой словарь
        #     # ключ: norm_name -- имя бренда
        #     # значение: список из двух элементов
        #     #       1-й элемент списка: множество синонимов (там где стоят "+") из moderation_items
        #     #       2-й элемент списка: множество синонимов после поиска синонимов в глубину функцией dfs
        #     brand_2_set_of_synonyms = {norm_name: [set()] for norm_name in norm_name_2_brand_id}
        #
        #     try:
        #         logging.debug('{} Чтение {}'.format(constants.LOGGING_START, constants.BRAND_SYNONYM_CHECKED_FILE))
        #         with open(
        #                 file=constants.BRAND_SYNONYM_CHECKED_FILE,
        #                 newline='',
        #                 encoding='utf8'
        #         ) as f_in:
        #             dialect = csv.Sniffer().sniff(f_in.read(20000))
        #             f_in.seek(0)
        #             csv_reader = csv.DictReader(f_in, dialect=dialect)
        #
        #             assert constants.BRAND_SYNONYM_CHECKED_FILE_NECESSARY_FIELDS_SET.issubset(
        #                 set(csv_reader.fieldnames)
        #             )
        #             n = 1
        #             while True:
        #                 try:
        #                     row = next(csv_reader)
        #                 except UnicodeDecodeError as e:
        #                     err_message = 'Не удаётся декодировать значение в строке номер {}. Ошибка: {}'.format(n,
        #                       e)
        #                     logging.error(err_message)
        #                     continue
        #                 except StopIteration:
        #                     break
        #                 else:
        #                     if '+' in row['check']:
        #                         # если стоит +, считается что brand1_norm_name и brand2_norm_name взаимные синонимы
        #                         brand1_norm_name = constants.normalize_brand(
        #                             constants.delete_unprintable(row['brand1'])
        #                         )
        #                         brand2_norm_name = constants.normalize_brand(
        #                             constants.delete_unprintable(row['brand2'])
        #                         )
        #                         # brand1 и brand2 должны иметься в базе
        #                         if (brand1_norm_name in norm_name_2_brand_id) \
        #                                 and (brand2_norm_name in norm_name_2_brand_id):
        #                             brand_2_set_of_synonyms[brand1_norm_name][0].add(brand2_norm_name)
        #                             brand_2_set_of_synonyms[brand2_norm_name][0].add(brand1_norm_name)
        #
        #     except FileNotFoundError:
        #         logging.error(
        #             'Файл {} не найден. '
        #             'Запись результатов ручной модерации брендов в базу данных не произведена.'.format(
        #                 constants.BRAND_SYNONYM_CHECKED_FILE
        #             )
        #         )
        #     else:
        #         # Обрабатываем результаты из файла
        #         def dfs(v):
        #             """
        #             Поиск в глубину цепочки брендов-синонимов
        #             """
        #             nonlocal visited_synonym_supplier_brand_ids
        #             visited_synonym_supplier_brand_ids[v] = True
        #             chain_of_synonym_supplier_brand_ids_in_dfs.add(v)
        #             for vertex in brand_2_set_of_synonyms[v][0]:
        #                 if not visited_synonym_supplier_brand_ids[vertex]:
        #                     dfs(vertex)
        #
        #         for norm_name in brand_2_set_of_synonyms:
        #             chain_of_synonym_supplier_brand_ids_in_dfs = set()  # цепочка (множество) взаимных синонимов
        #             visited_synonym_supplier_brand_ids = {brand_name: False for brand_name in norm_name_2_brand_id}
        #             dfs(norm_name)
        #             brand_2_set_of_synonyms[norm_name].append(chain_of_synonym_supplier_brand_ids_in_dfs)
        #
        #         all_synonyms = set()  # множество цепочек (frozenset) взаимных синонимов
        #         for norm_name in brand_2_set_of_synonyms:
        #             all_synonyms.add(frozenset(brand_2_set_of_synonyms[norm_name][-1]))
        #
        #         all_synonyms = sorted([sorted(synonyms) for synonyms in all_synonyms])  # сортируем по алфавиту
        #
        #         # if 0 and "Запись в файл":
        #         #     with open(
        #         #             file=constants.SYNONYM_CHAINS_FILE_PATH_AND_NAME,
        #         #             mode='w',
        #         #             encoding='utf8'
        #         #     ) as f_out:
        #         #         for synonym_chain in all_synonyms:
        #         #             print(' | '.join(brand for brand in synonym_chain), file=f_out)
        #
        #         if 0 and "Запись в базу true_brand как brand_name в верхнем регистре. Потом удалить":
        #             # Сначала очистим что было в прежде
        #             cls.db.clear_brand_true_brand_and_true_brand()
        #
        #             # пока считаем, что настоящие имена групп синонимов - это первое имя из группы в вехнем регистре
        #             # это нужно для заполнение таблицы  в бд
        #             true_brand_names = [dict(name=synonym_chain[0].upper(), ) for synonym_chain in all_synonyms]
        #             cls.db.insert_into_true_brand(true_brand_names)
        #
        #             # получаем коды настоящих имен производителей
        #             true_brand_name_2_id = cls.db.get_from_dbtable_true_brand__name_2_id()
        #             brand_id__true_brand_id = []
        #
        #             for synonym_chain in all_synonyms:
        #                 true_brand_id = true_brand_name_2_id[synonym_chain[0].upper()]
        #                 for norm_name in synonym_chain:
        #                     supplier_brand_key_2_supplier_brand_id = norm_name_2_brand_id[norm_name]
        #                     brand_id__true_brand_id.append(
        #                         dict(brand_id=supplier_brand_key_2_supplier_brand_id, true_brand_id=true_brand_id))
        #
        #             cls.db.insert_into_brand_true_brand(brand_id__true_brand_id)
        #
        #         if 1 and "Запись в базу":
        #
        #             # Получаем отображения norm_name->row для таблицы brand
        #             # Для этого считываем необходимые данные из таблиц brand и autocare_brand
        #             cursor = cls.db.connection.cursor(named_tuple=True)
        #             cursor.execute(
        #                 """
        #                     select b.supplier_brand_key_2_supplier_brand_id,
        #                            b.norm_name,
        #                            b.exact_name,
        #                            a.brand_name 'autocare_name',
        #                            b.keystone_name,
        #                            b.meyer_name,
        #                            b.premier_name,
        #                            b.trans_name,
        #                            b.turn14_name
        #                     from brand b
        #                            left join autocare_brand a using (autocare_brand_id);
        #                 """
        #             )
        #             dbtable_brand__norm_name_2_row = {row.norm_name: row for row in cursor}
        #             cursor.close()
        #
        #             # Вычисляем true_name для нормализованного norm_name бренда
        #             true_name_2_brand_id = {}
        #             for synonym_chain in all_synonyms:
        #                 true_name = None
        #                 for norm_name in synonym_chain:
        #                     exact_name = dbtable_brand__norm_name_2_row[norm_name].exact_name
        #                     if exact_name:
        #                         true_name = exact_name
        #                         break
        #                 if not true_name:
        #                     for norm_name in synonym_chain:
        #                         autocare_name = dbtable_brand__norm_name_2_row[norm_name].autocare_name
        #                         if autocare_name:
        #                             true_name = autocare_name
        #                             break
        #                 if not true_name:
        #                     for norm_name in synonym_chain:
        #                         true_name = dbtable_brand__norm_name_2_row[norm_name].turn14_name
        #                         if not true_name:
        #                             true_name = dbtable_brand__norm_name_2_row[norm_name].meyer_name
        #                             if not true_name:
        #                                 true_name = dbtable_brand__norm_name_2_row[norm_name].premier_name
        #                                 if not true_name:
        #                                     true_name = dbtable_brand__norm_name_2_row[norm_name].keystone_name
        #                                     if not true_name:
        #                                         true_name = dbtable_brand__norm_name_2_row[norm_name].trans_name
        #                         if true_name:
        #                             break
        #                 if not true_name:
        #                     raise Exception
        #                 true_name_2_brand_id[true_name] = [norm_name_2_brand_id[name] for name in synonym_chain]
        #
        #             # Сначала очистим что было в прежде
        #             cls.db.clear_brand_true_brand_and_true_brand()
        #
        #             # Запишем true_brand в базу
        #             true_brands = [dict(name=name) for name in sorted(true_name_2_brand_id)]
        #             cls.db.insert_into_true_brand(true_brands)
        #
        #             true_brand__name_2_id = cls.db.get_from_dbtable_true_brand__name_2_id()
        #             brands_true_brands = []
        #             for true_brand_name in true_name_2_brand_id:
        #                 for supplier_brand_key_2_supplier_brand_id in true_name_2_brand_id[true_brand_name]:
        #                     brands_true_brands.append(
        #                         dict(
        #                             brand_id=supplier_brand_key_2_supplier_brand_id,
        #                             true_brand_id=true_brand__name_2_id[true_brand_name]
        #                         )
        #                     )
        #             cls.db.insert_into_brand_true_brand(brands_true_brands)
        #     finally:
        #         logging.debug('{} Чтение {}'.format(constants.LOGGING_FINISH, constants.BRAND_SYNONYM_CHECKED_FILE))
        #
        # finally:
        #     logging.info(
        #         '{} Обработка результатов ручной модерации брендов с записью в базу.'.format(constants.LOGGING_FINISH)
        #     )

    # У Meyer есть category и subcategory в файлах.
    # Их можно извлекать при первом разборе fullparse=True в insert_into_specific_supplier_item
    # и сохранять в переменных класса meyer_category, meyer_subcategory
    # а дальше уже в update_meyer_item__category_subcategory добавлять в таблицы meyer_category и meyer_subcategory
    # и сопоставлять id в update_meyer_item__category_subcategory
    meyer_category = set()
    meyer_subcategory = set()

    @classmethod
    def insert_into_specific_supplier_item(cls) -> None:

        try:
            logging.info('{} Запись специфических данных по item\'ам в базу.'.format(constants.LOGGING_START))
            supplier_brand_ids = cls.db.get_from_supplier_brand__name_and_supplier_id_2_supplier_brand_id()

            for supplier in cls.suppliers:
                try:
                    logging.debug(
                        '{} Чтение {}.'.format(constants.LOGGING_START, supplier.INPUT_FILE)
                    )

                    with open(
                            file=supplier.INPUT_FILE,
                            newline='',
                            encoding='utf8'
                    ) as f_in:
                        csv_reader = csv.DictReader(f_in, dialect=constants.PROJECT_STANDARD_DIALECT)
                        assert set(csv_reader.fieldnames) == set(supplier.INPUT_FILE_NECESSARY_FIELDS_LIST)
                        try:
                            logging.info(
                                "{} Запись в базу данных полной информации по item'ам поставщика {}.".format(
                                    constants.LOGGING_START,
                                    supplier.SUPPLIER_NAME
                                )
                            )
                            data = []  # список database.Item для записи в таблицу item базы данных
                            for row in csv_reader:
                                item = supplier.item(row, full_parse=True)
                                if item.norm_brand and item.norm_mpn:
                                    key = (item.brand, supplier.id_in_db)
                                    try:
                                        supplier_brand_id = supplier_brand_ids[key]
                                    except KeyError:
                                        logging.error('Error key {} in supplier_brand_ids'.format(key))
                                        continue
                                    d = {
                                        'supplier_brand_id': supplier_brand_id,
                                        'norm_mpn': item.norm_mpn,
                                    }
                                    if supplier == Keystone:
                                        d.update(
                                            {
                                                'LongDescription': item.LongDescription,
                                                'JobberPrice': item.JobberPrice,
                                                'Cost': item.Cost,
                                                'Fedexable': item.Fedexable,
                                                'ExeterQty': item.ExeterQty,
                                                'MidWestQty': item.MidWestQty,
                                                'SouthEastQty': item.SouthEastQty,
                                                'TexasQty': item.TexasQty,
                                                'PacificNWQty': item.PacificNWQty,
                                                'GreatLakesQty': item.GreatLakesQty,
                                                'CaliforniaQty': item.CaliforniaQty,
                                                'TotalQty': item.TotalQty,
                                                'UPCCode': item.UPCCode,
                                                'Prop65Toxicity': item.Prop65Toxicity,
                                                'HazardousMaterial': item.HazardousMaterial
                                            }
                                        )
                                    elif supplier == Meyer:
                                        d.update(
                                            {
                                                'Description': item.Description,
                                                'Jobber_Price': item.Jobber_Price,
                                                'Customer_Price': item.Customer_Price,
                                                'UPC': item.UPC,
                                                'MAP': item.MAP,
                                                'Length': item.Length,
                                                'Width': item.Width,
                                                'Height': item.Height,
                                                'Weight': item.Weight,
                                                'LTL_Eligible': item.LTL_Eligible,
                                                'Discontinued': item.Discontinued
                                            }
                                        )
                                        if item.Category:
                                            cls.meyer_category.add(item.Category)
                                        if item.Sub_Category:
                                            cls.meyer_subcategory.add(item.Sub_Category)
                                    elif supplier == Premier:
                                        d.update(
                                            {
                                                'Distributor_Cost': item.Distributor_Cost,
                                                'Package_Quantity': item.Package_Quantity,
                                                'Core_Price': item.Core_Price,
                                                'UPC': item.UPC,
                                                'Part_Description': item.Part_Description,
                                                'Inventory_Count': item.Inventory_Count,
                                                'Inventory_Type': item.Inventory_Type
                                            }
                                        )
                                    elif supplier == Trans:
                                        d.update(
                                            {
                                                'CA': item.CA,
                                                'TX': item.TX,
                                                'FL': item.FL,
                                                'CO': item.CO,
                                                'OH': item.OH,
                                                'ID': item.ID,
                                                'PA': item.PA,
                                                'LIST_PRICE': item.LIST_PRICE,
                                                'JOBBER_PRICE': item.JOBBER_PRICE,
                                                'TOTAL': item.TOTAL,
                                                'STATUS': item.STATUS,
                                            }
                                        )
                                    elif supplier == Turn14:
                                        d.update(
                                            {
                                                'Description': item.Description,
                                                'Cost': item.Cost,
                                                'Retail': item.Retail,
                                                'Jobber': item.Jobber,
                                                'CoreCharge': item.CoreCharge,
                                                'Map': item.Map,
                                                'Other': item.Other,
                                                'OtherName': item.OtherName,
                                                'EastStock': item.EastStock,
                                                'WestStock': item.WestStock,
                                                'CentralStock': item.CentralStock,
                                                'Stock': item.Stock,
                                                'MfrStock': item.MfrStock,
                                                'MfrStockDate': item.MfrStockDate,
                                                'DropShip': item.DropShip,
                                                'DSFee': item.DSFee,
                                                'Weight': item.Weight
                                            }
                                        )
                                    data.append(d)

                                    if len(data) == 2000:
                                        # записываем в базу частями по 2000 item'ов, чтобы избежать чрезмерной нагрузки
                                        cls.db.insert_into_specific_supplier_item(supplier, data)
                                        data.clear()
                            if data:
                                # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                                cls.db.insert_into_specific_supplier_item(supplier, data)
                                data.clear()
                        finally:
                            logging.info(
                                "{} Запись в базу данных полной информации по item'ам поставщика {}.".format(
                                    constants.LOGGING_FINISH,
                                    supplier.SUPPLIER_NAME
                                )
                            )

                finally:
                    logging.debug(
                        '{} Чтение {}.'.format(constants.LOGGING_FINISH, supplier.INPUT_FILE)
                    )
        finally:
            logging.info('{} Запись специфических данных по item\'ам в базу.'.format(constants.LOGGING_FINISH))

    @classmethod
    def update_meyer_item__category_subcategory(cls) -> None:
        logging.info('START')
        # Вставляем category
        cls.db.execute_without_results(
            statement="""INSERT IGNORE INTO meyer_category (name) VALUES (%s);""",
            data=[(name,) for name in sorted(cls.meyer_category)],
            many=True,
            commit=True

        )
        # Вставляем subcategory
        cls.db.execute_without_results(
            statement="""INSERT IGNORE INTO meyer_subcategory (name) VALUES (%s);""",
            data=[(name,) for name in sorted(cls.meyer_subcategory)],
            many=True,
            commit=True
        )
        # Получаем meyer_category_ids
        cursor = cls.db.execute_with_results(
            statement="""
                SELECT
                  meyer_category_id,
                  name
                FROM meyer_category;"""
        )
        meyer_category_ids = {row.name: row.meyer_category_id for row in cursor}
        cursor.close()
        # Получаем meyer_subcategory_ids
        cursor = cls.db.execute_with_results(
            statement="""
                SELECT
                  meyer_subcategory_id,
                  name
                FROM meyer_subcategory;"""
        )
        meyer_subcategory_ids = {row.name: row.meyer_subcategory_id for row in cursor}
        cursor.close()

        try:
            logging.info(
                "{} Запись в базу данных информации о category и subcategory по item'ам поставщика {}.".format(
                    constants.LOGGING_START,
                    Meyer.SUPPLIER_NAME
                )
            )
            meyer_item_ids = cls.db.get_supplier_number_2_supplier_item_id(Meyer.id_in_db)

            try:
                logging.debug('{} Чтение {}.'.format(constants.LOGGING_START, Meyer.INPUT_FILE))

                with open(
                        file=Meyer.INPUT_FILE,
                        newline='',
                        encoding='utf8'
                ) as f_in:
                    csv_reader = csv.DictReader(f_in, dialect=constants.PROJECT_STANDARD_DIALECT)
                    assert set(csv_reader.fieldnames) == set(Meyer.INPUT_FILE_NECESSARY_FIELDS_LIST)

                    data = []  # список database.Item для записи в таблицу item базы данных
                    for row in csv_reader:
                        item = Meyer.item(row, full_parse=True)
                        if item.number:
                            try:
                                supplier_item_id = meyer_item_ids[item.number]
                            except KeyError:
                                continue
                            d = {
                                'supplier_item_id': supplier_item_id,
                                'meyer_category_id': meyer_category_ids.get(item.Category),
                                'meyer_subcategory_id': meyer_subcategory_ids.get(item.Sub_Category)
                            }
                            data.append(d)

                            if len(data) == 5000:
                                # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                                cls.db.update_meyer_item__category_subcategory(data)
                                data.clear()
                    if data:
                        # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                        cls.db.update_meyer_item__category_subcategory(data)
                        data.clear()

            finally:
                logging.debug('{} Чтение {}.'.format(constants.LOGGING_FINISH, Meyer.INPUT_FILE))

        finally:
            logging.info(
                "{} Запись в базу данных информации о category и subcategory по item'ам поставщика {}.".format(
                    constants.LOGGING_FINISH,
                    Meyer.SUPPLIER_NAME
                )
            )

    @classmethod
    def update_meyer_item__inventory(cls) -> None:
        try:
            logging.info(
                "{} Запись в базу данных дополнительной информации по item'ам поставщика {}.".format(
                    constants.LOGGING_START,
                    Meyer.SUPPLIER_NAME
                )
            )
            meyer_item_ids = cls.db.get_supplier_number_2_supplier_item_id(Meyer.id_in_db)

            try:
                logging.debug(
                    '{} Чтение {}.'.format(constants.LOGGING_START, Meyer.INPUT_FILE_INVENTORY)
                )

                with open(
                        file=Meyer.INPUT_FILE_INVENTORY,
                        newline='',
                        encoding='utf8'
                ) as f_in:
                    csv_reader = csv.DictReader(f_in, dialect=constants.PROJECT_STANDARD_DIALECT)
                    assert set(csv_reader.fieldnames) == set(Meyer.INPUT_FILE_INVENTORY_NECESSARY_FIELDS_LIST)

                    data = []  # список database.Item для записи в таблицу item базы данных
                    for row in csv_reader:
                        item = Meyer.item(row, inventory=True)
                        if item.Meyer_SKU:
                            try:
                                supplier_item_id = meyer_item_ids[item.Meyer_SKU]
                            except KeyError:
                                continue
                            d = {
                                'supplier_item_id': supplier_item_id,
                                'Qty_008': item.Qty_008,
                                'Qty_032': item.Qty_032,
                                'Qty_041': item.Qty_041,
                                'Qty_044': item.Qty_044,
                                'Qty_053': item.Qty_053,
                                'Qty_062': item.Qty_062,
                                'Qty_063': item.Qty_063,
                                'Qty_065': item.Qty_065,
                                'Qty_068': item.Qty_068,
                                'Qty_069': item.Qty_069,
                                'Qty_070': item.Qty_070,
                                'Qty_071': item.Qty_071,
                                'Qty_072': item.Qty_072,
                                'Qty_077': item.Qty_077,
                                'Qty_093': item.Qty_093,
                                'Qty_094': item.Qty_094,
                                'Qty_098': item.Qty_098,
                                'Discontinued': item.Discontinued
                            }
                            data.append(d)

                            if len(data) == 5000:
                                # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                                cls.db.update_meyer_item__inventory(data)
                                data.clear()
                    if data:
                        # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                        cls.db.update_meyer_item__inventory(data)
                        data.clear()

            finally:
                logging.debug(
                    '{} Чтение {}.'.format(constants.LOGGING_FINISH, Meyer.INPUT_FILE_INVENTORY)
                )

        finally:
            logging.info(
                "{} Запись в базу данных дополнительной информации по item'ам поставщика {}.".format(
                    constants.LOGGING_FINISH,
                    Meyer.SUPPLIER_NAME
                )
            )


if __name__ == '__main__':
    # Именно ЗДЕСЬ начинается работа программы
    Program.run()
