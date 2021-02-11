"""
Формирует файл со всеми возможными парами брендов из базы данных для ручной проверки каждой пары брендов на идентичность
(синонимичность).
При этом брендам с одинаковыми нормализованными названиями автоматически ставится пометка "+"
Также переносятся пометки из предыдущей версии файла ручной проверки (этот перенос имеет наивысший приоритет, т.е.
пометка "+" в для пар брендов, у которых совпадают нормализованные имена может быть изменена, если в предыдущей версии
файла она имеется и отличается от "+")

После внесения вручную данных в файл модерации получаемый данной программой необходимо записать данные в базу данных.
И после этого обязательно запустить special_make_file_with_name_of_brand_chains.py и проверить его вручную!!!
"""

import argparse
import csv
import logging
import os
from collections import defaultdict
from typing import Optional, Dict, FrozenSet

import constants
import database
from constants import SupplierBrandKey


class Program:
    def __init__(self,
                 *,
                 db_option_file: Optional[str],
                 previous_file: Optional[str],
                 new_file: Optional[str]
                 ) -> None:
        """
        Инициализирует экземпляр программы
        :param db_option_file: файл конфигурации базы данных
        :param previous_file: файл проверки пар брендов предыдущей версии
        :param new_file: выходной файл
        """
        self.db = database.Database(option_files=db_option_file)
        self.previous_file = previous_file
        self.new_file = new_file

    def run(self) -> None:
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self.run.__name__))

            # Считываем проверки прошлой версии
            checked = self._get_checked()

            # brands from database
            brands = self._get_brands()

            # items from database
            items = self._get_items()

            # получаем отсортированный список sorted_brand_ids из brands
            sorted_brand_ids = sorted(
                brands,
                key=lambda x: constants.normalize_brand(brands[x].name)
            )

            # создаём папку для выходного файла
            dirname = os.path.dirname(self.new_file)
            if dirname:
                os.makedirs(dirname, exist_ok=True)

            # производим запись в выходной файл
            try:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_START, self.new_file))

                with open(
                        file=self.new_file,
                        mode='w',
                        encoding='utf8',
                        newline=''
                ) as f_out:
                    writer = csv.DictWriter(
                        f=f_out,
                        fieldnames=['check', 'brand_1__name', 'brand_1__supplier_id', 'brand_1__number_of_mpns',
                                    'brand_2__name', 'brand_2__supplier_id', 'brand_2__number_of_mpns',
                                    'pairs_of_common_numbers'],
                        dialect=constants.PROJECT_STANDARD_DIALECT
                    )
                    writer.writeheader()

                    # проходим по всем возможным парам брендов (без повторений)
                    for i1 in range(0, len(sorted_brand_ids) - 1):
                        for i2 in range(i1 + 1, len(sorted_brand_ids)):
                            supplier_brand_id_1 = sorted_brand_ids[i1]
                            supplier_brand_id_2 = sorted_brand_ids[i2]

                            # находим общие номера у каждой пары брендов
                            common_norm_mpns = \
                                items[supplier_brand_id_1].keys() & items[supplier_brand_id_2].keys()
                            pairs_of_common_numbers = [
                                (
                                    items[supplier_brand_id_1][common_norm_mpn],
                                    items[supplier_brand_id_2][common_norm_mpn],
                                )
                                for common_norm_mpn in common_norm_mpns
                            ] if common_norm_mpns else ''

                            check = ''

                            # + для брендов, у которых нормализованные имена совпадают
                            if constants.normalize_brand(brands[supplier_brand_id_1].name) == \
                                    constants.normalize_brand(brands[supplier_brand_id_2].name):
                                check = '+'

                            # перенос check из прошлой версии
                            supplier_brand_key_1 = SupplierBrandKey(
                                name=brands[supplier_brand_id_1].name,
                                supplier_id=brands[supplier_brand_id_1].supplier_id
                            )
                            supplier_brand_key_2 = SupplierBrandKey(
                                name=brands[supplier_brand_id_2].name,
                                supplier_id=brands[supplier_brand_id_2].supplier_id
                            )
                            key = frozenset((supplier_brand_key_1, supplier_brand_key_2))
                            try:
                                previous_check = checked[key]
                            except KeyError:
                                pass
                            else:
                                check = previous_check

                            # записываем данные в выходной файл
                            writer.writerow(
                                {
                                    'check': check,
                                    'brand_1__name': brands[supplier_brand_id_1].name,
                                    'brand_1__supplier_id': brands[supplier_brand_id_1].supplier_id,
                                    'brand_1__number_of_mpns': len(items[supplier_brand_id_1]),
                                    'brand_2__name': brands[supplier_brand_id_2].name,
                                    'brand_2__supplier_id': brands[supplier_brand_id_2].supplier_id,
                                    'brand_2__number_of_mpns': len(items[supplier_brand_id_2]),
                                    'pairs_of_common_numbers': pairs_of_common_numbers
                                }
                            )
                            logging.info('Запись пары: {} {}'.format(supplier_brand_id_1, supplier_brand_id_2))

            finally:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_FINISH, self.new_file))

        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self._get_brands.__name__))

    def _get_checked(self) -> Dict[FrozenSet[SupplierBrandKey], str]:
        """
        Создаёт и возвращает словарь checked из файла предыдущей версии self.previous_version, если он задан.
        checked имеет вид:
        {
            frozenset({supplier_brand_key_1, supplier_brand_key_2}): check
        }
        """
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self._get_checked.__name__))

            if self.previous_file:
                try:
                    logging.debug('{} Чтение {}'.format(constants.LOGGING_START, self.previous_file))
                    with open(
                            file=self.previous_file,
                            mode='r',
                            encoding='utf8',
                            newline=''
                    ) as f_in:
                        csv.field_size_limit(1000000)
                        dialect = csv.Sniffer().sniff(f_in.read(20000))
                        f_in.seek(0)
                        reader = csv.DictReader(f_in, dialect=dialect)
                        assert constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE_NECESSARY_FIELDS_SET.issubset(
                            set(reader.fieldnames))
                        checked = {}
                        for row in reader:
                            check = row['check']
                            if check:
                                supplier_brand_key_1 = SupplierBrandKey(
                                    name=row['brand_1__name'],
                                    supplier_id=int(row['brand_1__supplier_id'])
                                )
                                supplier_brand_key_2 = SupplierBrandKey(
                                    name=row['brand_2__name'],
                                    supplier_id=int(row['brand_2__supplier_id'])
                                )
                                key = frozenset((supplier_brand_key_1, supplier_brand_key_2))
                                checked[key] = check
                except FileNotFoundError as e:
                    logging.error(e)
                    raise
                finally:
                    logging.debug('{} Чтение {}'.format(constants.LOGGING_FINISH, self.previous_file))
            else:
                checked = {}

            return checked

        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self._get_checked.__name__))

    def _get_brands(self) -> Dict[int, SupplierBrandKey]:
        """
        Создаёт и возвращает словарь brands из базы данных.
        brands имеет вид:
        {
            supplier_brand_id: supplier_brand_key
        }
        """
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self._get_brands.__name__))

            statement = """
                SELECT
                  supplier_brand_id AS supplier_brand_id, 
                  name              AS supplier_brand_name,
                  supplier_id       AS supplier_id
                FROM supplier_brand
            """
            cursor = self.db.execute_with_results(statement=statement)
            brands = {}
            try:
                for row in cursor:
                    brands[row.supplier_brand_id] = SupplierBrandKey(
                        name=row.supplier_brand_name,
                        supplier_id=row.supplier_id
                    )
            finally:
                cursor.close()
            return brands

        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self._get_brands.__name__))

    def _get_items(self) -> Dict[int, Dict[str, str]]:
        """
        Создаёт и возвращает словарь items из базы данных.
        items имеет вид:
        {supplier_brand_id: {supplier_item_norm_mpn: supplier_item_number}}
        """
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self._get_items.__name__))

            statement = """
                SELECT
                  supplier_brand_id AS supplier_brand_id,
                  norm_mpn          AS supplier_item_norm_mpn,
                  number            AS supplier_item_number
                FROM supplier_item;
            """
            cursor = self.db.execute_with_results(statement=statement)
            items = defaultdict(dict)
            try:
                for row in cursor:
                    items[row.supplier_brand_id][row.supplier_item_norm_mpn] = row.supplier_item_number
            finally:
                cursor.close()
            return items

        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self._get_items.__name__))


def parse_program_arguments() -> argparse.Namespace:
    """
    Парсит входные параметры программы
    """

    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description="Создаёт файл с парами брендов для ручной модерации."
    )

    parser.add_argument(
        '--db-option-file',
        dest='db_option_file',
        action='store',
        default=constants.DATABASE_DB_CONFIG_FILE,
        help='файл конфигурации подключения к базе данных (default: %(default)s)'
    )

    parser.add_argument(
        '--previous-file',
        dest='previous_file',
        action='store',
        default=constants.PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE,
        help="файл предыдущей версии, из которого переносить результаты (default: %(default)s)",
    )

    parser.add_argument(
        '--new-file',
        dest='new_file',
        action='store',
        default=constants.SPECIAL_MAKE_FILE_WITH_BRAND_PAIRS_OUT_FILE,
        help="выходной файл (default: %(default)s)",
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_program_arguments()

    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(args)

    program = Program(
        db_option_file=args.db_option_file,
        previous_file=args.previous_file,
        new_file=args.new_file
    )
    program.run()
