# noinspection SpellCheckingInspection
"""
Заносит в базу данных данные из файла TransAmerican, который иногда приходит на почту.
В файле должны быть колонки: constants.SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE_NECESSARY_FIELDS_SET.
Т.к. файл приходит на почту в формате .xlsb то его предварительно нужно сохранить в .csv с помощью excel или другого ПО.
Также файл желательно сохранить как constants.SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE, хотя это и настраивается через
параметры программы.

Перед запуском программы поменять диалект csv-файла (значение delimiter и т.д.).
"""

import argparse
import csv
import logging
from csv import Dialect

import constants
import database
from items import TransFileItem
from suppliers import Trans


class FileDialect(Dialect):
    """Диалект файла, который нужно поменять"""
    delimiter = ';'
    quotechar = '"'
    escapechar = None
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL


# noinspection PyTypeChecker
csv.register_dialect('file_dialect', FileDialect)


class Program:
    def __init__(self,
                 *,
                 db_option_file: str,
                 file: str
                 ) -> None:
        """
        Инициализирует экземпляр программы
        :param db_option_file: файл конфигурации базы данных
        :param file: входной файл
        """
        self.db = database.Database(option_files=db_option_file)
        self.file = file

    def run(self) -> None:
        try:
            logging.debug('{} Чтение {}.'.format(constants.LOGGING_START, self.file))
            with open(
                    file=self.file,
                    encoding='utf8',
                    newline='',
                    errors='replace'
            ) as f_in:
                reader = csv.DictReader(f_in, dialect='file_dialect')
                assert set(reader.fieldnames) == constants.SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE_NECESSARY_FIELDS_SET
                items = []
                for row in reader:
                    item = TransFileItem(row)
                    items.append(item)

                # вставка supplier_brand
                try:
                    logging.debug('{} вставка в supplier_brand'.format(constants.LOGGING_START))
                    supplier_brands = {(Trans.id_in_db, item.brand) for item in items}
                    data = []
                    for supplier_id, name in sorted(supplier_brands, key=lambda x: x[-1].casefold()):
                        data.append({'supplier_id': supplier_id, 'name': name})
                    self.db.insert_into_supplier_brand(data)
                    del supplier_brands, data
                finally:
                    logging.debug('{} вставка в supplier_brand'.format(constants.LOGGING_FINISH))

                supplier_brand_ids = self.db.get_from_supplier_brand__name_and_supplier_id_2_supplier_brand_id()

                try:
                    logging.debug('{} вставка в supplier_item'.format(constants.LOGGING_START))
                    data = []
                    for item in items:
                        if item.norm_brand and item.norm_mpn:
                            key = (item.brand, Trans.id_in_db)
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
                                self.db.insert_into_supplier_item(data)
                                data.clear()
                    if data:
                        # дописываем оставшиеся item'ы
                        self.db.insert_into_supplier_item(data)
                        data.clear()
                finally:
                    logging.debug('{} вставка в supplier_item'.format(constants.LOGGING_FINISH))

                try:
                    logging.debug('{} вставка в trans_item'.format(constants.LOGGING_START))
                    data = []
                    for item in items:
                        if item.norm_brand and item.norm_mpn:
                            key = (item.brand, Trans.id_in_db)
                            try:
                                supplier_brand_id = supplier_brand_ids[key]
                            except KeyError:
                                logging.error('Error key {} in supplier_brand_ids'.format(key))
                                continue
                            d = {
                                'supplier_brand_id': supplier_brand_id,
                                'norm_mpn': item.norm_mpn,
                                'DESCRIPTION': item.Description,
                                'YOUR_PRICE': item.Your_Price,
                                'JOBBER_PRICE': item.Jobber,
                                'MAP_CONFIRM_W_JOBBER': item.MAP_CONFIRM_W_JOBBER,
                                'CORE_PRICE': item.Core_Price,
                                'FEDERAL_EXCISE_TAX': item.Federal_Excise_Tax,
                                'OVERSIZE': item.Oversize,
                                'STATUS': item.Status
                            }
                            data.append(d)

                            if len(data) == 2000:
                                # записываем в базу частями по 2000 item'ов, чтобы избежать чрезмерной нагрузки
                                self.db.insert_into_trans_supplier_item_from_file(data)
                                data.clear()
                    if data:
                        # записываем в базу частями по 5000 item'ов, чтобы избежать чрезмерной нагрузки
                        self.db.insert_into_trans_supplier_item_from_file(data)
                        data.clear()
                finally:
                    logging.debug('{} вставка в trans_item'.format(constants.LOGGING_FINISH))

        finally:
            logging.debug('{} Чтение {}.'.format(constants.LOGGING_FINISH, self.file))


def parse_program_arguments() -> argparse.Namespace:
    """
    Парсит входные параметры программы
    """

    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description="Вводит в базу данных файл Transamerican c полями: {}".format(
            constants.SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE_NECESSARY_FIELDS_SET
        )
    )

    parser.add_argument(
        '--db-option-file',
        dest='db_option_file',
        action='store',
        default=constants.DATABASE_DB_CONFIG_FILE,
        help='файл конфигурации подключения к базе данных (default: %(default)s)'
    )

    parser.add_argument(
        '--file',
        dest='file',
        action='store',
        default=constants.SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE,
        help="файл (default: %(default)s)",
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
        file=args.file
    )
    program.run()
