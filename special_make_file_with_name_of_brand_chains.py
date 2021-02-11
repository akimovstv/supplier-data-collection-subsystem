import argparse
import csv
import logging
import os
from collections import defaultdict
from typing import Optional, Dict, FrozenSet, Set

import constants
import database
from constants import SupplierBrandKey, BrandCheck, split_chain


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
        :param previous_file: файл предыдущей версии
        :param new_file: выходной файл
        """
        self.db = database.Database(option_files=db_option_file)
        self.previous_file = previous_file
        self.new_file = new_file

    def run(self) -> None:
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self.run.__name__))

            # Считываем данные прошлой версии
            previous_data = self._get_previous()

            # data from database
            data = self._get_data_from_db()

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
                        fieldnames=['name', 'check', 'chain'],
                        dialect=constants.PROJECT_STANDARD_DIALECT
                    )
                    writer.writeheader()
                    for brand_id in data:
                        frozen_chain = frozenset(data[brand_id])
                        sorted_chain = sorted(data[brand_id], key=lambda x: (x.name, x.supplier_id))
                        chain_string = ' | '.join(
                            '{} ({})'.format(
                                supplier_brand_key.name,
                                supplier_brand_key.supplier_id)
                            for supplier_brand_key in sorted_chain
                        )

                        brand_name = constants.NOT_CHECKED_BRAND_P.format(chain_string)
                        check = ''
                        try:
                            brand_key = previous_data[frozen_chain]
                            if brand_key.check == '+':
                                check = '+'
                                brand_name = brand_key.name
                        except KeyError:
                            pass

                        writer.writerow(
                            {
                                'name': brand_name,
                                'check': check,
                                'chain': chain_string
                            }
                        )

            finally:
                logging.debug('{} Запись {}.'.format(constants.LOGGING_FINISH, self.new_file))
        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self.run.__name__))

    def _get_previous(self) -> Dict[FrozenSet[SupplierBrandKey], BrandCheck]:
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self._get_previous.__name__))

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
                        assert constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE_NECESSARY_FIELDS_SET.issubset(
                            set(reader.fieldnames))

                        previous_data = {}
                        for row in reader:
                            brand_key = BrandCheck(name=row['name'], check=row['check'])
                            supplier_brand_keys = split_chain(row['chain'])
                            previous_data[supplier_brand_keys] = brand_key

                except FileNotFoundError as e:
                    logging.error(e)
                    return {}
                finally:
                    logging.debug('{} Чтение {}'.format(constants.LOGGING_FINISH, self.previous_file))
            else:
                previous_data = {}

            return previous_data

        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self._get_previous.__name__))

    def _get_data_from_db(self) -> Dict[int, Set[SupplierBrandKey]]:
        """
        Возвращает из базы данных {brand_id: {supplier_brand_key1, supplier_brand_key2, ...}}
        Мы считаем что в таблице brand_supplier_brand есть только актуальные данные о соответствии
        supplier_brand и brand
        """
        try:
            logging.info('{} {}'.format(constants.LOGGING_START, self._get_data_from_db.__name__))

            statement = """
                SELECT
                  sb.name        AS supplier_brand_name,
                  sb.supplier_id AS supplier_id,
                  b.brand_id     AS brand_id,
                  b.name         AS brand_name
                FROM supplier_brand sb
                       INNER JOIN brand_supplier_brand bsb 
                            ON sb.supplier_brand_id = bsb.supplier_brand_id
                       INNER JOIN brand b 
                            ON bsb.brand_id = b.brand_id;
            """
            cursor = self.db.execute_with_results(statement=statement)
            data = defaultdict(set)
            try:
                for row in cursor:
                    data[row.brand_id].add(
                        SupplierBrandKey(
                            name=row.supplier_brand_name,
                            supplier_id=row.supplier_id
                        )
                    )
            finally:
                cursor.close()
            return data

        finally:
            logging.info('{} {}'.format(constants.LOGGING_FINISH, self._get_data_from_db.__name__))


def parse_program_arguments() -> argparse.Namespace:
    """
    Парсит входные параметры программы
    """

    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description="Создаёт файл с именами цепочек синонимов"
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
        default=constants.PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE,
        help="файл предыдущей версии, из которого переносить результаты (default: %(default)s)",
    )

    parser.add_argument(
        '--new-file',
        dest='new_file',
        action='store',
        default=constants.SPECIAL_MAKE_FILE_WITH_NAME_OF_BRAND_CHAINS_OUT_FILE,
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
