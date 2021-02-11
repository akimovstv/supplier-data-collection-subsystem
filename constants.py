"""
Независимые вспомогательные функции и константы, которые нужны в других модулях.
"""

import csv
import os
import re
from collections import namedtuple
from string import punctuation, whitespace, printable
from typing import FrozenSet


class MyDialect(csv.Dialect):
    """Мой диалект для нормализованных csv-файлов"""
    delimiter = ';'
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = csv.QUOTE_MINIMAL


PROJECT_STANDARD_DIALECT = MyDialect()
# =================================================================

NUMBER_TRANSLATION_TABLE = str.maketrans('', '', punctuation + whitespace)

BRAND_TRANSLATION_TABLE = str.maketrans(dict.fromkeys(punctuation + whitespace, ' '))

p_unprintable = re.compile(r'[^{}]'.format(printable))


def delete_unprintable(s: str) -> str:
    return p_unprintable.sub('', s)


def normalize_number(number: str) -> str:
    return number.translate(NUMBER_TRANSLATION_TABLE).casefold()


def normalize_brand(brand: str) -> str:
    return ' '.join(brand.casefold().translate(BRAND_TRANSLATION_TABLE).split())


# =================================================================

LOGGING_START = 'НАЧАЛО:'
LOGGING_FINISH = 'КОНЕЦ: '

# =================================================================

DATA_DIR = 'DATA'

API_MEYER_BACKUP_DIR = os.path.join(DATA_DIR, 'api_meyer_BACKUP')
API_PREMIER_BACKUP_DIR = os.path.join(DATA_DIR, 'api_premier_BACKUP')
API_TURN14_BACKUP_DIR = os.path.join(DATA_DIR, 'api_turn14_BACKUP')
DATABASE_IN_DIR = os.path.join(DATA_DIR, 'database_IN')
PARSE_SUPPLIERS_FILES_BACKUP_DIR = os.path.join(DATA_DIR, 'parse_suppliers_files_BACKUP')
PARSE_SUPPLIERS_FILES_IN_DIR = os.path.join(DATA_DIR, 'parse_suppliers_files_IN')
PARSE_SUPPLIERS_FILES_TEMP_DIR = os.path.join(DATA_DIR, 'parse_suppliers_files_TEMP')
SPECIAL_GET_ALL_WEATHER_TECH_ITEMS_OUT_DIR = os.path.join(DATA_DIR, 'special_get_all_weather_tech_items_OUT')
SPECIAL_GET_RANCHO_PRICES_OUT_DIR = os.path.join(DATA_DIR, 'special_get_rancho_prices_OUT')
SPECIAL_MAKE_FILE_WITH_BRAND_PAIRS_OUT_DIR = os.path.join(DATA_DIR, 'special_make_file_with_brand_pairs_OUT')
SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_DIR = os.path.join(DATA_DIR, 'special_make_trans_prefixes_dict_IN')
SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_DIR = os.path.join(DATA_DIR, 'special_make_trans_prefixes_dict_OUT')
SPECIAL_MAKE_FILE_WITH_NAME_OF_BRAND_CHAINS_OUT_DIR = os.path.join(
    DATA_DIR,
    'special_make_file_with_name_of_brand_chains_OUT'
)
SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_DIR = os.path.join(DATA_DIR, 'special_input_trans_file_into_db_IN')

API_MEYER_ITEM_INFORMATION_BACKUP_FILE_T = os.path.join(API_MEYER_BACKUP_DIR, 'meyer_item_information_{}.jl')
API_PREMIER_PRICING_BACKUP_FILE_T = os.path.join(API_PREMIER_BACKUP_DIR, 'premier_pricing_{}.jl')
API_PREMIER_INVENTORY_BACKUP_FILE_T = os.path.join(API_PREMIER_BACKUP_DIR, 'premier_inventory_{}.jl')
API_TURN14_ALL_ITEMS_BACKUP_FILE_T = os.path.join(API_TURN14_BACKUP_DIR, 'turn14_all_items_{}.jl')
API_TURN14_ALL_ITEM_DATA_BACKUP_FILE_T = os.path.join(API_TURN14_BACKUP_DIR, 'turn14_all_item_data_{}.jl')

PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE = os.path.join(PARSE_SUPPLIERS_FILES_IN_DIR, 'brand_pairs_checked.csv')
PARSE_SUPPLIERS_FILES_BRAND_SYNONYM_CHECKED_FILE_NECESSARY_FIELDS_SET = {
    'check', 'brand_1__name', 'brand_1__supplier_id', 'brand_2__name', 'brand_2__supplier_id'
}

PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE = os.path.join(PARSE_SUPPLIERS_FILES_IN_DIR, 'brand_names_of_chains.csv')
PARSE_SUPPLIERS_FILES_BRAND_NAME_OF_CHAIN_FILE_NECESSARY_FIELDS_SET = {'name', 'check', 'chain'}

DATABASE_DB_CONFIG_FILE = os.path.join(DATABASE_IN_DIR, 'db_config.cnf')

SPECIAL_GET_ALL_WEATHER_TECH_ITEMS_OUT_FILE = os.path.join(
    SPECIAL_GET_ALL_WEATHER_TECH_ITEMS_OUT_DIR,
    'WeatherTech.csv'
)

SPECIAL_GET_RANCHO_PRICES_OUT_FILE = os.path.join(
    SPECIAL_GET_RANCHO_PRICES_OUT_DIR,
    'Rancho_prices.csv'
)

SPECIAL_MAKE_FILE_WITH_BRAND_PAIRS_OUT_FILE = os.path.join(
    SPECIAL_MAKE_FILE_WITH_BRAND_PAIRS_OUT_DIR,
    'brand_pairs_checked.csv'
)

SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_FILE = os.path.join(
    SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_DIR,
    'trans_code.csv'
)

SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_FILE_NECESSARY_FIELDS_SET = {'prefix', 'brand'}

SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_FILE = os.path.join(
    SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_DIR,
    'trans_prefix_brand.py'
)

SPECIAL_MAKE_FILE_WITH_NAME_OF_BRAND_CHAINS_OUT_FILE = os.path.join(
    SPECIAL_MAKE_FILE_WITH_NAME_OF_BRAND_CHAINS_OUT_DIR,
    'brand_names_of_chains.csv'
)

SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE = os.path.join(
    SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_DIR,
    'trans_in_file.csv'
)

SPECIAL_INPUT_TRANS_FILE_INTO_DB_IN_FILE_NECESSARY_FIELDS_SET = {
    'Line Code', 'Class', 'TAW PN', 'VEND PN', 'Description', 'Your Price', 'Jobber',
    'MAP-CONFIRM W JOBBER', 'Core Price', 'Federal Excise Tax', 'Status Code', 'Oversize'
}

# =================================================================

SupplierBrandKey = namedtuple('SupplierBrandKey', ['name', 'supplier_id'])
BrandCheck = namedtuple('BrandKey', ['name', 'check'])
NOT_CHECKED_BRAND_P = 'NOT_CHECKED: {}'
BRAND_CHAIN_P = re.compile(r'^(?P<supplier_brand_name>.+) \((?P<supplier_id>\d+)\)$')


def split_chain(chain: str) -> FrozenSet[SupplierBrandKey]:
    """
    Выделяет из строк вида
    303 PRODUCTS (1) | 303 Products, Inc. (2) | OCTANE BOOST (1)
    все supplier_brand_keys
    и возвращает frozenset из них
    """
    chain_set = set()
    for el in chain.split(' | '):
        m = BRAND_CHAIN_P.search(el)
        if m:
            supplier_brand_key = SupplierBrandKey(
                name=m.group('supplier_brand_name'),
                supplier_id=int(m.group('supplier_id'))
            )
            chain_set.add(supplier_brand_key)
        else:
            raise Exception('Error while parsing: {!r}'.format(chain))
    return frozenset(chain_set)
