# noinspection SpellCheckingInspection
"""
Создаёт из файла constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_FILE файл
constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_FILE.

Файл constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_FILE нужно заполнять вручную и он содержит соответствия префиксов
поставщика Trans названиям брендов, т.к. у Trans в файле полученном по ftp нет названий бреднов, а есть только префиксы
у itemов.

Файл constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_FILE содержит словарь trans_code, где ключами являются префиксы,
а значениями - соответствующие им названия брендов. Данный файл необходим для работы программы parse_suppliers_files.py
(импортируется в модуль items.py).
Таким образом полученный после работы данной программы файл trans_prefix_brand.py нужно скопировать в папку
с программой items.py.
"""

import csv
import os
import pprint

import constants

trans_code = {}
with open(
        file=constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_FILE,
        newline='',
        encoding='utf8'
) as f_in:
    dialect = csv.Sniffer().sniff(f_in.read(4096))
    f_in.seek(0)
    csv_reader = csv.DictReader(f_in, dialect=dialect)
    assert constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_IN_FILE_NECESSARY_FIELDS_SET.issubset(set(csv_reader.fieldnames))
    for row in csv_reader:
        trans_code[row['prefix']] = row['brand']

os.makedirs(constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_DIR, exist_ok=True)
with open(
        file=constants.SPECIAL_MAKE_TRANS_PREFIXES_DICT_OUT_FILE,
        mode='w',
        encoding='utf8'
) as f_out:
    f_out.write('trans_code = ')
    pprint.pprint(trans_code, stream=f_out, indent=4, compact=True)
