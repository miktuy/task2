import csv
import hashlib
import sys
from dataclasses import dataclass
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import List

BUF_SIZE = 65536
EXPECTED_HASH_TYPE = ('md5', 'sha1', 'sha256')
HashLib = 'HashLib'


@dataclass
class Item:
    file_name: str
    hash_type: str
    hash_code: str


def read_input_file(file_path: Path):
    with open(file_path) as f:
        reader = csv.reader(f, delimiter=' ', skipinitialspace=True)
        items = []
        for row in reader:
            if not row:
                continue
            if len(row) != 3 or not all(row):
                raise ValueError(f'Invalid data line in input file: {" ".join(row)}!')
            items.append(
                Item(*row)
            )
        return items


def get_hash_lib(hash_type: str) -> HashLib:
    if hash_type not in EXPECTED_HASH_TYPE:
        raise ValueError(f'Unexpected hash type {hash_type}!')
    return getattr(hashlib, hash_type)


def get_file_hash(file_path: Path, hash_lib: HashLib) -> str:
    lib = hash_lib()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            lib.update(data)
    return lib.hexdigest()


def process_item(item: Item, source_folder: str):
    path = Path(source_folder, item.file_name)
    if not path.exists():
        print(f'{path} NOT FOUND')
        return
    hash_lib = get_hash_lib(item.hash_type)
    hash_code = get_file_hash(path, hash_lib)
    if hash_code != item.hash_code:
        print(f'{path} FAIL')
    else:
        print(f'{path} OK')


if __name__ == '__main__':
    try:
        input_file, source_folder = sys.argv[1:]
    except ValueError as e:
        print(f'Unexpected count of input parameters: `{str(e).split("(")[1].replace(")", "")}`')
        exit(1)

    items: List[Item] = read_input_file(Path(input_file))

    with Pool(2) as p:
        p.map(partial(process_item, source_folder=source_folder), items)

