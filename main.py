import csv
import requests
import json
from typing import IO, Dict, List

url: str = 'https://partners.uber.com/p3/payments/api/fetchPayStatementsPaginated'


def get_cookie() -> str:
    json_file: IO = open('./cookie.json', 'r')
    json_str: str = json_file.read()
    json_obj: json = json.loads(json_str)

    return json_obj["cookie"]


def get_headers() -> Dict[str, str]:
    return {
        'x-csrf-token': 'x',
        'content-type': 'application/json',
        'cookie': get_cookie()
    }


def get_statement_page(offset: int) -> json:
    data = {
        "pageIndex": offset,
        "pageSize": 100,
        "pagination": {
            "hasMoreData": True,
            "nextCursor": "1539716406307",
            "pageNumber": 3,
            "totalPages": 4,
            "cursors": ["1571466758406", "1541255383715", "1539716406307"]}
    }
    resp = requests.post(url, data=json.dumps(data), headers=get_headers())
    json_str: str = resp.text
    json_obj: json = json.loads(json_str)

    return json_obj


def get_statement_uuids(offset: int) -> List[str]:
    page: json = get_statement_page(offset)
    stmts = page['data']['payStatementsPaginatedEA']['statements']

    uuids: List[str] = []
    for stm in stmts:
        uuids.append(stm['uuid'])

    return uuids


def get_all_statement_uuid() -> List[str]:
    uuids: List[str] = []
    i = 1
    while True:
        stmts: List[str] = get_statement_uuids(i)
        print(i, len(stmts), stmts)

        if len(stmts) == 0:
            break
        uuids.extend(stmts)
        i += 1

    return uuids


def main() -> None:
    print(get_all_statement_uuid())


if __name__ == '__main__':
    main()
