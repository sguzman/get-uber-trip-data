import requests
import json
from typing import IO, Dict, List
import queue
import threading

statement_url: str = 'https://partners.uber.com/p3/payments/api/fetchPayStatementsPaginated'

stmt_queue = queue.Queue()
trip_queue = queue.Queue()


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
    resp = requests.post(statement_url, json=data, headers=get_headers())
    json_str: str = resp.text
    json_obj: json = json.loads(json_str)

    return json_obj


def get_statement_uuids() -> None:
    offset = 1
    while True:
        page: json = get_statement_page(offset)
        stmts = page['data']['payStatementsPaginatedEA']['statements']
        if len(stmts) == 0:
            break

        for stm in stmts:
            uuid: str = stm['uuid']
            print('statement', uuid)
            stmt_queue.put_nowait(uuid)

        offset += 1

    print('Finished retrieving all statements')


def get_statement_csv(uuid: str) -> str:
    url: str = f'https://partners.uber.com/p3/payments/statements/{uuid}/csv'

    return requests.get(url, headers=get_headers(), params={'disable_attachment': '1/print'}).text


def get_trip_uuids_from_statement():
    while True:
        try:
            stmt = stmt_queue.get(block=True)
            csv = get_statement_csv(stmt)
            lines: List[str] = csv.split('\n')
            lines: List[str] = lines[1:]
            for ln in lines:
                split: List[str] = ln.split(',')
                if len(split) > 1:
                    trip: str = split[6].replace('"', '')
                    print('trip', trip)
                    trip_queue.put(trip)
        except queue.Empty:
            print('Finished trip lookup')
            return


def get_trip_details() -> None:
    url: str = 'https://partners.uber.com/p3/payments/api/fetchTripDetails'
    while True:
        try:
            uuid: str = trip_queue.get(block=True)
            print('Calling', uuid, 'trip data')
            data = {"tripUUID": uuid}
            resp = requests.post(url, json=data, headers=get_headers()).text
            print(resp)
        except queue.Empty:
            print('Finished trip queue')
            return


def main() -> None:
    threading.Thread(target=get_statement_uuids).start()

    threading.Thread(target=get_trip_uuids_from_statement).start()
    threading.Thread(target=get_trip_uuids_from_statement).start()
    threading.Thread(target=get_trip_uuids_from_statement).start()
    threading.Thread(target=get_trip_uuids_from_statement).start()

    threading.Thread(target=get_trip_details).start()
    threading.Thread(target=get_trip_details).start()
    threading.Thread(target=get_trip_details).start()
    threading.Thread(target=get_trip_details).start()


if __name__ == '__main__':
    main()
