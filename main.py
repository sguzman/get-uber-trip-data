import atexit
import json
from datetime import datetime

import psycopg2
import requests
import queue
import threading
from typing import IO, Dict, List, Optional

statement_url: str = 'https://partners.uber.com/p3/payments/api/fetchPayStatementsPaginated'

stmt_queue = queue.Queue()
trip_queue = queue.Queue()
trip_data_queue = queue.Queue()


def con() -> psycopg2:
    conn: psycopg2 = \
        psycopg2.connect(user='admin', password='', host='127.0.0.1', port='5432', database='misc')

    def clean_up() -> None:
        conn.close()
        print('Closing connection', conn)

    atexit.register(clean_up)
    return conn


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
            json_resp = json.loads(resp)
            trip_data_queue.put_nowait(json_resp)
        except json.decoder.JSONDecodeError:
            print('Bad JSON decode')
            continue
        except queue.Empty:
            print('Finished trip queue')
            return


def get_index_price(t, n: int) -> Optional[float]:
    try:
        float(t['breakdown'][0]['items'][n]['amount'])
    except:
        return None


def insert_trip_sql() -> None:
    conn = con()
    insert_sql: str = 'INSERT INTO misc.public.trips (uuid,vehicle_type, total, request_at, is_surge, distance, duration, pickup_address, dropoff_address, status, total_toll, custom_route_map, chain_uuid, driver_fare, dropoff_at, distance_price, duration_price, surge_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) On CONFLICT DO NOTHING '

    while True:
        try:
            trip_obj = trip_data_queue.get()
            t = trip_obj['data']['tripDetails']
            total_toll: Optional[float] = None if t['totalToll'] is None else float(t['totalToll'])
            dropoff_at: Optional[datetime] = None if t['dropoffAt'] is None else datetime.fromtimestamp(t['dropoffAt'])

            data = [
                t['uuid'], t['vehicleType'], float(t['total']), datetime.fromtimestamp(t['requestAt']),
                t['isSurge'], float(t['distance']), int(t['duration']), t['pickupAddress'], t['dropoffAddress'],
                t['status'], total_toll, t['customRouteMap'], t['chainUuid'], float(t['driverFare']),dropoff_at,
                get_index_price(t, 0), get_index_price(t, 1), get_index_price(t, 2)
            ]
            print(data)
            cursor = conn.cursor()
            cursor.execute(insert_sql, data)

            conn.commit()
            cursor.close()
        except queue.Empty:
            print('Done inserting trip data')
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

    insert_trip_sql()


if __name__ == '__main__':
    main()
