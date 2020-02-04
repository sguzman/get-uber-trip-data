import atexit
import json
from datetime import datetime

import psycopg2
import pytz
import requests
from typing import IO, Dict, List, Optional, Tuple

statement_url: str = 'https://drivers.uber.com/p3/payments/api/fetchWeeklyEarnings'


class Trip:
    def __init__(self, uuid: str, total: float, timezone: str,
                 request_at: int, dropoff_at: int, surge: bool,
                 distance: float, duration: float,
                 status: str, vehicle_type: str,
                 pickup_address: str, dropoff_address: str,
                 custom_route_map: str):
        self.uuid: str = uuid
        self.total: float = float(total)

        tz = pytz.timezone(timezone)
        self.request_at: datetime = datetime.fromtimestamp(request_at, tz)
        self.dropoff_at: Optional[datetime] = Trip.datetime_or_none(dropoff_at, tz)
        self.surge: bool = surge
        self.distance: float = distance
        self.duration: float = duration
        self.pickup_lat: Optional[float] = None
        self.pickup_lon: Optional[float] = None
        self.dropoff_lat: Optional[float] = None
        self.dropoff_lon: Optional[float] = None
        self.status: str = status
        self.vehicle_type: str = vehicle_type
        self.pickup_address: str = pickup_address
        self.dropoff_address: str = dropoff_address
        self.custom_route_map: str = custom_route_map

        self.set_lat_lon()

    @staticmethod
    def datetime_or_none(n: Optional[int], tz) -> Optional[datetime]:
        if n is None:
            return None

        return datetime.fromtimestamp(n, tz)

    @staticmethod
    def get_pickup_lat(url: str) -> Optional[float]:
        try:
            params_idx: int = url.index('?')
            params: str = url[params_idx + 1:]
            split = params.split('&')

            pickup = split[1].split('7C')
            pickup = pickup[-1].split('%2C')

            return float(pickup[0])
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def get_pickup_lon(url: str) -> Optional[float]:
        try:
            params_idx: int = url.index('?')
            params: str = url[params_idx + 1:]
            split = params.split('&')

            pickup = split[1].split('7C')
            pickup = pickup[-1].split('%2C')

            return float(pickup[1])
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def get_dropoff_lat(url: str) -> Optional[float]:
        try:
            params_idx: int = url.index('?')
            params: str = url[params_idx + 1:]
            split = params.split('&')

            dropoff = split[2].split('7C')
            dropoff = dropoff[-1].split('%2C')

            return float(dropoff[0])
        except Exception as e:
            print(e)
            return None

    @staticmethod
    def get_dropoff_lon(url: str) -> Optional[float]:
        try:
            params_idx: int = url.index('?')
            params: str = url[params_idx + 1:]
            split = params.split('&')

            dropoff = split[2].split('7C')
            dropoff = dropoff[-1].split('%2C')

            return float(dropoff[1])
        except Exception as e:
            print(e)
            return None

    def set_lat_lon(self):
        self.pickup_lat = self.get_pickup_lat(self.custom_route_map)
        self.pickup_lon = self.get_pickup_lon(self.custom_route_map)
        self.dropoff_lat = self.get_dropoff_lat(self.custom_route_map)
        self.dropoff_lon = self.get_dropoff_lon(self.custom_route_map)

    def data(self):
        return [
            self.uuid,
            self.total,
            self.request_at,
            self.dropoff_at,
            self.surge,
            self.distance,
            self.duration,
            self.pickup_lat,
            self.pickup_lon,
            self.dropoff_lat,
            self.dropoff_lon,
            self.status,
            self.vehicle_type,
            self.pickup_address,
            self.dropoff_address,
            self.custom_route_map
        ]


def con() -> psycopg2:
    conn: psycopg2 = \
        psycopg2.connect(user='admin', password='', host='127.0.0.1', port='5432', database='projects')

    def clean_up() -> None:
        conn.close()
        print('Closing connection', conn)

    atexit.register(clean_up)
    return conn


conn: psycopg2 = con()


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


def get_trip_data(offset: int) -> List[Trip]:
    try:
        data = {
            "weekOffset": offset
        }
        resp = requests.post(statement_url, json=data, headers=get_headers())
        json_str: str = resp.text
        json_obj: json = json.loads(json_str)

        data: Dict = json_obj['data']['earnings']['trips']
        trips: List[Trip] = []

        for d in data.keys():
            trip = data[d]
            obj: Trip = Trip(
                trip['uuid'],
                trip['total'],
                trip['timezone'],
                trip['requestAt'],
                trip['dropoffAt'],
                trip['isSurge'],
                trip['distance'],
                trip['duration'],
                trip['status'],
                trip['vehicleType'],
                trip['pickupAddress'],
                trip['dropoffAddress'],
                trip['customRouteMap']
            )
            trips.append(obj)

        return trips
    except Exception as e:
        print(e)
        return []


def insert_trip_sql(trips: List[Trip]) -> None:
    if len(trips) == 0:
        return

    insert_sql: str = 'INSERT INTO projects.public.trips (uuid, total, request_at, dropoff_at, surge, distance, duration, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, status, vehicle_type, pickup_address, dropoff_address, custom_route_map) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s , %s , %s , %s , %s ) ON CONFLICT DO NOTHING '

    cursor = conn.cursor()
    for t in trips:
        data: List = t.data()
        print('Inserting', data)
        cursor.execute(insert_sql, data)

    conn.commit()
    cursor.close()


def main() -> None:
    for i in range(200):
        trips: List[Trip] = get_trip_data(i)
        if len(trips) > 0:
            print(i, len(trips), trips[0].data())
        else:
            print(i, len(trips))

        insert_trip_sql(trips)


if __name__ == '__main__':
    main()
