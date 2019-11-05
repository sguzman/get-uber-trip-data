import requests
import json
from typing import IO, Dict

url: str = 'https://partners.uber.com/p3/payments/api/fetchWeeklyEarning'


def get_cookie() -> str:
    json_file: IO = open('./cookie.json', 'r')
    json_str: str = json_file.read()
    json_obj: json = json.loads(json_str)

    return json_obj["cookie"]


def get_headers() -> Dict[str, str]:
    return {
        'x-csrf-token': 'x',
        'cookie': get_cookie()
    }


def get_week(n: int) -> str:
    resp = requests.post(url, json={'weekOffset': str(n)}, headers=get_headers())
    json_str: str = resp.content
    return json_str


def main() -> None:
    print('Hello world')
    print(get_cookie())
    print(get_week(1))


if __name__ == '__main__':
    main()
