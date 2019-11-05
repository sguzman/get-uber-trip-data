import requests
import json
from typing import IO


def get_cookie() -> str:
    json_file: IO = open('./cookie.json', 'r')
    json_str: str = json_file.read()
    json_obj: json = json.loads(json_str)

    return json_obj["cookie"]


def main() -> None:
    print('Hello world')
    print(get_cookie())


if __name__ == '__main__':
    main()
