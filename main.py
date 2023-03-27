# -*- coding: utf-8 -*-

import argparse
import asyncio
import random
import re
from urllib.parse import urlparse

import aiofiles
import requests
from aiocsv import AsyncReader
from bs4 import BeautifulSoup
from fake_headers import Headers

from handler import Handler


class Parser:

    def __init__(self, query: str, attr: str):
        self.tag = attr
        self.query = query
        self.all_links_from_google = []

    @staticmethod
    async def get_proxy():
        async with aiofiles.open('proxies.txt', 'r') as f:
            proxies = await f.readlines()
        proxy_str = str(random.choice(proxies)).replace("\n", "")
        log_and_pass = re.search(r"([a-zA-Z0-9]+:[a-zA-Z0-9]+)$", proxy_str).group(0).split(":")
        proxy = ":".join(proxy_str.split(":")[:2])
        return {"http": f"https://{proxy}"}, log_and_pass[0], log_and_pass[1]

    async def parsing(self) -> None:
        for page in range(1, 21):
            url = f"https://www.google.com/search?q={self.query}{f'&start={10 * (page - 1)}' if page > 1 else ''}"
            try:
                headers = Headers(
                    headers=True
                ).generate()
                with requests.Session() as session:
                    proxy, login, password = await self.get_proxy()
                    proxy_auth = (login, password)
                    with session.get(url, headers=headers, proxies=proxy, auth=proxy_auth) as response:
                        # response = requests.get(url, headers=headers)
                        if response.status_code != 200:
                            raise ConnectionError(
                                "Слишком много запросов. Повторите попытку через 10 минут или смените IP-адрес")
                        data = response.content
                soup = BeautifulSoup(data, "lxml")
                all_links = [item.find("a").get("href") for item in
                             soup.find_all("div", class_="yuRUbf")]
                self.all_links_from_google.extend(
                    [urlparse(item).scheme + f"://{urlparse(item).netloc}" for item in all_links])
                await asyncio.sleep(random.choice([0.1, 0.5]))
            except TimeoutError as err:
                print(err)
        self.all_links_from_google = set(self.all_links_from_google)
        await self.parse_url()

    async def parse_url(self) -> None:
        handler = Handler(list(self.all_links_from_google), self.tag, self.query)
        await handler.main()


async def start(attr) -> None:
    async with aiofiles.open("query.csv", mode="r", encoding="utf-8") as file:
        async for row in AsyncReader(file):
            parser = Parser(row[0], attr)
            await parser.parsing()


parser = argparse.ArgumentParser(description='Парсер поисковой выдачи google')
parser.add_argument("-t", '--tag', type=str, help='TAG, который необходимо искать на страницах')
args = parser.parse_args()

if __name__ == "__main__":
    tag = args.tag
    asyncio.run(start(tag))
