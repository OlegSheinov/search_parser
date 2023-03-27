# -*- coding: utf-8 -*-
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
        return {
                   "http": f"socks5h://{log_and_pass[0]}:{log_and_pass[1]}@{proxy}",
                   "https": f"socks5h://{log_and_pass[0]}:{log_and_pass[1]}@{proxy}"
               }

    async def parsing(self) -> None:
        with requests.Session() as session:
            for page in range(1, 21):
                url = f"https://www.google.com/search?q={self.query}{f'&start={10 * (page - 1)}' if page > 1 else ''}"
                try:
                    headers = Headers(
                        headers=True
                    ).generate()
                    proxy = await self.get_proxy()
                    session.proxies = proxy
                    session.headers = headers
                    with session.get(url) as response:
                        if response.status_code != 200:
                            raise ConnectionError(response.reason)
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
    async with aiofiles.open("query.csv", mode="r", encoding="cp1251") as file:
        async for row in AsyncReader(file):
            try:
                parser = Parser(row[0], attr)
                await parser.parsing()
            except BaseException as err:
                print(f"Произошла ошибка в запросе - {row}\n{err}")


if __name__ == "__main__":
    with open('tag.txt', 'r') as f:
        tag = f.readlines()
    asyncio.run(start(tag[0]))
