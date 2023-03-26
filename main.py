import argparse
import asyncio
import random
from urllib.parse import urlparse

import aiofiles
import uvloop
from aiocsv import AsyncReader
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from fake_headers import Headers

from handler import Handler


class Parser:

    def __init__(self, query: str, attr: str):
        self.tag = attr
        self.query = query
        self.all_links_from_google = []

    async def parsing(self) -> None:
        async with ClientSession() as session:
            for page in range(1, 21):
                url = f"https://www.google.com/search?q={self.query}{f'&start={10 * (page - 1)}' if page > 1 else ''}"
                try:
                    # TODO раскомментировать этот код, если нужно использование прокси
                    # proxy = "https://196.18.224.210:8000"
                    # proxy_auth = BasicAuth("S5Z5dU", "Xr0qPk")
                    # async with session.get(url, headers=headers, proxy=proxy, proxy_auth=proxy_auth) as response:
                    headers = Headers(
                        headers=True
                    ).generate()
                    async with session.get(url, headers=headers) as response:
                        if response.status != 200:
                            raise ConnectionError(
                                "Слишком много запросов. Повторите попытку через 10 минут или смените IP-адрес")
                        data = await response.read()
                    soup = BeautifulSoup(data, "lxml")
                    all_links = [item.find("a").get("href") for item in
                                 soup.find_all("div", class_="yuRUbf")]
                    self.all_links_from_google.extend(
                        [urlparse(item).scheme + f"://{urlparse(item).netloc}" for item in all_links])
                    await asyncio.sleep(random.choice([0.1, 0.5]))
                except TimeoutError as err:
                    print(err)
                    await session.close()
        self.all_links_from_google = set(self.all_links_from_google)
        await self.parse_url()

    async def parse_url(self) -> None:
        handler = Handler(list(self.all_links_from_google), self.tag, self.query)
        await handler.main()


async def start(attr) -> None:
    async with aiofiles.open("query.csv", mode="r", encoding="utf-8") as file:
        async for row in AsyncReader(file):
            await Parser(row[0], attr).parsing()


parser = argparse.ArgumentParser(description='Парсер поисковой выдачи google')
parser.add_argument("-t", '--tag', type=str, help='TAG, который необходимо искать на страницах')
args = parser.parse_args()

if __name__ == "__main__":
    tag = args.tag
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(start(tag))
