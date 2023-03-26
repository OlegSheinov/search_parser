import asyncio
import sys

from urllib.parse import urlparse

import aiofiles
import uvloop
from aiocsv import AsyncReader
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from fake_headers import Headers

from handler import Handler


class Parser:

    def __init__(self, query):
        self.query = query
        self.all_links_from_google = []

    async def parsing(self):
        async with ClientSession() as session:
            for page in range(1, 3):
                # url = f"https://customsearch.googleapis.com/customsearch/v1?cx={os.getenv('CX_KEY')}&lr=lang_en" \
                #       f"&q={quote(query)}{f'&start={10 * (page - 1)}' if page > 1 else ''}&num200&key={os.getenv('API_KEY')}"
                url = f"https://www.google.com/search?q={self.query}{f'&start={10 * (page - 1)}' if page > 1 else ''}"
                # url = f"https://cse.google.com/cse/element/v1?num=200&hl=ru&cx={os.getenv('CX_KEY')}&q={query}" \
                #       f"&cse_tok=AFW0emxq9B2hAHXgkISR4NDoJ_UI:1679764553222&callback=google.search.cse.api18571"
                # task = asyncio.ensure_future(parse_url(url))
                # tasks.append(task)
                headers = Headers(
                    headers=True
                ).generate()
                try:
                    async with session.get(url, headers=headers) as response:
                        data = await response.read()
                    soup = BeautifulSoup(data, "lxml")
                    all_links = [item.find("a").get("href") for item in
                                 soup.find_all("div", class_="yuRUbf")]
                    self.all_links_from_google.extend(
                        [urlparse(item).scheme + f"://{urlparse(item).netloc}" for item in all_links])
                except BaseException as err:
                    print(err)
                    await session.close()
        self.all_links_from_google = set(self.all_links_from_google)
        await self.parse_url()

    async def parse_url(self):
        try:
            handler = Handler(list(self.all_links_from_google), "figure")
            print(await handler.main())
        except KeyError as err:
            print(err)


async def start():
    async with aiofiles.open("query.csv", mode="r", encoding="utf-8") as file:
        async for row in AsyncReader(file):
            await Parser(row[0]).parsing()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(start())
