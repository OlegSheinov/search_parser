import asyncio
import concurrent
import csv
from concurrent.futures import ThreadPoolExecutor
from typing import List
from urllib.parse import urlparse, urljoin

import aiofiles
from aiocsv import AsyncDictWriter
from aiohttp import ClientSession, ClientTimeout, TCPConnector, ClientError
from bs4 import BeautifulSoup
from fake_headers import Headers


class Handler:
    def __init__(self, data: List[str], tag: str, query: str):
        self.query = query
        self.start_urls = data
        self.tag = tag
        self.visited_urls = set()
        self.visited_domains = set()
        self.headers = Headers(headers=True).generate()
        self.fieldnames = ["current_page", "sub_page", "query"]
        self.timeout = ClientTimeout(total=10)

    async def main(self):
        async with ClientSession(headers=self.headers, connector=TCPConnector(limit=10),
                                 timeout=self.timeout) as session:
            tasks = [asyncio.create_task(self.search_recursive(session, link, depth=0)) for link in self.start_urls]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def write_to_csv(self, data_list):
        filename = f"output_{self.tag}.csv"
        async with aiofiles.open(filename, mode="a", encoding="utf-8", newline="") as csvfile:
            writer = AsyncDictWriter(csvfile, fieldnames=self.fieldnames, delimiter=";")
            if not csvfile.tell():
                await writer.writeheader()
            await asyncio.gather(*[await writer.writerow(data) for data in data_list])

    async def search_recursive(self, session: ClientSession, link, depth):
        parsed_link = urlparse(link)
        if link in self.visited_urls or depth > 2:
            return
        self.visited_urls.add(link)
        try:
            async with session.get(link) as response:
                if response.status != 200:
                    return
                soup = BeautifulSoup(await response.text(), "lxml")
        except (ClientError, TimeoutError, UnicodeDecodeError):
            return
        if soup.find(self.tag):
            if f"{parsed_link.scheme}://{parsed_link.hostname}" in self.visited_domains:
                return
            parsed_url = urlparse(link)
            sub_url = f"{parsed_url.scheme}://{parsed_url.hostname}{parsed_url.path}"
            self.visited_domains.add(f"{parsed_url.scheme}://{parsed_url.hostname}")
            print(f'Найдено на {sub_url}')
            return await self.write_to_csv(
                [{self.fieldnames[0]: f"{parsed_url.scheme}://{parsed_url.hostname}", self.fieldnames[1]: sub_url,
                  self.fieldnames[2]: self.query}])
        parsed_url = urlparse(link)
        base_url = f"{parsed_url.scheme}://{parsed_url.hostname}"
        tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=20):
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.startswith('/'):
                    href = urljoin(base_url, href)
                if not href.startswith(base_url):
                    continue
                task = asyncio.create_task(self.search_recursive(session, href, depth=depth + 1))
                tasks.append(task)
            await asyncio.gather(*tasks)
