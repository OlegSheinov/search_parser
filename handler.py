from typing import List

import aiofiles
import asyncio
import csv
from bs4 import BeautifulSoup
from aiohttp import ClientSession, ClientTimeout
from urllib.parse import urlparse, urljoin

from fake_headers import Headers


class Handler:
    def __init__(self, data: List[str], tag: str):
        self.start_urls = data
        self.tag = tag
        self.visited_urls = set()
        self.visited_domains = set()
        self.headers = Headers(headers=True).generate()
        self.fieldnames = ["current_page", "sub_page"]
        self.timeout = ClientTimeout(total=10)

    async def main(self):
        tasks = []
        for link in self.start_urls:
            task = asyncio.create_task(self.search_recursive(link, depth=0))
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def write_to_csv(self, data_list):
        filename = "output.csv"
        async with aiofiles.open(filename, mode="a", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames, delimiter=";")
            if not csvfile.tell():
                writer.writeheader()
            await asyncio.gather(*[writer.writerow(data) for data in data_list])

    async def search_recursive(self, link, depth):
        parsed_link = urlparse(link)
        await asyncio.sleep(1)
        if f"{parsed_link.scheme}://{parsed_link.hostname}" in self.visited_domains:
            return
        if link in self.visited_urls:
            return
        if depth > 2:
            return
        self.visited_urls.add(link)
        async with ClientSession(headers=self.headers) as session:
            async with session.get(link) as response:
                if response.status != 200:
                    return
                try:
                    soup = BeautifulSoup(await response.text(), "lxml")
                except UnicodeDecodeError as err:
                    print(f"С сайта {link} пришла неверная кодировка - {err}")
                    return
        if soup.find(self.tag):
            if f"{parsed_link.scheme}://{parsed_link.hostname}" in self.visited_domains:
                return
            parsed_url = urlparse(link)
            sub_url = f"{parsed_url.scheme}://{parsed_url.hostname}{parsed_url.path}"
            print(f'Найдено на {sub_url}')
            self.visited_domains.add(f"{parsed_url.scheme}://{parsed_url.hostname}")
            await asyncio.sleep(1)
            return await self.write_to_csv(
                [{self.fieldnames[0]: f"{parsed_url.scheme}://{parsed_url.hostname}", self.fieldnames[1]: sub_url}])
        parsed_url = urlparse(link)
        base_url = f"{parsed_url.scheme}://{parsed_url.hostname}"
        tasks = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('/'):
                href = urljoin(base_url, href)
            if not href.startswith(base_url):
                continue
            task = asyncio.create_task(self.search_recursive(href, depth=depth + 1))
            tasks.append(task)
        await asyncio.sleep(1)
        return await asyncio.gather(*tasks)
