from __future__ import annotations

import calendar
import datetime
import mimetypes
import re
from typing import TYPE_CHECKING, Optional

from aiolimiter import AsyncLimiter
from yarl import URL

from cyberdrop_dl.clients.errors import NoExtensionFailure, ScrapeFailure
from cyberdrop_dl.scraper.crawler import Crawler
from cyberdrop_dl.utils.dataclasses.url_objects import ScrapeItem
from cyberdrop_dl.utils.utilities import error_handling_wrapper, get_filename_and_ext

if TYPE_CHECKING:
    from bs4 import BeautifulSoup

    from cyberdrop_dl.managers.manager import Manager


class FilesterCrawler(Crawler):
    def __init__(self, manager: Manager):
        super().__init__(manager, "filester", "Filester")
        self.primary_base_domain = URL("https://filester.me")
        self.download_api = self.primary_base_domain / "api" / "public" / "download"
        self.cdn_base = URL("https://cache1.filester.me")
        self.request_headers = {
            "Content-Type": "application/json",
            "Origin": "https://filester.me",
            "Referer": "https://filester.me/",
        }
        self.request_limiter = AsyncLimiter(10, 1)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        """Determines where to send the scrape item based on the url"""
        task_id = await self.scraping_progress.add_task(scrape_item.url)

        if "f" in scrape_item.url.parts:
            await self.folder(scrape_item)
        else:
            await self.file(scrape_item)

        await self.scraping_progress.remove_task(task_id)

    @error_handling_wrapper
    async def folder(self, scrape_item: ScrapeItem) -> None:
        """Scrapes a public Filester folder"""
        folder_id = scrape_item.url.parts[-1]
        page_url = scrape_item.url
        title: Optional[str] = None
        visited_pages = set()

        while page_url and str(page_url) not in visited_pages:
            visited_pages.add(str(page_url))

            async with self.request_limiter:
                soup = await self.client.get_BS4(self.domain, page_url)

            if title is None:
                title = await self.create_title(self._get_folder_title(soup), folder_id, None)

            for link in self._get_folder_file_links(soup, page_url):
                new_scrape_item = await self.create_scrape_item(scrape_item, link, title, True, folder_id)
                self.manager.task_group.create_task(self.run(new_scrape_item))

            page_url = self._get_next_page_url(soup, page_url)

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem) -> None:
        """Scrapes a single Filester file"""
        if await self.check_complete_from_referer(scrape_item):
            return

        async with self.request_limiter:
            soup = await self.client.get_BS4(self.domain, scrape_item.url)

        filename = self._get_filename(soup, scrape_item.url.name)
        try:
            filename, ext = await get_filename_and_ext(filename)
        except NoExtensionFailure:
            ext = self._get_extension_from_mime_type(soup)
            if not ext:
                raise NoExtensionFailure()
            filename, ext = await get_filename_and_ext(f"{filename}{ext}")

        uploaded = self._get_file_detail(soup, "Uploaded")
        if uploaded:
            scrape_item.possible_datetime = await self.parse_datetime(uploaded)

        link = await self.create_download_link(scrape_item.url.parts[-1])
        await self.handle_file(link, scrape_item, filename, ext)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def create_download_link(self, file_slug: str) -> URL:
        """Creates a temporary CDN download link for a Filester file"""
        async with self.request_limiter:
            json_resp = await self.client.post_json(
                self.domain,
                self.download_api,
                data={"file_slug": file_slug},
                headers_inc=self.request_headers,
            )

        if not json_resp.get("success") or not json_resp.get("download_url"):
            raise ScrapeFailure(403, "Couldn't generate Filester download link")

        return (self.cdn_base / json_resp["download_url"].lstrip("/")).with_query({"download": "true"})

    async def parse_datetime(self, date: str) -> int:
        """Parses a datetime string into a unix timestamp"""
        date = date.replace("T", " ").rstrip("Z").split(".")[0]
        parsed_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        return calendar.timegm(parsed_date.timetuple())

    def _absolute_url(self, href: str, base_url: URL) -> URL:
        if href.startswith("http"):
            return URL(href)
        if href.startswith("?"):
            return base_url.with_query(href[1:])
        if href.startswith("/"):
            relative_url = URL(href)
            return self.primary_base_domain.with_path(relative_url.path).with_query(relative_url.query)
        return self.primary_base_domain / href

    def _get_filename(self, soup: BeautifulSoup, fallback: str) -> str:
        meta_title = soup.select_one('meta[property="og:title"]')
        if meta_title and meta_title.get("content"):
            return meta_title.get("content").strip()

        file_title = soup.select_one("main h1")
        if file_title and file_title.get_text(strip=True):
            return file_title.get_text(strip=True)

        script_text = "\n".join(script.string or "" for script in soup.select("script"))
        match = re.search(r'window\.fileName\s*=\s*"([^"]+)"', script_text)
        if match:
            return match.group(1)

        return fallback

    def _get_folder_title(self, soup: BeautifulSoup) -> str:
        page_title = soup.select_one("title")
        if page_title and page_title.get_text(strip=True):
            return page_title.get_text(strip=True).split("|")[0].strip()

        headings = [heading.get_text(strip=True) for heading in soup.select("h1")]
        if len(headings) > 1:
            return headings[1]
        return headings[0] if headings else "Filester Folder"

    def _get_folder_file_links(self, soup: BeautifulSoup, page_url: URL) -> list[URL]:
        links = []
        for item in soup.select(".file-item[onclick]"):
            onclick = item.get("onclick", "")
            match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if match:
                links.append(self._absolute_url(match.group(1), page_url))

        for link in soup.select('a[href^="/d/"], a[href*="filester.me/d/"]'):
            links.append(self._absolute_url(link.get("href"), page_url))

        return list(dict.fromkeys(links))

    def _get_next_page_url(self, soup: BeautifulSoup, page_url: URL) -> Optional[URL]:
        for link in soup.select("a[href]"):
            if link.get_text(strip=True) == "→":
                return self._absolute_url(link.get("href"), page_url)
        return None

    def _get_file_detail(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        for container in soup.select("#detailsContent .flex.flex-col"):
            spans = container.select("span")
            if len(spans) < 2:
                continue
            if spans[0].get_text(strip=True).lower() == label.lower():
                return spans[-1].get_text(strip=True)
        return None

    def _get_extension_from_mime_type(self, soup: BeautifulSoup) -> Optional[str]:
        mime_type = self._get_file_detail(soup, "Type")
        if not mime_type:
            script_text = "\n".join(script.string or "" for script in soup.select("script"))
            match = re.search(r'window\.fileType\s*=\s*"([^"]+)"', script_text)
            mime_type = match.group(1).replace("\\/", "/") if match else None
        return mimetypes.guess_extension(mime_type) if mime_type else None
