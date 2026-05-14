from __future__ import annotations

from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter
from yarl import URL

from cyberdrop_dl.clients.errors import ScrapeFailure
from cyberdrop_dl.scraper.crawler import Crawler
from cyberdrop_dl.utils.dataclasses.url_objects import ScrapeItem
from cyberdrop_dl.utils.utilities import error_handling_wrapper, get_filename_and_ext

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager


DOWNLOAD_SELECTORS = (
    'a.btn[href*="md5="]',
    'a.btn-main[download][href]',
    'a[download][href*="md5="]',
    'video source[src*="md5="]',
    'img[src*="md5="]',
)
HOMEPAGE_CATCHALL_FILE = "/s21/FHVZKQyAZlIsrneDAsp.jpeg"


class FileditchCrawler(Crawler):
    def __init__(self, manager: Manager):
        super().__init__(manager, "fileditch", "Fileditch")
        self.primary_base_domain = URL("https://fileditchfiles.me")
        self.request_limiter = AsyncLimiter(10, 1)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        """Determines where to send the scrape item based on the url"""
        task_id = await self.scraping_progress.add_task(scrape_item.url)

        if scrape_item.url.path == "/file.php":
            await self.file(scrape_item)
        else:
            await self.direct_file(scrape_item)

        await self.scraping_progress.remove_task(task_id)

    @error_handling_wrapper
    async def file(self, scrape_item: ScrapeItem) -> None:
        """Scrapes a Fileditch viewer page"""
        if await self.check_complete_from_referer(scrape_item):
            return

        async with self.request_limiter:
            soup = await self.client.get_BS4(self.domain, scrape_item.url)

        link = self._get_download_link(soup, scrape_item.url)
        if not link:
            if self._has_turnstile_challenge(soup):
                raise ScrapeFailure(403, "Fileditch requires Cloudflare Turnstile verification")
            raise ScrapeFailure(422, "Fileditch download link not found")

        if link.path == HOMEPAGE_CATCHALL_FILE:
            raise ScrapeFailure(422, "Fileditch returned homepage catchall file")

        filename, ext = await get_filename_and_ext(link.name)
        await self.handle_file(link, scrape_item, filename, ext)

    @error_handling_wrapper
    async def direct_file(self, scrape_item: ScrapeItem) -> None:
        """Handles older Fileditch files that are only direct linkable"""
        if await self.check_complete_from_referer(scrape_item):
            return

        if scrape_item.url.path == HOMEPAGE_CATCHALL_FILE:
            raise ScrapeFailure(422, "Fileditch returned homepage catchall file")

        filename, ext = await get_filename_and_ext(scrape_item.url.name)
        await self.handle_file(scrape_item.url, scrape_item, filename, ext)

    def _absolute_url(self, href: str, base_url: URL) -> URL:
        if href.startswith("http"):
            return URL(href)
        return base_url.join(URL(href))

    def _get_download_link(self, soup, base_url: URL) -> URL | None:
        for selector in DOWNLOAD_SELECTORS:
            link_container = soup.select_one(selector)
            if not link_container:
                continue

            href = link_container.get("href") or link_container.get("src")
            if href:
                return self._absolute_url(href, base_url)

        return None

    def _has_turnstile_challenge(self, soup) -> bool:
        return bool(soup.select_one(".cf-turnstile, form#ts-form"))
