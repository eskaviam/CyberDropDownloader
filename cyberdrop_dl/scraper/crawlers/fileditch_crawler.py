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


DOWNLOAD_SELECTOR = 'a.btn[href*="md5="]'
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

        link_container = soup.select_one(DOWNLOAD_SELECTOR)
        if not link_container or not link_container.get("href"):
            raise ScrapeFailure(422, "Fileditch download link not found")

        link = self._absolute_url(link_container.get("href"), scrape_item.url)
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
