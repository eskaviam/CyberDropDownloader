from __future__ import annotations

from typing import TYPE_CHECKING

from yarl import URL

from cyberdrop_dl.scraper.crawler import Crawler
from cyberdrop_dl.utils.utilities import get_filename_and_ext, error_handling_wrapper

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager
    from cyberdrop_dl.utils.dataclasses.url_objects import ScrapeItem


class TurbovidCrawler(Crawler):
    def __init__(self, manager: Manager):
        super().__init__(manager, "turbovid", "Turbovid")

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        """Fetch the turbovid link"""
        task_id = await self.scraping_progress.add_task(scrape_item.url)
        await self.scrape(scrape_item)
        await self.scraping_progress.remove_task(task_id)

    @error_handling_wrapper
    async def scrape(self, scrape_item: ScrapeItem) -> None:
        """Scrapes the turbovid page using the signing API"""
        if await self.check_complete_from_referer(scrape_item):
            return

        video_id = scrape_item.url.parts[-1]
        if "/embed/" not in scrape_item.url.path:
            embed_url = scrape_item.url.with_path(f"/embed/{video_id}")
        else:
            embed_url = scrape_item.url

        # Dynamically set the API URL based on the incoming domain (turbovid.cr or turbo.cr)
        api_url = URL(f"https://{scrape_item.url.host}/api/sign")
        
        params = {
            'v': video_id,
        }
        
        headers = {
            'referer': str(embed_url),
            'x-requested-with': 'XMLHttpRequest',
            'accept': '*/*',
        }
        
        self.manager.client_manager.cookies.update_cookies(
            {'captcha_verified': '1'}, 
            response_url=URL(f"https://{scrape_item.url.host}")
        )

        data = await self.client.get_json(
            self.domain, 
            api_url, 
            params=params, 
            headers_inc=headers
        )

        if not data.get("success"):
            await self.manager.log_manager.error(f"Turbovid API error: {data} for {scrape_item.url}")
            return

        video_link = URL(data["url"])
        
        # Use filename from API or fallback to ID
        filename_str = data.get("filename") or f"{video_id}.mp4"
        filename, ext = await get_filename_and_ext(filename_str)
        
        await self.handle_file(video_link, scrape_item, filename, ext)