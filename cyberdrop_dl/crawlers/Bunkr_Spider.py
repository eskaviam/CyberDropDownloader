from __future__ import annotations

import html
import re
from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter
from yarl import URL

from ..base_functions.base_functions import (
    FILE_FORMATS,
    get_filename_and_ext,
    log,
    logger,
    make_title_safe,
)
from ..base_functions.data_classes import AlbumItem, MediaItem
from ..base_functions.error_classes import NoExtensionFailure

if TYPE_CHECKING:
    from ..base_functions.base_functions import ErrorFileWriter
    from ..base_functions.sql_helper import SQLHelper
    from ..client.client import ScrapeSession


class BunkrCrawler:
    def __init__(self, quiet: bool, SQL_Helper: SQLHelper, remove_bunkr_id: bool, error_writer: ErrorFileWriter):
        self.quiet = quiet
        self.SQL_Helper = SQL_Helper
        self.remove_bunkr_id = remove_bunkr_id
        self.limiter = AsyncLimiter(10, 1)
        self.small_limiter = AsyncLimiter(1, 1)

        self.error_writer = error_writer

        self.primary_base_domain = URL("https://bunkrr.su")
        self.api_link = URL(f"https://api-v2.{self.primary_base_domain.host}")

    async def set_cookie(self, session, ddg1, ddg2, ddg5, ddgid, ddgmark):
        """Sets the given tokens as cookies into the session (and client)"""
        cookie_domain = 'bunkrr.su'
        
        # Create separate cookies for each key
        cookie_ddg1 = http.cookies.Morsel()
        cookie_ddg1['domain'] = cookie_domain
        cookie_ddg1.set('__ddg1_', ddg1, ddg1)

        cookie_ddg2 = http.cookies.Morsel()
        cookie_ddg2['domain'] = cookie_domain
        cookie_ddg2.set('__ddg2_', ddg2, ddg2)

        cookie_ddg2 = http.cookies.Morsel()
        cookie_ddg2['domain'] = cookie_domain
        cookie_ddg2.set('__ddg5_', ddg5, ddg5)

        cookie_ddgid = http.cookies.Morsel()
        cookie_ddgid['domain'] = cookie_domain
        cookie_ddgid.set('__ddgid_', ddgid, ddgid)

        cookie_ddgid = http.cookies.Morsel()
        cookie_ddgid['domain'] = cookie_domain
        cookie_ddgid.set('__ddgmark_', ddgmark, ddgmark)

        # Update the cookie jar with the new cookies
        session.client_session.cookie_jar.update_cookies({
            cookie_domain: cookie_ddg1,
            cookie_domain: cookie_ddg2,
            cookie_domain: cookie_ddg3,
            cookie_domain: cookie_ddgid,
            cookie_domain: cookie_ddgmark,
        })



    async def fetch(self, session, url):
        """Scraper for Bunkr"""
        album_obj = AlbumItem("Loose Bunkr Files", [])
        log(f"Starting: {url}", quiet=self.quiet, style="green")
        conf_file_path = '/content/conf.json'
        if os.path.exists(conf_file_path):
            with open(conf_file_path, 'r') as conf_file:
                conf_data = json.load(conf_file)
        else:
            print("No config file found")
            return

        ddg1 = conf_data.get('bunkrr_ddg1')
        ddg2 = conf_data.get('bunkrr_ddg2')
        ddg5 = conf_data.get('bunkrr_ddg5')
        ddgid = conf_data.get('bunkrr_ddgid')
        ddgmark = conf_data.get('bunkrr_ddgmark')
        
        # You need to call the set_cookie function with the session parameter and the values for ddg1, ddg2, and ddgid
        await self.set_cookie(session, ddg1, ddg2, ddg5, ddgid, ddgmark)

        url = await self.get_stream_link(url)

        if "v" in url.parts or "d" in url.parts:
            if "v" in url.parts:
                media = await self.get_video(session, url)
            else:
                media = await self.get_file(session, url)
            if not media.filename:
                return album_obj
            await album_obj.add_media(media)
            log(f"Finished: {url}", quiet=self.quiet, style="green")
            if not media.complete:
                await self.SQL_Helper.insert_media("bunkr", "", media)
            return album_obj

        if "a" in url.parts:
            album_obj = await self.get_album(session, url)
            await self.SQL_Helper.insert_album("bunkr", url, album_obj)

            if album_obj.media:
                log(f"Finished: {url}", quiet=self.quiet, style="green")
            return album_obj

        ext = '.' + url.parts[-1].split('.')[-1]
        if ext:
            ext = ext.lower()
        if ext in FILE_FORMATS['Images']:
            filename, ext = await get_filename_and_ext(url.name)
            original_filename, filename = await self.remove_id(filename, ext)

            await self.SQL_Helper.fix_bunkr_entries(url, original_filename)
            check_complete = await self.SQL_Helper.check_complete_singular("bunkr", url)

            media_item = MediaItem(url, url, check_complete, filename, ext, original_filename)
            await album_obj.add_media(media_item)
        else:
            media_item = await self.get_file(session, url)
            await album_obj.add_media(media_item)

        await self.SQL_Helper.insert_album("bunkr", url, album_obj)
        log(f"Finished: {url}", quiet=self.quiet, style="green")
        return album_obj

    async def get_stream_link(self, url: URL):
        cdn_possibilities = r"^(?:media-files|cdn|c)[0-9]{0,2}\.bunkrr?\.[a-z]{2,3}$"

        if not re.match(cdn_possibilities, url.host):
            return url

        ext = url.suffix.lower()
        if ext == "":
            return url

        if ext in FILE_FORMATS['Images']:
            url = url.with_host(re.sub(r"^cdn(\d*)\.", r"i\1.", url.host))
        elif ext in FILE_FORMATS['Videos']:
            url = self.primary_base_domain / "v" / url.parts[-1]
        else:
            url = self.primary_base_domain / "d" / url.parts[-1]

        return url

    async def remove_id(self, filename: str, ext: str):
        """Removes the additional string bunkr adds to the end of every filename"""
        original_filename = filename
        if self.remove_bunkr_id:
            filename = filename.rsplit(ext, 1)[0]
            filename = filename.rsplit("-", 1)[0]
            if ext not in filename:
                filename = filename + ext
        return original_filename, filename

    async def check_for_la(self, url: URL):
        assert url.host is not None
        if "12" in url.host:
            url_host = url.host.replace(".su", ".la").replace(".ru", ".la")
            url = url.with_host(url_host)
        return url

    async def get_video(self, session: ScrapeSession, url: URL):
        # filename = url.parts[-1] if url.parts[-1] else url.parts[-2]
        # async with self.small_limiter:
        #     json_obj = await session.post(self.api_link / "getToken", {})
        #     if not json_obj:
        #         raise Exception("No Token Object returned")
        #     token = json_obj["token"]
        #
        #     queries = {"file_name": filename, "tkn": token}
        #     link = (self.api_link / "getFile").with_query(queries)
        #     headers_resp, link_resp = await session.head(link, {"Referer": str(url)})

        async with self.limiter:
            soup = await session.get_BS4(url)
            link = soup.select_one("a[class*=bg-blue-500]")
            link_resp = URL(link.get("href"))

        try:
            filename, ext = await get_filename_and_ext(link_resp.name)
        except NoExtensionFailure:
            filename, ext = await get_filename_and_ext(url.name)
        if ext not in FILE_FORMATS['Images']:
            link_resp = await self.check_for_la(link_resp)

        original_filename, filename = await self.remove_id(filename, ext)

        await self.SQL_Helper.fix_bunkr_entries(link_resp, original_filename)
        complete = await self.SQL_Helper.check_complete_singular("bunkr", link_resp)
        return MediaItem(link_resp, url, complete, filename, ext, original_filename)

    async def get_file(self, session: ScrapeSession, url: URL):
        """Gets the media item from the supplied url"""

        url = self.primary_base_domain.with_path(url.path)

        try:
            async with self.limiter:
                soup = await session.get_BS4(url)
            head = soup.select_one("head")
            scripts = head.select('script[type="text/javascript"]')
            link = None

            for script in scripts:
                if script.text and "link.href" in script.text:
                    link = script.text.split('link.href = "')[-1].split('";')[0]
                    break
            if not link:
                raise

            # URL Cleanup
            link = URL(html.unescape(str(link)))

            try:
                filename, ext = await get_filename_and_ext(link.name)
            except NoExtensionFailure:
                filename, ext = await get_filename_and_ext(url.name)
            if ext not in FILE_FORMATS['Images']:
                link = await self.check_for_la(link)

            original_filename, filename = await self.remove_id(filename, ext)

            await self.SQL_Helper.fix_bunkr_entries(link, original_filename)
            complete = await self.SQL_Helper.check_complete_singular("bunkr", link)
            return MediaItem(link, url, complete, filename, ext, original_filename)

        except Exception as e:
            logger.debug("Error encountered while handling %s", url, exc_info=True)
            log(f"Error: {url}", quiet=self.quiet, style="red")
            await self.error_writer.write_errored_scrape(url, e, self.quiet)
            logger.debug(e)
            return MediaItem(url, url, False, "", "", "")

    async def get_album(self, session: ScrapeSession, url: URL):
        """Iterates through an album and creates the media items"""

        url = self.primary_base_domain.with_path(url.path)

        album = AlbumItem(url.name, [])
        try:
            async with self.limiter:
                soup = await session.get_BS4(url)
            title = soup.select_one('h1[class="text-[24px] font-bold text-dark dark:text-white"]')
            for elem in title.find_all("span"):
                elem.decompose()
            title = await make_title_safe(title.get_text())
            await album.set_new_title(title)
            for file in soup.select('a[class*="grid-images_box-link"]'):
                link = file.get("href")

                assert url.host is not None
                if link.startswith("/"):
                    link = URL("https://" + url.host + link)
                link = URL(link)

                try:
                    referer = await self.get_stream_link(link)
                except Exception as e:
                    logger.debug("Error encountered while handling %s", link, exc_info=True)
                    log(f"Error: {link}", quiet=self.quiet, style="red")
                    await self.error_writer.write_errored_scrape(link, e, self.quiet)
                    logger.debug(e)
                    continue

                try:
                    filename, ext = await get_filename_and_ext(link.name)
                except NoExtensionFailure:
                    logger.debug("Couldn't get extension for %s", link)
                    continue

                if ext in FILE_FORMATS["Images"]:
                    if "d" in link.parts:
                        media = await self.get_file(session, referer)
                        link = media.url
                    link = URL(str(link).replace("https://cdn", "https://i"))
                else:
                    try:
                        if "v" in referer.parts:
                            media = await self.get_video(session, referer)
                        else:
                            media = await self.get_file(session, referer)
                        link = media.url
                    except Exception as e:
                        logger.debug("Error encountered while handling %s", referer, exc_info=True)
                        await self.error_writer.write_errored_scrape(referer, e, self.quiet)
                        continue

                if ext not in FILE_FORMATS['Images']:
                    link = await self.check_for_la(link)

                original_filename, filename = await self.remove_id(filename, ext)

                await self.SQL_Helper.fix_bunkr_entries(link, original_filename)
                complete = await self.SQL_Helper.check_complete_singular("bunkr", link)
                media = MediaItem(link, referer, complete, filename, ext, original_filename)
                await album.add_media(media)

        except Exception as e:
            logger.debug("Error encountered while handling %s", url, exc_info=True)
            await self.error_writer.write_errored_scrape(url, e, self.quiet)

        return album