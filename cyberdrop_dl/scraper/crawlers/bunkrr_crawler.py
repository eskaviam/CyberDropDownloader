from __future__ import annotations

import calendar
import datetime
import re
from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter
from yarl import URL

from cyberdrop_dl.clients.errors import NoExtensionFailure
from cyberdrop_dl.scraper.crawler import Crawler
from cyberdrop_dl.utils.dataclasses.url_objects import ScrapeItem
from cyberdrop_dl.utils.utilities import FILE_FORMATS, get_filename_and_ext, error_handling_wrapper

if TYPE_CHECKING:
    from cyberdrop_dl.managers.manager import Manager


class BunkrrCrawler(Crawler):
    def __init__(self, manager: Manager):
        super().__init__(manager, "bunkrr", "Bunkrr")
        self.primary_base_domain = URL("https://bunkr.sk")
        self.request_limiter = AsyncLimiter(10, 1)

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def fetch(self, scrape_item: ScrapeItem) -> None:
        task_id = await self.scraping_progress.add_task(scrape_item.url)
        
        if not scrape_item.url:
            print(f"[FETCH] Invalid URL: {scrape_item.url}")
            self.manager.logger.error(f"Invalid URL: {scrape_item.url}")
            await self.scraping_progress.remove_task(task_id)
            return
            
        scrape_item.url = await self.get_stream_link(scrape_item.url)
        print(f"[FETCH] After get_stream_link: {scrape_item.url}")

        if scrape_item.url.host and scrape_item.url.host.startswith("get"):
            print("[FETCH] Host starts with 'get', calling reinforced_link")
            scrape_item.url = await self.reinforced_link(scrape_item.url)
            if not scrape_item.url:
                print("[FETCH] reinforced_link returned None")
                await self.scraping_progress.remove_task(task_id)
                return
            scrape_item.url = await self.get_stream_link(scrape_item.url)
            print(f"[FETCH] After second get_stream_link: {scrape_item.url}")

        print(f"[FETCH] URL parts: {scrape_item.url.parts}")
        if scrape_item.url.parts and len(scrape_item.url.parts) > 1:
            if "a" in scrape_item.url.parts:
                print("[FETCH] Directing to album handler")
                await self.album(scrape_item)
            elif "v" in scrape_item.url.parts:
                print("[FETCH] Directing to video handler")
                await self.video(scrape_item)
            else:
                print("[FETCH] Directing to other handler")
                await self.other(scrape_item)
        else:
            print(f"[FETCH] URL has no parts: {scrape_item.url}")
            self.manager.logger.error(f"URL has no parts: {scrape_item.url}")
            
        await self.scraping_progress.remove_task(task_id)

    @error_handling_wrapper
    async def album(self, scrape_item: ScrapeItem) -> None:
        scrape_item.url = self.primary_base_domain.with_path(scrape_item.url.path)
        album_id = scrape_item.url.parts[2]
        results = await self.get_album_results(album_id)

        async with self.request_limiter:
            soup = await self.client.get_BS4(self.domain, scrape_item.url)
        title = soup.select_one('h1[class="truncate"]')

        title = await self.create_title(title.get_text().strip(), scrape_item.url.parts[2], None)
        await scrape_item.add_to_parent_title(title)

        card_listings = soup.select('div[class*="relative group/item theItem"]')
        for card_listing in card_listings:
            file = card_listing.select_one('a')
            date = await self.parse_datetime(card_listing.select_one('span[class*="theDate"]').text.strip())
            link = file.get("href")
            if link.startswith("/"):
                link = URL("https://" + scrape_item.url.host + link)
            link = URL(link)
            link = await self.get_stream_link(link)

            try:
                filename = card_listing.select_one('p[class*="truncate theName"]').text
                file_ext = "." + filename.split(".")[-1]
                if file_ext.lower() not in FILE_FORMATS['Images'] and file_ext.lower() not in FILE_FORMATS['Videos']:
                    raise FileNotFoundError()
                image_obj = card_listing.select_one("img")
                src = image_obj.get("src")
                src = src.replace("/thumbs/", "/")
                src = URL(src, encoded=True)
                src = src.with_suffix(file_ext)
                src = src.with_query("download=true")
                if file_ext.lower() not in FILE_FORMATS['Images']:
                    src = src.with_host(src.host.replace("i-", ""))
                new_scrape_item = await self.create_scrape_item(scrape_item, link, "", True, album_id, date)

                if "no-image" in src.name:
                    raise FileNotFoundError("No image found, reverting to parent")

                filename, ext = await get_filename_and_ext(src.name)
                if not await self.check_album_results(src, results):
                    await self.handle_file(src, new_scrape_item, filename, ext)
            except FileNotFoundError:
                self.manager.task_group.create_task(self.run(ScrapeItem(link, scrape_item.parent_title, True, album_id, date)))

    @error_handling_wrapper
    async def video(self, scrape_item: ScrapeItem) -> None:
        scrape_item.url = self.primary_base_domain.with_path(scrape_item.url.path)
        if await self.check_complete_from_referer(scrape_item):
            return

        async with self.request_limiter:
            soup = await self.client.get_BS4(self.domain, scrape_item.url)
        
        link_container = None
        selectors = [
            "a[class*=ic-download-01]",
            "a[href*='get.bunkrr.su/file/']",
            "a[class*='btn-main'][href*='get.bunkrr']",
            "a[class*='download']",
            "a[href*='.mp4']"
        ]
        
        for selector in selectors:
            links = soup.select(selector)
            if links:
                link_container = links[-1]
                break
                
        if not link_container:
            video_source = soup.select_one("video source")
            if video_source:
                link = URL(video_source.get("src"))
                try:
                    filename, ext = await get_filename_and_ext(link.name)
                except NoExtensionFailure:
                    title_tag = soup.select_one("title")
                    if title_tag:
                        title_text = title_tag.text.strip()
                        if "|" in title_text:
                            possible_filename = title_text.split("|")[0].strip()
                            if "." in possible_filename:
                                try:
                                    filename, ext = await get_filename_and_ext(possible_filename)
                                    await self.handle_file(link, scrape_item, filename, ext)
                                    return
                                except NoExtensionFailure:
                                    pass
                    
                    filename = scrape_item.url.parts[-1]
                    ext = ".mp4"
                
                await self.handle_file(link, scrape_item, filename, ext)
                return
            else:
                self.manager.logger.error(f"Could not find download link or video source for {scrape_item.url}")
                return

        href = link_container.get('href')
        if not href:
            self.manager.logger.error(f"Found link container but no href attribute for {scrape_item.url}")
            title_tag = soup.select_one('title')
            if title_tag and '.' in title_tag.text:
                try:
                    title_text = title_tag.text.strip()
                    if '|' in title_text:
                        possible_filename = title_text.split('|')[0].strip()
                        filename, ext = await get_filename_and_ext(possible_filename)
                    else:
                        filename, ext = await get_filename_and_ext(title_tag.text.strip())
                    
                    video_source = soup.select_one('video source')
                    if video_source and video_source.get('src'):
                        link = URL(video_source.get('src'))
                        await self.handle_file(link, scrape_item, filename, ext)
                        return
                except NoExtensionFailure:
                    pass
            
            video_source = soup.select_one('video source')
            if video_source and video_source.get('src'):
                link = URL(video_source.get('src'))
                filename = scrape_item.url.parts[-1]
                ext = ".mp4"
                await self.handle_file(link, scrape_item, filename, ext)
                return
            else:
                self.manager.logger.error(f"Could not find any usable source for {scrape_item.url}")
                return

        link = URL(href)

        try:
            filename, ext = await get_filename_and_ext(link.name)
        except NoExtensionFailure:
            try:
                video_source = soup.select_one('video source')
                if video_source:
                    src_url = URL(video_source.get('src'))
                    filename, ext = await get_filename_and_ext(src_url.name)
                    link = src_url
                else:
                    raise NoExtensionFailure()
            except (NoExtensionFailure, AttributeError):
                if link and link.host and "get" in link.host:
                    link = await self.reinforced_link(link)
                    if not link:
                        return
                    filename, ext = await get_filename_and_ext(link.name)
                else:
                    title_tag = soup.select_one('title')
                    if title_tag and '.' in title_tag.text:
                        try:
                            title_text = title_tag.text.strip()
                            if '|' in title_text:
                                possible_filename = title_text.split('|')[0].strip()
                                filename, ext = await get_filename_and_ext(possible_filename)
                            else:
                                filename, ext = await get_filename_and_ext(title_tag.text.strip())
                        except NoExtensionFailure:
                            filename = scrape_item.url.parts[-1]
                            ext = ".mp4"
                    else:
                        filename = scrape_item.url.parts[-1]
                        ext = ".mp4"

        await self.handle_file(link, scrape_item, filename, ext)

    @error_handling_wrapper
    async def other(self, scrape_item: ScrapeItem) -> None:
        print(f"[OTHER] Starting to process: {scrape_item.url}")
        scrape_item.url = self.primary_base_domain.with_path(scrape_item.url.path)
        print(f"[OTHER] Modified URL to primary domain: {scrape_item.url}")
        
        if await self.check_complete_from_referer(scrape_item):
            print("[OTHER] URL already completed from referrer")
            return

        filename = ""
        ext = ""
        
        print("[OTHER] Getting BS4 from URL")
        async with self.request_limiter:
            soup = await self.client.get_BS4(self.domain, scrape_item.url)
        print("[OTHER] BS4 retrieval successful")

        title_h1 = soup.select_one('h1.text-2xl.text-subs, h1.text-2xl')
        if title_h1 and '.mp4' in title_h1.text:
            print(f"[OTHER] Found title with filename: {title_h1.text}")
            
            try:
                filename, ext = await get_filename_and_ext(title_h1.text.strip())
                print(f"[OTHER] Extracted filename: {filename}, ext: {ext}")
                
                file_id = None
                
                download_link = soup.select_one('a[href*="get.bunkrr.su/file/"]')
                if download_link and download_link.get('href'):
                    link_parts = download_link.get('href').split('/')
                    if link_parts and len(link_parts) > 0:
                        file_id = link_parts[-1]
                        print(f"[OTHER] Got file ID from download link: {file_id}")
                
                if not file_id and scrape_item.url.parts and len(scrape_item.url.parts) > 2:
                    file_id = scrape_item.url.parts[-1]
                    print(f"[OTHER] Got file ID from URL: {file_id}")
                
                if file_id:
                    cdn_domains = [
                        "i-wiener.bunkr.ru", "wiener.bunkr.ru", "ramen.bunkr.ru", "i-ramen.bunkr.ru",
                        "nachos.bunkr.ru", "cdn-wiener.bunkr.ru"
                    ]
                    
                    for cdn in cdn_domains:
                        video_url = URL(f"https://{cdn}/{file_id}{ext}")
                        print(f"[OTHER] Trying CDN URL: {video_url}")
                        await self.handle_file(video_url, scrape_item, filename, ext)
                        return
            except NoExtensionFailure:
                print("[OTHER] Failed to extract extension from title")
        
        print("[OTHER] Checking for video source first")
        video_source = soup.select_one('video source')
        if video_source and video_source.get('src'):
            print("[OTHER] Found video source directly")
            link = URL(video_source.get('src'))
            print(f"[OTHER] Video source URL: {link}")
            
            if not filename or not ext:
                try:
                    filename, ext = await get_filename_and_ext(link.name)
                    print(f"[OTHER] Using filename from link: {filename}, ext: {ext}")
                except NoExtensionFailure:
                    filename = scrape_item.url.parts[-1]
                    ext = ".mp4"
                    print(f"[OTHER] Using URL part as filename: {filename}, with ext: {ext}")
                    
            print(f"[OTHER] Calling handle_file with: link={link}, filename={filename}, ext={ext}")
            await self.handle_file(link, scrape_item, filename, ext)
            return

        print("[OTHER] Trying to find download link with specific selectors")
        
        download_selectors = [
            'a[href*="get.bunkrr.su/file/"]',
            'a[class*="btn-main"][href*="get.bunkrr"]',
            'a[href*="get.bunkr"]',
            'a[class*="download"]',
            'a[download]',
            'a[class*="btn-main"][href*="download"]'
        ]
        
        link_container = None
        for selector in download_selectors:
            links = soup.select(selector)
            if links:
                for possible_link in links:
                    href = possible_link.get('href')
                    if href and not href.endswith('/upload') and not href.startswith('/upload'):
                        link_container = possible_link
                        print(f"[OTHER] Found download link with selector: {selector}")
                        break
                if link_container:
                    break
        
        if not link_container:
            print("[OTHER] No download-specific link found, trying file-type selectors")
            file_selectors = [
                'a[href*=".mp4"]',
                'a[href*=".jpg"]',
                'a[href*=".png"]',
                'a[href*=".gif"]'
            ]
            
            for selector in file_selectors:
                links = soup.select(selector)
                if links:
                    link_container = links[-1]
                    print(f"[OTHER] Found link container with selector: {selector}")
                    break
            
        if link_container:
            href = link_container.get('href')
            if href and href != '#':
                print(f"[OTHER] Found href: {href}")
                link = URL(href)
                print(f"[OTHER] Created URL object: {link}")
                
                if link.host and 'get.bunkr' in link.host:
                    print("[OTHER] Link is to get.bunkrr.su, calling reinforced_link")
                    link = await self.reinforced_link(link)
                    
                    if not link and filename and ext:
                        file_id = href.split('/')[-1]
                        print("[OTHER] Reinforced link failed, using file_id and existing filename")
                        link = URL(f"https://i-wiener.bunkr.ru/{file_id}{ext}")
                
                if filename and ext:
                    print(f"[OTHER] Using existing filename: {filename}, ext: {ext}")
                else:
                    try:
                        filename, ext = await get_filename_and_ext(link.name)
                        print(f"[OTHER] Got filename from link: {filename}, ext: {ext}")
                    except (NoExtensionFailure, AttributeError):
                        filename = scrape_item.url.parts[-1]
                        ext = ".mp4"
                        print(f"[OTHER] Using URL part: {filename} with ext: {ext}")
                
                if link:
                    print(f"[OTHER] Calling handle_file with: link={link}, filename={filename}, ext={ext}")
                    await self.handle_file(link, scrape_item, filename, ext)
                    return
        
        print("[OTHER] No direct link found, attempting to extract video information")
        
        video_url, extracted_filename, extracted_ext = await self.extract_video_from_get_page(soup, scrape_item.url)
        if video_url:
            print(f"[OTHER] Successfully extracted video URL: {video_url}")
            if extracted_filename and extracted_ext:
                filename = extracted_filename
                ext = extracted_ext
                print(f"[OTHER] Using extracted filename: {filename}, ext: {ext}")
            elif not filename or not ext:
                try:
                    filename, ext = await get_filename_and_ext(video_url.name)
                    print(f"[OTHER] Got filename from video URL: {filename}, ext: {ext}")
                except NoExtensionFailure:
                    filename = scrape_item.url.parts[-1]
                    ext = ".mp4"
                    print(f"[OTHER] Using URL part: {filename} with default ext: {ext}")
            
            print(f"[OTHER] Calling handle_file with: link={video_url}, filename={filename}, ext={ext}")
            await self.handle_file(video_url, scrape_item, filename, ext)
            return
        
        if len(scrape_item.url.parts) > 1:
            file_id = scrape_item.url.parts[-1]
            print(f"[OTHER] Last resort - using file ID from URL: {file_id}")
            
            if not filename or not ext:
                filename = file_id
                ext = ".mp4"
            
            video_url = URL(f"https://i-wiener.bunkr.ru/{file_id}{ext}")
            print(f"[OTHER] Constructed potential URL: {video_url}")
            await self.handle_file(video_url, scrape_item, filename, ext)
            return
            
        print("[OTHER] Failed to find any usable video source")
        self.manager.logger.error(f"Could not find any usable source for {scrape_item.url}")
        return

    @error_handling_wrapper
    async def extract_video_from_get_page(self, soup, url) -> tuple:
        print(f"[EXTRACT_VIDEO] Processing get.bunkrr.su page for: {url}")
        
        extracted_filename = None
        extracted_ext = None
        
        title = soup.select_one('h1.text-2xl')
        if title:
            title_text = title.text.strip()
            print(f"[EXTRACT_VIDEO] Found title: {title_text}")
            if ".mp4" in title_text:
                try:
                    extracted_filename, extracted_ext = await get_filename_and_ext(title_text)
                    print(f"[EXTRACT_VIDEO] Extracted filename: {extracted_filename}, ext: {extracted_ext}")
                except NoExtensionFailure:
                    print("[EXTRACT_VIDEO] Failed to extract extension from title")
        
        scripts = soup.select('script[data-v]')
        for script in scripts:
            data_v = script.get('data-v')
            if data_v and data_v.endswith('.mp4'):
                print(f"[EXTRACT_VIDEO] Found data-v attribute with mp4: {data_v}")
                file_id = data_v.split('/')[-1].replace('.mp4', '')
                print(f"[EXTRACT_VIDEO] Extracted file ID: {file_id}")
                
                cdn_domains = [
                    "wiener.bunkr.ru", "i-wiener.bunkr.ru", "ramen.bunkr.ru", "i-ramen.bunkr.ru",
                    "nachos.bunkr.ru", "cdn-wiener.bunkr.ru", "cdn-ramen.bunkr.ru", "i-burger.bunkr.ru",
                    "i-pizza.bunkr.ru", "i-fries.bunkr.ru"
                ]
                
                for cdn in cdn_domains:
                    video_url = URL(f"https://{cdn}/{data_v}")
                    print(f"[EXTRACT_VIDEO] Trying URL: {video_url}")
                    return (video_url, extracted_filename, extracted_ext)
        
        file_tracker = soup.select_one('#fileTracker')
        if file_tracker:
            file_id = file_tracker.get('data-file-id')
            if file_id:
                print(f"[EXTRACT_VIDEO] Found file ID in tracker: {file_id}")
                file_id = url.parts[-1] if url.parts else file_id
                
                cdn_domains = [
                    "wiener.bunkr.ru", "i-wiener.bunkr.ru", "ramen.bunkr.ru", "i-ramen.bunkr.ru",
                    "nachos.bunkr.ru", "cdn-wiener.bunkr.ru", "cdn-ramen.bunkr.ru", "i-burger.bunkr.ru",
                    "i-pizza.bunkr.ru", "i-fries.bunkr.ru"
                ]
                
                for cdn in cdn_domains:
                    video_url = URL(f"https://{cdn}/{file_id}.mp4")
                    print(f"[EXTRACT_VIDEO] Trying URL with file ID: {video_url}")
                    return (video_url, extracted_filename, extracted_ext)
        
        for script in soup.select('script'):
            if script.string and '.mp4' in script.string:
                matches = re.findall(r'https?://[^"\']+\.mp4', script.string)
                if matches:
                    video_url = URL(matches[0])
                    print(f"[EXTRACT_VIDEO] Found video URL in script: {video_url}")
                    return (video_url, extracted_filename, extracted_ext)
                    
                matches = re.findall(r'src=["\'](https?://[^"\']+\.mp4)["\']', script.string)
                if matches:
                    video_url = URL(matches[0])
                    print(f"[EXTRACT_VIDEO] Found video URL in src attribute: {video_url}")
                    return (video_url, extracted_filename, extracted_ext)
        
        cdn_pattern = re.compile(r'(https?://(?:(?:i-)?(?:wiener|ramen|nachos|burger|pizza|fries)\.bunkr\.(?:ru|sk|pk|su))/[^"\']+\.mp4)')
        for script in soup.select('script'):
            if script.string:
                matches = cdn_pattern.findall(script.string)
                if matches:
                    video_url = URL(matches[0])
                    print(f"[EXTRACT_VIDEO] Found CDN URL in script: {video_url}")
                    return (video_url, extracted_filename, extracted_ext)
        
        file_id = url.parts[-1] if url.parts else None
        if file_id:
            cdn_domains = [
                "wiener.bunkr.ru", "i-wiener.bunkr.ru", "ramen.bunkr.ru", "i-ramen.bunkr.ru",
                "nachos.bunkr.ru", "cdn-wiener.bunkr.ru", "cdn-ramen.bunkr.ru", "i-burger.bunkr.ru",
                "i-pizza.bunkr.ru", "i-fries.bunkr.ru"
            ]
            
            for cdn in cdn_domains:
                video_url = URL(f"https://{cdn}/{file_id}.mp4")
                print(f"[EXTRACT_VIDEO] Last resort URL with file ID: {video_url}")
                return (video_url, extracted_filename, extracted_ext)
                
        print("[EXTRACT_VIDEO] Could not extract video URL from page")
        return (None, None, None)

    @error_handling_wrapper
    async def reinforced_link(self, url: URL) -> URL:
        print(f"[REINFORCED_LINK] Starting with URL: {url}")
        async with self.request_limiter:
            print("[REINFORCED_LINK] Getting BS4 for URL")
            soup = await self.client.get_BS4(self.domain, url)
            print("[REINFORCED_LINK] Got BS4 successfully")
        
        if url.host and 'get.bunkr' in url.host:
            print("[REINFORCED_LINK] Using specialized get.bunkrr.su extraction")
            video_url, _, _ = await self.extract_video_from_get_page(soup, url)
            if video_url:
                return video_url
        
        try:
            print("[REINFORCED_LINK] Looking for download links")
            link_container = soup.select('a[download*=""]')[-1]
            print("[REINFORCED_LINK] Found download link with a[download*='']")
        except IndexError:
            print("[REINFORCED_LINK] No download link found, trying alternative selector")
            try:
                link_container = soup.select('a[class*=download]')[-1]
                print("[REINFORCED_LINK] Found download link with a[class*=download]")
            except IndexError:
                print("[REINFORCED_LINK] No download link found with any selector")
                title = soup.select_one('h1.text-2xl.text-subs')
                if title:
                    print(f"[REINFORCED_LINK] Found title: {title.text.strip()}")
                    try:
                        filename = title.text.strip()
                        print(f"[REINFORCED_LINK] Extracted filename from title: {filename}")
                        
                        for element in soup.select('video'):
                            source = element.select_one('source')
                            if source and source.get('src'):
                                video_url = URL(source.get('src'))
                                print(f"[REINFORCED_LINK] Found video source: {video_url}")
                                return video_url
                        
                        for video in soup.select('video'):
                            poster = video.get('poster')
                            if poster and 'thumbs' in poster:
                                print(f"[REINFORCED_LINK] Found poster: {poster}")
                                try:
                                    video_url_str = poster.replace('thumbs/', '').replace('_grid.png', '.mp4')
                                    video_url = URL(video_url_str)
                                    print(f"[REINFORCED_LINK] Transformed poster to video URL: {video_url}")
                                    return video_url
                                except Exception as e:
                                    print(f"[REINFORCED_LINK] Error transforming poster URL: {e}")
                    except Exception as e:
                        print(f"[REINFORCED_LINK] Error extracting video URL: {e}")
                return None
        
        href = link_container.get('href')
        if not href or href == '#':
            print(f"[REINFORCED_LINK] Found link container but href is {href}, trying extract_video_from_get_page")
            video_url, _, _ = await self.extract_video_from_get_page(soup, url)
            if video_url:
                print(f"[REINFORCED_LINK] Successfully extracted video URL: {video_url}")
                return video_url
            
            print("[REINFORCED_LINK] extract_video_from_get_page failed, trying alternative approaches")
            try:
                title = soup.select_one('h1.text-2xl')
                if title:
                    print(f"[REINFORCED_LINK] Found title element: {title.text.strip()}")
                    
                    video = soup.select_one('video')
                    if video:
                        source = video.select_one('source')
                        if source and source.get('src'):
                            video_url = URL(source.get('src'))
                            print(f"[REINFORCED_LINK] Found video source: {video_url}")
                            return video_url
                        
                        poster = video.get('poster')
                        if poster and 'thumbs' in poster:
                            print(f"[REINFORCED_LINK] Found poster: {poster}")
                            try:
                                video_url_str = poster.replace('thumbs/', '').replace('_grid.png', '.mp4')
                                video_url = URL(video_url_str)
                                print(f"[REINFORCED_LINK] Transformed poster to video URL: {video_url}")
                                return video_url
                            except Exception as e:
                                print(f"[REINFORCED_LINK] Error transforming poster URL: {e}")
                
                for script in soup.select('script'):
                    if script.string and '.mp4' in script.string:
                        print("[REINFORCED_LINK] Found script with mp4 reference")
                        matches = re.findall(r'https?://[^"\']+\.mp4', script.string)
                        if matches:
                            video_url = URL(matches[0])
                            print(f"[REINFORCED_LINK] Extracted video URL from script: {video_url}")
                            return video_url
                
                scripts = soup.select('script[data-t]')
                for script in scripts:
                    data_t = script.get('data-t')
                    if data_t and ".mp4" in data_t:
                        print(f"[REINFORCED_LINK] Found data-t attribute with mp4: {data_t}")
                        video_url = URL(data_t)
                        return video_url
                
                file_id = url.parts[-1] if url.parts else None
                if file_id:
                    print(f"[REINFORCED_LINK] Using file ID from URL: {file_id}")
                    cdn_domains = [
                        "i-wiener.bunkr.ru", "wiener.bunkr.ru", "ramen.bunkr.ru", "i-ramen.bunkr.ru",
                        "nachos.bunkr.ru", "cdn-wiener.bunkr.ru"
                    ]
                    
                    for cdn in cdn_domains:
                        video_url = URL(f"https://{cdn}/{file_id}.mp4")
                        print(f"[REINFORCED_LINK] Trying CDN URL: {video_url}")
                        return video_url
            except Exception as e:
                print(f"[REINFORCED_LINK] Error finding alternative source: {e}")
            return None
            
        print(f"[REINFORCED_LINK] Got href: {href}")
        link = URL(href)
        print(f"[REINFORCED_LINK] Returning URL: {link}")
        return link

    """~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    async def get_stream_link(self, url: URL) -> URL:
        print(f"[GET_STREAM_LINK] Starting with URL: {url}")
        cdn_domains = [
            "cdn-wiener", "i-wings", "i-soup", "ramen", "i-ramen", "i-wiener", "nachos", 
            "cdn-ramen", "i-burger", "i-pizza", "i-fries", "wiener", "media-files", 
            "cdn", "c", "pizza", "cdn-burger", "cdn-nugget", "burger", "taquito", 
            "fries", "meatballs", "milkshake", "kebab", "nachos", "big-taco", 
            "cdn-pizza", "cdn-meatballs", "cdn-milkshake", "i.kebab", "i.fries", 
            "i-nugget", "i-milkshake"
        ]
        
        if not url.host:
            print(f"[GET_STREAM_LINK] URL has no host, returning as is: {url}")
            return url
            
        host_parts = url.host.split('.')
        bunkr_tlds = ["ru", "sk", "pk", "su", "black", "cr"]
        
        print(f"[GET_STREAM_LINK] Host parts: {host_parts}")
        
        is_cdn = False
        if len(host_parts) > 1:
            domain_part = host_parts[0]
            print(f"[GET_STREAM_LINK] Domain part: {domain_part}")
            
            for cdn in cdn_domains:
                if domain_part == cdn or domain_part.startswith(f"{cdn}-") or domain_part.startswith(f"{cdn}"):
                    print(f"[GET_STREAM_LINK] Matched CDN domain: {cdn}")
                    is_cdn = True
                    break
            
            if is_cdn and len(host_parts) > 1:
                tld = host_parts[-1]
                print(f"[GET_STREAM_LINK] TLD: {tld}")
                if tld not in bunkr_tlds and tld.startswith('bunkr'):
                    print("[GET_STREAM_LINK] TLD starts with 'bunkr', marking as CDN")
                    is_cdn = True
        
        if not is_cdn:
            print(f"[GET_STREAM_LINK] Not a CDN URL, returning as is: {url}")
            return url

        ext = url.suffix.lower() if url.suffix else ""
        print(f"[GET_STREAM_LINK] URL extension: {ext}")
        if ext == "":
            print("[GET_STREAM_LINK] No extension, returning as is")
            return url

        print(f"[GET_STREAM_LINK] URL parts: {url.parts}")
        if not url.parts or len(url.parts) < 1:
            print("[GET_STREAM_LINK] No URL parts, returning as is")
            return url
            
        original_url = url
        if ext in FILE_FORMATS['Images']:
            print(f"[GET_STREAM_LINK] Image extension detected: {ext}")
            url = self.primary_base_domain / "d" / url.parts[-1]
        elif ext in FILE_FORMATS['Videos']:
            print(f"[GET_STREAM_LINK] Video extension detected: {ext}")
            url = self.primary_base_domain / "v" / url.parts[-1]
        else:
            print(f"[GET_STREAM_LINK] Other extension detected: {ext}")
            url = self.primary_base_domain / "d" / url.parts[-1]

        print(f"[GET_STREAM_LINK] Transformed URL from {original_url} to {url}")
        return url

    async def parse_datetime(self, date: str) -> int:
        date = datetime.datetime.strptime(date, "%H:%M:%S %d/%m/%Y")
        return calendar.timegm(date.timetuple())

    def export(self, album, url):
        exportFile = "/home/mahaprasad/CyberDrop/" + album + "-data.txt"
        with open(exportFile, "a") as exportHandle:
            exportHandle.write(url)
            exportHandle.write("\n")
