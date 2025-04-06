from __future__ import annotations

import calendar
import datetime
import re
from typing import TYPE_CHECKING, Tuple, Optional

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
            print(f"Invalid URL: {scrape_item.url}")
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
            print(f"URL has no parts: {scrape_item.url}")
            
        await self.scraping_progress.remove_task(task_id)

    @error_handling_wrapper
    async def album(self, scrape_item: ScrapeItem) -> None:
        print(f"[ALBUM] Processing album URL: {scrape_item.url}")
        scrape_item.url = self.primary_base_domain.with_path(scrape_item.url.path)
        print(f"[ALBUM] Modified URL to primary domain: {scrape_item.url}")
        
        # Try to extract album ID from URL
        album_id = None
        if len(scrape_item.url.parts) > 2:
            album_id = scrape_item.url.parts[2]
            print(f"[ALBUM] Extracted album ID from URL: {album_id}")
        
        results = await self.get_album_results(album_id) if album_id else []

        async with self.request_limiter:
            print(f"[ALBUM] Getting BS4 content for: {scrape_item.url}")
            soup = await self.client.get_BS4(self.domain, scrape_item.url)
            print("[ALBUM] Got BS4 content successfully")

        # Extract album title
        title_element = soup.select_one('h1.truncate') or soup.select_one('div.text-subs.font-semibold h1.truncate')
        if not title_element:
            title_element = soup.select_one('meta[property="og:title"]')
            title = title_element.get('content') if title_element else "Unknown Album"
        else:
            title = title_element.get_text().strip()
            
        print(f"[ALBUM] Found album title: {title}")
        title = await self.create_title(title, album_id or "unknown", None)
        await scrape_item.add_to_parent_title(title)
        print(f"[ALBUM] Created title: {title}")

        # Try different selectors for album items
        card_listings = soup.select('div.relative.group\\/item.theItem')
        if not card_listings:
            card_listings = soup.select('div[class*="relative group/item theItem"]')
            
        print(f"[ALBUM] Found {len(card_listings)} items in the album")

        for card_listing in card_listings:
            try:
                print("[ALBUM] Processing an album item")
                # Extract date
                date_element = card_listing.select_one('span[class*="theDate"]')
                date = await self.parse_datetime(date_element.text.strip()) if date_element else 0
                print(f"[ALBUM] Extracted date: {date}")
                
                # Get link directly
                link_element = card_listing.select_one('a[href]')
                if not link_element:
                    print("[ALBUM] No link element found, skipping item")
                    continue
                    
                link = link_element.get("href")
                print(f"[ALBUM] Found link: {link}")
                
                if link.startswith("/"):
                    link = URL("https://" + scrape_item.url.host + link)
                    print(f"[ALBUM] Converted relative link to absolute: {link}")
                else:
                    link = URL(link)
                
                # Get the filename
                filename_element = card_listing.select_one('p[class*="truncate theName"]')
                if not filename_element:
                    print("[ALBUM] No filename element found, trying alt methods")
                    img = card_listing.select_one("img")
                    if img and img.get("alt"):
                        filename = img.get("alt")
                    else:
                        filename = link.parts[-1] if link.parts else "unknown"
                else:
                    filename = filename_element.text.strip()
                    
                print(f"[ALBUM] Extracted filename: {filename}")
                
                # Extract file extension
                try:
                    _, file_ext = await get_filename_and_ext(filename)
                except NoExtensionFailure:
                    print("[ALBUM] No extension in filename, trying to determine from context")
                    if "Video" in card_listing.text or card_listing.select_one('span[class*="type-Video"]'):
                        file_ext = ".mp4"
                    elif "Image" in card_listing.text or card_listing.select_one('span[class*="type-Image"]'):
                        file_ext = ".jpg"
                    else:
                        file_ext = ".mp4"  # Default to mp4
                        
                print(f"[ALBUM] Using file extension: {file_ext}")

                # Create a new scrape item to process the file link
                new_scrape_item = await self.create_scrape_item(scrape_item, link, "", True, album_id, date)
                print(f"[ALBUM] Created new scrape item for: {link}")
                
                # Check if this file should be skipped (was in previous results)
                if results and await self.check_album_results(link, results):
                    print(f"[ALBUM] Skipping previously processed link: {link}")
                    continue
                    
                # Queue this file for processing
                print(f"[ALBUM] Queuing file for processing: {link}")
                self.manager.task_group.create_task(self.run(new_scrape_item))
                
            except Exception as e:
                print(f"[ALBUM] Error processing item: {str(e)}")
                print(f"Error processing album item: {str(e)}")
                continue

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
                print(f"Could not find download link or video source for {scrape_item.url}")
                return

        href = link_container.get('href')
        if not href:
            print(f"Found link container but no href attribute for {scrape_item.url}")
            title_tag = soup.select_one('title')
            if title_tag and '.' in title_tag.text:
                try:
                    title_text = title_tag.text.strip()
                    if '|' in title_text:
                        possible_filename = title_text.split('|')[0].strip()
                        filename, ext = await get_filename_and_ext(possible_filename)
                    else:
                        filename, ext = await get_filename_and_ext(title_text.strip())
                    
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
                print(f"Could not find any usable source for {scrape_item.url}")
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
                                filename, ext = await get_filename_and_ext(title_text.strip())
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

        # First check if this is a video by examining the meta tags and title
        is_video = False
        
        # Check meta tag for video type
        meta_type = soup.select_one('meta[property="og:type"]')
        if meta_type and meta_type.get('content') == 'video':
            print("[OTHER] Detected video via meta og:type tag")
            is_video = True
        
        # Check for video in the title
        title_tag = soup.select_one('title')
        if title_tag:
            title_text = title_tag.text.strip().lower()
            print(f"[OTHER] Found title: {title_text}")
            if '.mp4' in title_text or '.webm' in title_text or '.mov' in title_text:
                print("[OTHER] Title indicates this is a video file")
                is_video = True
        
        # Check for video elements on the page
        video_element = soup.select_one('video')
        if video_element:
            print("[OTHER] Found video element on page")
            is_video = True
        
        # Check for download button with video-like URL
        download_link = soup.select_one('a[href*="get.bunkrr.su/file/"]')
        if download_link:
            print("[OTHER] Found download link to get.bunkrr.su")
            is_video = True
        
        if is_video:
            print("[OTHER] Processing as video file")
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
                        # Try to get from title
                        if title_tag:
                            title_text = title_tag.text.strip()
                            if ".mp4" in title_text:
                                parts = title_text.split("|")[0].strip() if "|" in title_text else title_text
                                filename = parts
                                ext = ".mp4"
                            else:
                                filename = scrape_item.url.parts[-1]
                                ext = ".mp4"
                        else:
                            filename = scrape_item.url.parts[-1]
                            ext = ".mp4"
                        print(f"[OTHER] Using title or URL part: {filename} with ext: {ext}")
                
                print(f"[OTHER] Calling handle_file with: link={video_url}, filename={filename}, ext={ext}")
                await self.handle_file(video_url, scrape_item, filename, ext)
                return
        
        # If we get here, check if it's an image file
        img_tags = soup.select('figure img[src*="bunkr.ru"]')
        if img_tags:
            print("[OTHER] Found image content")
            # Use the main image (not the blurred background one)
            for img in img_tags:
                if 'blur' not in img.get('class', '') and not img.get('src', '').endswith('thumbs/'):
                    image_url = URL(img.get('src'))
                    print(f"[OTHER] Found direct image URL: {image_url}")
                    
                    try:
                        filename, ext = await get_filename_and_ext(image_url.name)
                        print(f"[OTHER] Extracted filename from image URL: {filename}, ext: {ext}")
                    except NoExtensionFailure:
                        # Try to get filename from page title
                        title_tag = soup.select_one('h1')
                        if title_tag:
                            title_text = title_tag.text.strip()
                            print(f"[OTHER] Using page title for filename: {title_text}")
                            try:
                                filename, ext = await get_filename_and_ext(title_text)
                            except NoExtensionFailure:
                                # If no extension in title, get it from URL
                                filename = title_text
                                ext = ".webp" if ".webp" in title_text else ".jpg"
                                
                print(f"[OTHER] Downloading image with filename: {filename}{ext}")
                await self.handle_file(image_url, scrape_item, filename, ext)
                return
        
        # Check for meta image tags (often used for images)
        meta_image = soup.select_one('meta[property="og:image"]')
        if meta_image and meta_image.get('content') and not is_video:
            # Only use meta image for actual images, not video thumbnails
            image_url_str = meta_image.get('content')
            print(f"[OTHER] Found meta image: {image_url_str}")
            
            # Check if this is truly an image and not a video thumbnail
            if 'thumbs' in image_url_str and is_video:
                print("[OTHER] This appears to be a video thumbnail, not treating as image")
            else:
                # Convert thumbs URL to direct image URL if needed
                if 'thumbs' in image_url_str:
                    image_url_str = image_url_str.replace('/thumbs/', '/')
                    if image_url_str.endswith('.png'):
                        # Remove the .png suffix for thumbnails
                        image_url_str = image_url_str[:-4]
                        
                    # Add correct extension based on page title if possible
                    title_tag = soup.select_one('h1')
                    if title_tag:
                        title_text = title_tag.text.strip()
                        if '.webp' in title_text.lower():
                            image_url_str += '.webp'
                        elif '.jpg' in title_text.lower() or '.jpeg' in title_text.lower():
                            image_url_str += '.jpg'
                        else:
                            image_url_str += '.webp'  # Default extension for image
                    else:
                        image_url_str += '.webp'  # Default extension
                
                image_url = URL(image_url_str)
                print(f"[OTHER] Processed image URL: {image_url}")
                
                try:
                    filename, ext = await get_filename_and_ext(image_url.name)
                    print(f"[OTHER] Extracted filename from image URL: {filename}, ext: {ext}")
                except NoExtensionFailure:
                    # Try to get filename from page title
                    title_tag = soup.select_one('h1')
                    if title_tag:
                        title_text = title_tag.text.strip()
                        print(f"[OTHER] Using page title for filename: {title_text}")
                        try:
                            filename, ext = await get_filename_and_ext(title_text)
                        except NoExtensionFailure:
                            # If no extension in title, get it from URL
                            filename = title_text
                            ext = ".webp"  # Default to webp
                
                print(f"[OTHER] Downloading image with filename: {filename}{ext}")
                await self.handle_file(image_url, scrape_item, filename, ext)
                return

        # If we're still here, look for download links for any file type
        print("[OTHER] Looking for download link with specific selectors")
        
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
        
        if link_container:
            href = link_container.get('href')
            if href and href != '#':
                print(f"[OTHER] Found href: {href}")
                link = URL(href)
                print(f"[OTHER] Created URL object: {link}")
                
                # Check if this is a get.bunkrr.su link (used for downloads)
                if link.host and 'get.bunkr' in link.host:
                    print("[OTHER] Link is to get.bunkrr.su, calling reinforced_link")
                    link = await self.reinforced_link(link)
                    
                    if not link:
                        # Try to derive link from file ID if reinforced_link fails
                        file_id = href.split('/')[-1]
                        print(f"[OTHER] Reinforced link failed, using file_id: {file_id}")
                        
                        # Get title to determine if it's a video or image
                        title_tag = soup.select_one('h1') or soup.select_one('title')
                        if title_tag:
                            title_text = title_tag.text.strip().lower()
                            if '.mp4' in title_text or '.webm' in title_text or 'video' in title_text:
                                print("[OTHER] Title suggests this is a video file")
                                # Try CDN domains for video
                                cdn_domains = [
                                    "nachos.bunkr.ru", "wings.bunkr.ru", "wiener.bunkr.ru",
                                    "ramen.bunkr.ru", "pizza.bunkr.ru", "burger.bunkr.ru", 
                                    "fries.bunkr.ru", "meatballs.bunkr.ru", "milkshake.bunkr.ru",
                                    "kebab.bunkr.ru", "taquito.bunkr.ru", "soup.bunkr.ru"
                                ]
                                
                                for cdn in cdn_domains:
                                    link = URL(f"https://{cdn}/{file_id}.mp4")
                                    print(f"[OTHER] Trying CDN URL for video: {link}")
                                    break
                            else:
                                # Probably an image
                                print("[OTHER] Title suggests this is an image file")
                                link = URL(f"https://i-wings.bunkr.ru/{file_id}.webp")
                                print(f"[OTHER] Using image URL: {link}")
                        
                    if filename and ext:
                        print(f"[OTHER] Using existing filename: {filename}, ext: {ext}")
                    else:
                        try:
                            filename, ext = await get_filename_and_ext(link.name)
                            print(f"[OTHER] Got filename from link: {filename}, ext: {ext}")
                        except (NoExtensionFailure, AttributeError):
                            # Try to determine type from title
                            title_tag = soup.select_one('h1.text-subs') or soup.select_one('title')
                            if title_tag:
                                title_text = title_tag.text.strip().lower()
                                if '.mp4' in title_text or '.webm' in title_text:
                                    filename = title_tag.text.strip().split('|')[0].strip() if '|' in title_tag.text else title_tag.text.strip()
                                    ext = ".mp4"
                                elif '.jpg' in title_text or '.jpeg' in title_text or '.webp' in title_text or '.png' in title_text:
                                    filename = title_tag.text.strip().split('|')[0].strip() if '|' in title_tag.text else title_tag.text.strip()
                                    ext = ".webp" if ".webp" in title_text else ".jpg"
                                else:
                                    filename = scrape_item.url.parts[-1]
                                    ext = ".mp4"  # Default to video if can't determine
                                print(f"[OTHER] Determined from title: {filename} with ext: {ext}")
                            else:
                                filename = scrape_item.url.parts[-1]
                                ext = ".mp4"
                                print(f"[OTHER] Using URL part: {filename} with default ext: {ext}")
                        
                    if link:
                        print(f"[OTHER] Calling handle_file with: link={link}, filename={filename}, ext={ext}")
                        await self.handle_file(link, scrape_item, filename, ext)
                        return
        
        # Last resort - try to extract video information directly
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
        
        # Ultimate fallback - try to construct a CDN URL from file ID in URL
        if len(scrape_item.url.parts) > 1:
            file_id = scrape_item.url.parts[-1]
            print(f"[OTHER] Last resort - using file ID from URL: {file_id}")
            
            # Try to determine if it's a video or image from available clues
            is_likely_video = False
            title_tag = soup.select_one('h1.text-subs') or soup.select_one('title')
            if title_tag:
                title_text = title_tag.text.strip().lower()
                if '.mp4' in title_text or '.webm' in title_text or 'video' in title_text:
                    is_likely_video = True
            
            if video_element or is_video:
                is_likely_video = True
            
            if not filename or not ext:
                if title_tag:
                    filename = title_tag.text.strip().split('|')[0].strip() if '|' in title_tag.text else title_tag.text.strip()
                else:
                    filename = file_id
                
                ext = ".mp4" if is_likely_video else ".webp"
            
            if is_likely_video:
                # Try standard CDN domain for video
                video_url = URL(f"https://wings.bunkr.ru/{file_id}{ext}")
            else:
                # Try i- prefixed domain for image
                video_url = URL(f"https://i-wings.bunkr.ru/{file_id}{ext}")
            
            print(f"[OTHER] Constructed potential URL: {video_url}")
            await self.handle_file(video_url, scrape_item, filename, ext)
            return
            
        print("[OTHER] Failed to find any usable video source")
        print(f"Could not find any usable source for {scrape_item.url}")
        return

    @error_handling_wrapper
    async def extract_video_url_from_scripts(self, soup, url) -> Optional[URL]:
        """Extract video URL directly from script tags, prioritizing gigachad-cdn URLs."""
        print("[EXTRACT_SCRIPT] Searching for video URLs in script tags")
        
        # Extract raw HTML content for regex searching
        content = str(soup)
        
        # First priority: Check for direct gigachad-cdn URL pattern in the entire HTML
        gigachad_match = re.search(r'(https?://mlk-bk\.cdn\.gigachad-cdn\.ru/[^"\']+?\.mp4)', content)
        if gigachad_match:
            video_url = URL(gigachad_match.group(1))
            print(f"[EXTRACT_SCRIPT] Found gigachad-cdn URL in HTML content: {video_url}")
            return video_url
    
        
        # Third priority: Extract from source element in the raw HTML
        source_match = re.search(r'<source src="([^"]+?\.mp4)"', content)
        if source_match:
            video_url = URL(source_match.group(1))
            print(f"[EXTRACT_SCRIPT] Found source element in HTML content: {video_url}")
            return video_url
        
        # Fourth priority: Check scripts for direct video URLs
        for script in soup.select('script'):
            if not script.string:
                continue
                
            # Check for gigachad-cdn pattern in scripts
            if 'gigachad-cdn.ru' in script.string:
                print("[EXTRACT_SCRIPT] Found script with gigachad-cdn reference")
                matches = re.findall(r'(https?://[^"\']+?gigachad-cdn\.ru/[^"\']+?\.mp4)', script.string)
                if matches:
                    video_url = URL(matches[0])
                    print(f"[EXTRACT_SCRIPT] Extracted gigachad-cdn video URL: {video_url}")
                    return video_url
            
            # Look for any mp4 URL in scripts
            if '.mp4' in script.string:
                print("[EXTRACT_SCRIPT] Found script with mp4 reference")
                matches = re.findall(r'(https?://[^"\']+?\.mp4)', script.string)
                if matches:
                    video_url = URL(matches[0])
                    print(f"[EXTRACT_SCRIPT] Extracted generic video URL: {video_url}")
                    return video_url
        
        # Fifth priority: Extract from meta tags and try to construct the URL
        meta_image = soup.select_one('meta[property="og:image"]')
        if meta_image and meta_image.get('content'):
            image_url = meta_image.get('content')
            print(f"[EXTRACT_SCRIPT] Found meta image: {image_url}")
            
            # Try to extract identifier from meta image URL
            id_match = re.search(r'([^/]+)-([a-zA-Z0-9]+)\.png$', image_url)
            if id_match:
                base_name = id_match.group(1)
                identifier = id_match.group(2)
                
                # Prioritize gigachad-cdn URL
                video_url = URL(f"https://mlk-bk.cdn.gigachad-cdn.ru/{base_name}-{identifier}.mp4")
                print(f"[EXTRACT_SCRIPT] Constructed gigachad-cdn URL from meta image: {video_url}")
                return video_url
        
        # Sixth priority: Look for video elements with data-poster
        video_tags = soup.select('video')
        for video in video_tags:
            # Try data-poster attribute first
            poster = video.get('data-poster') or video.get('poster')
            if poster and 'thumbs' in poster:
                print(f"[EXTRACT_SCRIPT] Found poster: {poster}")
                try:
                    # Extract identifier from poster
                    id_match = re.search(r'([^/]+)-([a-zA-Z0-9]+)\.png$', poster)
                    if id_match:
                        base_name = id_match.group(1)
                        identifier = id_match.group(2)
                        
                        # Try constructing gigachad-cdn URL directly
                        video_url = URL(f"https://mlk-bk.cdn.gigachad-cdn.ru/{base_name}-{identifier}.mp4")
                        print(f"[EXTRACT_SCRIPT] Constructed gigachad-cdn URL from poster: {video_url}")
                        return video_url
                    else:
                        # Alternative transformation
                        video_url_str = poster.replace('thumbs/', '').replace('_grid.png', '.mp4')
                        if '://' in video_url_str:  # Ensure it's a complete URL
                            video_url = URL(video_url_str)
                            print(f"[EXTRACT_SCRIPT] Transformed poster to video URL: {video_url}")
                            return video_url
                except Exception as e:
                    print(f"[EXTRACT_SCRIPT] Error constructing URL from poster: {e}")
        
        print("[EXTRACT_SCRIPT] No video URLs found in scripts or HTML")
        return None

    @error_handling_wrapper
    async def extract_video_from_get_page(self, soup, url) -> Tuple[Optional[URL], Optional[str], Optional[str]]:
        print(f"[EXTRACT_VIDEO] Starting extraction for URL: {url}")
        
        # First try to extract from scripts - most reliable for dynamically loaded content
        script_video_url = await self.extract_video_url_from_scripts(soup, url)
        if script_video_url:
            print(f"[EXTRACT_VIDEO] Using video URL from scripts: {script_video_url}")
            
            # Get filename and extension from title or default
            title_tag = soup.select_one('h1') or soup.select_one('title')
            if title_tag:
                title_text = title_tag.text.strip()
                try:
                    filename, ext = await get_filename_and_ext(title_text)
                    print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                except NoExtensionFailure:
                    # If no extension in title, use default
                    filename = title_text.split('|')[0].strip() if '|' in title_text else title_text
                    ext = ".mp4"  # Default for video
                    print(f"[EXTRACT_VIDEO] Using title with default extension: {filename}, ext: {ext}")
            else:
                # No title available, try to get from URL or use default
                try:
                    filename, ext = await get_filename_and_ext(script_video_url.name)
                    print(f"[EXTRACT_VIDEO] Extracted filename from URL: {filename}, ext: {ext}")
                except NoExtensionFailure:
                    filename = url.parts[-1] if url.parts and len(url.parts) > 1 else "video"
                    ext = ".mp4"
                    print(f"[EXTRACT_VIDEO] Using URL part or default as filename: {filename}, ext: {ext}")
            
            return script_video_url, filename, ext
        
        # First, try to directly extract video source from video element
        video_element = soup.select_one('video')
        if video_element:
            source_element = video_element.select_one('source')
            if source_element and source_element.get('src'):
                video_src = source_element.get('src')
                print(f"[EXTRACT_VIDEO] Found direct video source URL: {video_src}")
                return URL(video_src), None, None
        
        # Check if we're dealing with an image by examining the title
        title_tag = soup.select_one('h1')
        if title_tag:
            title_text = title_tag.text.strip()
            print(f"[EXTRACT_VIDEO] Found title: {title_text}")
            
            # Check if it's an image based on extension in title
            if any(ext in title_text.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif']):
                print("[EXTRACT_VIDEO] Title indicates this is an image file")
                
                # Look for image URLs in the page
                img_tags = soup.select('figure img[src*="bunkr.ru"]')
                if img_tags:
                    # Use the main image (not the blurred background one)
                    for img in img_tags:
                        if 'blur' not in img.get('class', '') and not img.get('src', '').endswith('thumbs/'):
                            image_url = URL(img.get('src'))
                            print(f"[EXTRACT_VIDEO] Found direct image URL: {image_url}")
                            
                            try:
                                filename, ext = await get_filename_and_ext(image_url.name)
                                print(f"[EXTRACT_VIDEO] Extracted filename from URL: {filename}, ext: {ext}")
                                return image_url, filename, ext
                            except NoExtensionFailure:
                                # Use title as filename
                                try:
                                    filename, ext = await get_filename_and_ext(title_text)
                                    print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                                    return image_url, filename, ext
                                except NoExtensionFailure:
                                    # Determine extension from title text or URL
                                    if '.webp' in title_text.lower():
                                        ext = '.webp'
                                    elif '.jpg' in title_text.lower() or '.jpeg' in title_text.lower():
                                        ext = '.jpg'
                                    elif '.png' in title_text.lower():
                                        ext = '.png'
                                    elif '.gif' in title_text.lower():
                                        ext = '.gif'
                                    else:
                                        ext = '.webp'  # Default to webp
                                    
                                    filename = title_text
                                    print(f"[EXTRACT_VIDEO] Using title with derived extension: {filename}, ext: {ext}")
                                    return image_url, filename, ext
                
                # Try finding "enlarge image" link
                enlarge_link = soup.select_one('a[href*="bunkr.ru"][href*=".webp"], a[href*="bunkr.ru"][href*=".jpg"], a[href*="bunkr.ru"][href*=".png"]')
                if enlarge_link:
                    image_url = URL(enlarge_link.get('href'))
                    print(f"[EXTRACT_VIDEO] Found direct image URL from enlarge link: {image_url}")
                    
                    try:
                        filename, ext = await get_filename_and_ext(image_url.name)
                        print(f"[EXTRACT_VIDEO] Extracted filename from URL: {filename}, ext: {ext}")
                        return image_url, filename, ext
                    except NoExtensionFailure:
                        # Use title as filename
                        try:
                            filename, ext = await get_filename_and_ext(title_text)
                            print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                            return image_url, filename, ext
                        except NoExtensionFailure:
                            # If no extension in title, derive from title text
                            if '.webp' in title_text.lower():
                                ext = '.webp'
                            elif '.jpg' in title_text.lower() or '.jpeg' in title_text.lower():
                                ext = '.jpg'
                            elif '.png' in title_text.lower():
                                ext = '.png'
                            elif '.gif' in title_text.lower():
                                ext = '.gif'
                            else:
                                ext = '.webp'  # Default to webp
                            
                            filename = title_text
                            print(f"[EXTRACT_VIDEO] Using title with derived extension: {filename}, ext: {ext}")
                            return image_url, filename, ext
                
                # Try meta image
                meta_image = soup.select_one('meta[property="og:image"]')
                if meta_image and meta_image.get('content'):
                    image_url_str = meta_image.get('content')
                    print(f"[EXTRACT_VIDEO] Found meta image: {image_url_str}")
                    
                    # Convert thumbs URL to direct image URL
                    if 'thumbs' in image_url_str:
                        image_url_str = image_url_str.replace('/thumbs/', '/')
                        if image_url_str.endswith('.png'):
                            image_url_str = image_url_str[:-4]
                        
                        # Add extension based on title
                        if '.webp' in title_text.lower():
                            image_url_str += '.webp'
                        elif '.jpg' in title_text.lower() or '.jpeg' in title_text.lower():
                            image_url_str += '.jpg'
                        elif '.png' in title_text.lower():
                            image_url_str += '.png'
                        elif '.gif' in title_text.lower():
                            image_url_str += '.gif'
                        else:
                            image_url_str += '.webp'  # Default
                    
                    image_url = URL(image_url_str)
                    print(f"[EXTRACT_VIDEO] Constructed image URL: {image_url}")
                    
                    try:
                        filename, ext = await get_filename_and_ext(title_text)
                        print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                        return image_url, filename, ext
                    except NoExtensionFailure:
                        # If no extension in title, derive from title text
                        if '.webp' in title_text.lower():
                            ext = '.webp'
                        elif '.jpg' in title_text.lower() or '.jpeg' in title_text.lower():
                            ext = '.jpg'
                        elif '.png' in title_text.lower():
                            ext = '.png'
                        elif '.gif' in title_text.lower():
                            ext = '.gif'
                        else:
                            ext = '.webp'  # Default to webp
                        
                        filename = title_text
                        print(f"[EXTRACT_VIDEO] Using title with derived extension: {filename}, ext: {ext}")
                        return image_url, filename, ext
        
        # If not an image or no image found, continue with video extraction
        print("[EXTRACT_VIDEO] Looking for video source first")
        
        # Try to find video player and direct source
        player_element = soup.select_one('div.plyr__video-wrapper')
        if player_element:
            print("[EXTRACT_VIDEO] Found video player element")
            video_element = player_element.select_one('video')
            if video_element:
                print("[EXTRACT_VIDEO] Found video element within player")
                source_element = video_element.select_one('source')
                if source_element and source_element.get('src'):
                    print("[EXTRACT_VIDEO] Found source element with src")
                    video_url = URL(source_element.get('src'))
                    print(f"[EXTRACT_VIDEO] Extracted video URL from source: {video_url}")
                    
                    # Try to extract filename from the URL
                    try:
                        filename, ext = await get_filename_and_ext(video_url.name)
                        print(f"[EXTRACT_VIDEO] Extracted filename from URL: {filename}, ext: {ext}")
                    except NoExtensionFailure:
                        # Try to get filename from title
                        if title_tag:
                            try:
                                filename, ext = await get_filename_and_ext(title_tag.text.strip())
                                print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                            except NoExtensionFailure:
                                # If no extension in title, use default
                                filename = title_tag.text.strip()
                                ext = ".mp4"  # Default for video
                                print(f"[EXTRACT_VIDEO] Using title with default extension: {filename}, ext: {ext}")
                        else:
                            # No title available, use the URL part as filename
                            if url.parts and len(url.parts) > 1:
                                filename = url.parts[-1]
                            else:
                                filename = "video"
                            ext = ".mp4"
                            print(f"[EXTRACT_VIDEO] Using URL part or default as filename: {filename}, ext: {ext}")
                    
                    return video_url, filename, ext
        
        # Extract title and try to get video file from scripts
        if title_tag:
            try:
                filename, ext = await get_filename_and_ext(title_tag.text.strip())
                print(f"[EXTRACT_VIDEO] Extracted filename from title: {filename}, ext: {ext}")
            except NoExtensionFailure:
                # If no extension in title, check if there's any hint in the text
                title_text = title_tag.text.strip()
                if '.mp4' in title_text.lower():
                    ext = '.mp4'
                elif '.webm' in title_text.lower():
                    ext = '.webm'
                else:
                    ext = '.mp4'  # Default for videos
                
                filename = title_text
                print(f"[EXTRACT_VIDEO] Using title with derived extension: {filename}, ext: {ext}")
            
            # Try to find direct video source in the HTML content
            video_element = soup.select_one('video')
            if video_element and video_element.select_one('source[src]'):
                video_src = video_element.select_one('source[src]').get('src')
                if video_src:
                    print(f"[EXTRACT_VIDEO] Found direct video source in HTML: {video_src}")
                    video_url = URL(video_src)
                    return video_url, filename, ext
                    
            # Try to find video URL in any script tag with the file or CDN information
            for script in soup.select('script'):
                if script.string:
                    # Look for direct video URLs with the specific format
                    if 'gigachad-cdn.ru' in script.string:
                        print("[EXTRACT_VIDEO] Found script with gigachad-cdn reference")
                        # Try to extract the full URL with regex
                        matches = re.findall(r'https?://[^"\']+\.mp4', script.string)
                        if matches:
                            video_url = URL(matches[0])
                            print(f"[EXTRACT_VIDEO] Extracted gigachad-cdn video URL from script: {video_url}")
                            return video_url, filename, ext
            
            # Try to find UUID in meta image tag first (preferred over file ID)
            uuid = None
            meta_image = soup.select_one('meta[property="og:image"]')
            if meta_image and meta_image.get('content'):
                image_url = meta_image.get('content')
                print(f"[EXTRACT_VIDEO] Found meta image: {image_url}")
                
                # Extract UUID from meta image URL
                uuid_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', image_url)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    print(f"[EXTRACT_VIDEO] Extracted UUID from meta image: {uuid}")
                else:
                    # Try another pattern (without dashes)
                    uuid_match = re.search(r'/thumbs/([a-f0-9]{32})', image_url)
                    if uuid_match:
                        uuid = uuid_match.group(1)
                        print(f"[EXTRACT_VIDEO] Extracted UUID (no dashes) from meta image: {uuid}")
                    else:
                        # Try to get any filename without extension
                        parts = image_url.split('/')
                        if parts and len(parts) > 0:
                            potential_uuid = parts[-1].split('.')[0]
                            if len(potential_uuid) > 8:  # Minimum length for a reasonable ID
                                uuid = potential_uuid
                                print(f"[EXTRACT_VIDEO] Extracted potential ID from meta image: {uuid}")
            
            # If UUID was found in meta image, use it to construct the video URL
            if uuid:
                # Extract CDN domain from meta image
                cdn_domain = None
                if meta_image and meta_image.get('content'):
                    image_url = meta_image.get('content')
                    try:
                        parsed_url = URL(image_url)
                        if parsed_url.host and 'bunkr' in parsed_url.host:
                            # Get the CDN domain without the i- prefix for videos
                            cdn_domain = parsed_url.host
                            if cdn_domain.startswith('i-'):
                                cdn_domain = cdn_domain.replace('i-', '')
                            print(f"[EXTRACT_VIDEO] Extracted CDN domain from meta image: {cdn_domain}")
                    except Exception as e:
                        print(f"[EXTRACT_VIDEO] Error extracting CDN domain: {e}")
                
                if cdn_domain:
                    # Construct video URL using the CDN domain and UUID (not numeric file ID)
                    video_url = URL(f"https://{cdn_domain}/{uuid}.mp4")
                    print(f"[EXTRACT_VIDEO] Constructed video URL with UUID: {video_url}")
                    return video_url, filename, ext
                else:
                    # Fallback to standard CDNs with UUID
                    cdn_domains = [
                        "nachos.bunkr.ru", "wings.bunkr.ru", "wiener.bunkr.ru",
                        "ramen.bunkr.ru", "pizza.bunkr.ru", "burger.bunkr.ru", 
                        "fries.bunkr.ru", "meatballs.bunkr.ru", "milkshake.bunkr.ru",
                        "kebab.bunkr.ru", "taquito.bunkr.ru", "soup.bunkr.ru",
                        "mlk-bk.cdn.gigachad-cdn.ru"
                    ]
                    
                    # Try each CDN with UUID
                    for cdn in cdn_domains:
                        video_url = URL(f"https://{cdn}/{uuid}.mp4")
                        print(f"[EXTRACT_VIDEO] Trying CDN URL with UUID: {video_url}")
                        return video_url, filename, ext
        
        # As fallback, try to find file_id in various places
        file_id = None
        
        # Try to get file_id from data attributes
        file_tracker = soup.select_one('#fileTracker[data-file-id]')
        if file_tracker:
            file_id = file_tracker.get('data-file-id')
            print(f"[EXTRACT_VIDEO] Found file_id in fileTracker element: {file_id}")
        
        # Try to get file_id from download link
        if not file_id:
            download_link = soup.select_one('a[href*="get.bunkrr.su/file/"]')
            if download_link and download_link.get('href'):
                href = download_link.get('href')
                file_id = href.split('/')[-1]
                print(f"[EXTRACT_VIDEO] Found file_id in download link: {file_id}")
        
        # Try to get file_id from script tags
        if not file_id:
            script_tags = soup.select('script[data-file-id]')
            if script_tags:
                file_id = script_tags[0].get('data-file-id')
                print(f"[EXTRACT_VIDEO] Found file_id in script tag: {file_id}")
        
        # Try to extract file_id from the URL
        if not file_id and url.parts and len(url.parts) > 1:
            file_id = url.parts[-1]
            print(f"[EXTRACT_VIDEO] Using URL part as file_id: {file_id}")
        
        # Only use numeric file_id if we couldn't find a UUID (which is preferred)
        if file_id:
            # Try to extract CDN domain from meta image
            cdn_domain = None
            meta_image = soup.select_one('meta[property="og:image"]')
            if meta_image and meta_image.get('content'):
                image_url = meta_image.get('content')
                print(f"[EXTRACT_VIDEO] Found meta image: {image_url}")
                
                try:
                    parsed_url = URL(image_url)
                    if parsed_url.host and 'bunkr' in parsed_url.host:
                        # Get the CDN domain without the i- prefix for videos
                        cdn_domain = parsed_url.host
                        if cdn_domain.startswith('i-'):
                            cdn_domain = cdn_domain.replace('i-', '')
                        print(f"[EXTRACT_VIDEO] Extracted CDN domain from meta image: {cdn_domain}")
                except Exception as e:
                    print(f"[EXTRACT_VIDEO] Error extracting CDN domain: {e}")
            
            if cdn_domain:
                # Construct video URL using the CDN domain
                video_url = URL(f"https://{cdn_domain}/{file_id}.mp4")
                print(f"[EXTRACT_VIDEO] Constructed video URL with file_id: {video_url}")
                return video_url, filename, ext
            else:
                # Fallback to standard CDNs
                cdn_domains = [
                    "nachos.bunkr.ru", "wings.bunkr.ru", "wiener.bunkr.ru",
                    "ramen.bunkr.ru", "pizza.bunkr.ru", "burger.bunkr.ru", 
                    "fries.bunkr.ru", "meatballs.bunkr.ru", "milkshake.bunkr.ru",
                    "kebab.bunkr.ru", "taquito.bunkr.ru", "soup.bunkr.ru",
                    "mlk-bk.cdn.gigachad-cdn.ru"
                ]
                
                # Try each CDN with file_id
                for cdn in cdn_domains:
                    video_url = URL(f"https://{cdn}/{file_id}.mp4")
                    print(f"[EXTRACT_VIDEO] Trying CDN URL with file_id: {video_url}")
                    return video_url, filename, ext
        
        # Last resort - look for video element
        video_element = soup.select_one('video')
        if video_element:
            print("[EXTRACT_VIDEO] Found video element")
            source_element = video_element.select_one('source')
            if source_element and source_element.get('src'):
                print("[EXTRACT_VIDEO] Found source element with src")
                video_url = URL(source_element.get('src'))
                print(f"[EXTRACT_VIDEO] Extracted video URL from source: {video_url}")
                
                try:
                    filename, ext = await get_filename_and_ext(video_url.name)
                    print(f"[EXTRACT_VIDEO] Extracted filename from URL: {filename}, ext: {ext}")
                except NoExtensionFailure:
                    # Try to get filename from title
                    if title_tag:
                        try:
                            filename, ext = await get_filename_and_ext(title_tag.text.strip())
                            print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                        except NoExtensionFailure:
                            # If no extension in title, use default
                            filename = title_tag.text.strip()
                            ext = ".mp4"  # Default for video
                            print(f"[EXTRACT_VIDEO] Using title with default extension: {filename}, ext: {ext}")
                    else:
                        # No title available, use the URL part as filename
                        if url.parts and len(url.parts) > 1:
                            filename = url.parts[-1]
                        else:
                            filename = "video"
                        ext = ".mp4"
                        print(f"[EXTRACT_VIDEO] Using URL part or default as filename: {filename}, ext: {ext}")
                
                return video_url, filename, ext
            
            # Try to derive video URL from poster
            poster = video_element.get('poster')
            if poster:
                print(f"[EXTRACT_VIDEO] Found poster: {poster}")
                if 'thumbs' in poster:
                    try:
                        # Extract UUID from poster URL
                        uuid_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', poster)
                        if uuid_match:
                            uuid = uuid_match.group(1)
                            print(f"[EXTRACT_VIDEO] Extracted UUID from poster: {uuid}")
                            
                            # Extract CDN domain from poster
                            cdn_domain = None
                            try:
                                parsed_url = URL(poster)
                                if parsed_url.host and 'bunkr' in parsed_url.host:
                                    cdn_domain = parsed_url.host
                                    if cdn_domain.startswith('i-'):
                                        cdn_domain = cdn_domain.replace('i-', '')
                                    print(f"[EXTRACT_VIDEO] Extracted CDN domain from poster: {cdn_domain}")
                            except Exception as e:
                                print(f"[EXTRACT_VIDEO] Error extracting CDN domain from poster: {e}")
                            
                            if cdn_domain:
                                video_url = URL(f"https://{cdn_domain}/{uuid}.mp4")
                                print(f"[EXTRACT_VIDEO] Constructed video URL from poster UUID: {video_url}")
                                
                                if title_tag:
                                    try:
                                        filename, ext = await get_filename_and_ext(title_tag.text.strip())
                                        print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                                    except NoExtensionFailure:
                                        filename = title_tag.text.strip()
                                        ext = ".mp4"
                                        print(f"[EXTRACT_VIDEO] Using title with default extension: {filename}, ext: {ext}")
                                else:
                                    filename = uuid
                                    ext = ".mp4"
                                    print(f"[EXTRACT_VIDEO] Using UUID as filename: {filename}, ext: {ext}")
                                
                                return video_url, filename, ext
                        else:
                            # Try to extract file ID or any identifier from poster
                            poster_parts = poster.split('/')
                            if poster_parts:
                                potential_id = poster_parts[-1].split('.')[0]
                                print(f"[EXTRACT_VIDEO] Found potential ID in poster: {potential_id}")
                                
                                # Transform poster URL to video URL
                                video_url_str = poster.replace('thumbs/', '').replace('.png', '.mp4')
                                video_url = URL(video_url_str)
                                print(f"[EXTRACT_VIDEO] Transformed poster to video URL: {video_url}")
                                
                                if title_tag:
                                    try:
                                        filename, ext = await get_filename_and_ext(title_tag.text.strip())
                                        print(f"[EXTRACT_VIDEO] Using title as filename: {filename}, ext: {ext}")
                                    except NoExtensionFailure:
                                        # If no extension in title, use default
                                        filename = title_tag.text.strip()
                                        ext = ".mp4"  # Default for video
                                        print(f"[EXTRACT_VIDEO] Using title with default extension: {filename}, ext: {ext}")
                                else:
                                    # No title available, use the URL part as filename
                                    if url.parts and len(url.parts) > 1:
                                        filename = url.parts[-1]
                                    else:
                                        filename = "video"
                                    ext = ".mp4"
                                    print(f"[EXTRACT_VIDEO] Using URL part or default as filename: {filename}, ext: {ext}")
                                
                                return video_url, filename, ext
                    except Exception as e:
                        print(f"[EXTRACT_VIDEO] Error transforming poster URL: {e}")
        
        print("[EXTRACT_VIDEO] No suitable video or image source found")
        return None, None, None

    @error_handling_wrapper
    async def reinforced_link(self, url: URL) -> URL:
        print(f"[REINFORCED_LINK] Starting with URL: {url}")
        
        # Check if the URL is likely for an image 
        file_id = url.parts[-1] if url.parts else None
        if file_id:
            print(f"[REINFORCED_LINK] Extracted file ID: {file_id}")
        
        async with self.request_limiter:
            print("[REINFORCED_LINK] Getting BS4 for URL")
            soup = await self.client.get_BS4(self.domain, url)
            print("[REINFORCED_LINK] Got BS4 successfully")
        
        # First, try to extract from scripts - most reliable for dynamically loaded content
        script_video_url = await self.extract_video_url_from_scripts(soup, url)
        if script_video_url:
            print(f"[REINFORCED_LINK] Using video URL from scripts: {script_video_url}")
            return script_video_url
        
        # First, try to directly extract video source
        video_element = soup.select_one('video')
        if video_element:
            source_element = video_element.select_one('source[src]')
            if source_element and source_element.get('src'):
                video_src = source_element.get('src')
                print(f"[REINFORCED_LINK] Found direct video source URL: {video_src}")
                return URL(video_src)
        
        # Try to find video URL in any script tag
        for script in soup.select('script'):
            if script.string:
                # Look for specific CDN domains
                if 'gigachad-cdn.ru' in script.string:
                    print("[REINFORCED_LINK] Found script with gigachad-cdn reference")
                    # Try to extract the full URL with regex
                    matches = re.findall(r'https?://[^"\']+\.mp4', script.string)
                    if matches:
                        video_url = URL(matches[0])
                        print(f"[REINFORCED_LINK] Extracted gigachad-cdn video URL from script: {video_url}")
                        return video_url
        
        # First check if this is an image file by examining the title
        title_tag = soup.select_one('h1')
        if title_tag:
            title_text = title_tag.text.strip().lower()
            print(f"[REINFORCED_LINK] Found title: {title_text}")
            
            # Check if it's an image based on extension in title
            if any(ext in title_text for ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif']):
                print("[REINFORCED_LINK] Title indicates this is an image file")
                
                # First try to find direct image links
                for img in soup.select('figure img'):
                    if not img.get('src', '').endswith('thumbs/'):
                        image_url = URL(img.get('src'))
                        print(f"[REINFORCED_LINK] Found direct image URL: {image_url}")
                        return image_url
                
                # Try finding "enlarge image" link
                enlarge_link = soup.select_one('a[href*="bunkr.ru"][href*=".webp"], a[href*="bunkr.ru"][href*=".jpg"], a[href*="bunkr.ru"][href*=".png"]')
                if enlarge_link:
                    image_url = URL(enlarge_link.get('href'))
                    print(f"[REINFORCED_LINK] Found direct image URL from enlarge link: {image_url}")
                    return image_url
                
                # Try meta image
                meta_image = soup.select_one('meta[property="og:image"]')
                if meta_image and meta_image.get('content'):
                    image_url_str = meta_image.get('content')
                    print(f"[REINFORCED_LINK] Found meta image: {image_url_str}")
                    
                    # Convert thumbs URL to direct image URL
                    if 'thumbs' in image_url_str:
                        image_url_str = image_url_str.replace('/thumbs/', '/')
                        if image_url_str.endswith('.png'):
                            image_url_str = image_url_str[:-4]
                        
                        # Add extension based on title
                        if '.webp' in title_text:
                            image_url_str += '.webp'
                        elif '.jpg' in title_text or '.jpeg' in title_text:
                            image_url_str += '.jpg'
                        elif '.png' in title_text:
                            image_url_str += '.png'
                        elif '.gif' in title_text:
                            image_url_str += '.gif'
                        else:
                            image_url_str += '.webp'  # Default
                    
                    image_url = URL(image_url_str)
                    print(f"[REINFORCED_LINK] Constructed image URL: {image_url}")
                    return image_url
        
        # If we get here, it's not an image, so continue with video extraction
        if url.host and 'get.bunkr' in url.host:
            print("[REINFORCED_LINK] Using specialized get.bunkrr.su extraction")
            video_url, _, _ = await self.extract_video_from_get_page(soup, url)
            if video_url:
                print(f"[REINFORCED_LINK] Successfully extracted video URL: {video_url}")
                return video_url
            
            # Fallback extraction for get.bunkrr.su pages
            file_id = url.parts[-1] if url.parts else None
            print(f"[REINFORCED_LINK] Extracted file ID from URL: {file_id}")
            
            if file_id:
                # Try to extract meta image to get the CDN domain
                meta_image = soup.select_one('meta[property="og:image"]')
                if meta_image and meta_image.get('content'):
                    image_url = meta_image.get('content')
                    print(f"[REINFORCED_LINK] Found meta image: {image_url}")
                    
                    # Try to extract CDN domain from meta image
                    cdn_domain = None
                    try:
                        parsed_url = URL(image_url)
                        if parsed_url.host and 'bunkr' in parsed_url.host:
                            cdn_domain = parsed_url.host
                            if cdn_domain.startswith('i-'):
                                cdn_domain = cdn_domain.replace('i-', '')
                            print(f"[REINFORCED_LINK] Extracted CDN domain from meta image: {cdn_domain}")
                            
                            # Construct video URL using the CDN domain
                            video_url = URL(f"https://{cdn_domain}/{file_id}.mp4")
                            print(f"[REINFORCED_LINK] Constructed video URL: {video_url}")
                            return video_url
                    except Exception as e:
                        print(f"[REINFORCED_LINK] Error extracting CDN domain: {e}")
                
                # Fallback to standard CDNs
                cdn_domains = [
                    "nachos.bunkr.ru", "wings.bunkr.ru", "wiener.bunkr.ru",
                    "ramen.bunkr.ru", "pizza.bunkr.ru", "burger.bunkr.ru", 
                    "fries.bunkr.ru", "meatballs.bunkr.ru", "milkshake.bunkr.ru",
                    "kebab.bunkr.ru", "taquito.bunkr.ru", "soup.bunkr.ru",
                    "mlk-bk.cdn.gigachad-cdn.ru"
                ]
                
                # Try each CDN
                for cdn in cdn_domains:
                    video_url = URL(f"https://{cdn}/{file_id}.mp4")
                    print(f"[REINFORCED_LINK] Trying CDN URL: {video_url}")
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
                
                # Try to extract download link
                download_link = soup.select_one('a[href*="get.bunkrr.su/file/"]')
                if download_link and download_link.get('href'):
                    download_url = URL(download_link.get('href'))
                    print(f"[REINFORCED_LINK] Found download link: {download_url}")
                    if download_url.parts and len(download_url.parts) > 1:
                        file_id = download_url.parts[-1]
                        print(f"[REINFORCED_LINK] Extracted file ID from download link: {file_id}")
                        
                        # Try to extract meta image to get the CDN domain
                        meta_image = soup.select_one('meta[property="og:image"]')
                        cdn_domain = None
                        if meta_image and meta_image.get('content'):
                            image_url = meta_image.get('content')
                            print(f"[REINFORCED_LINK] Found meta image: {image_url}")
                            
                            # Try to extract CDN domain from meta image
                            try:
                                parsed_url = URL(image_url)
                                if parsed_url.host and 'bunkr' in parsed_url.host:
                                    cdn_domain = parsed_url.host
                                    if cdn_domain.startswith('i-'):
                                        cdn_domain = cdn_domain.replace('i-', '')
                                    print(f"[REINFORCED_LINK] Extracted CDN domain from meta image: {cdn_domain}")
                            except Exception as e:
                                print(f"[REINFORCED_LINK] Error extracting CDN domain: {e}")
                        
                        # Construct video URL using the CDN domain or fallback
                        if cdn_domain:
                            video_url = URL(f"https://{cdn_domain}/{file_id}.mp4")
                            print(f"[REINFORCED_LINK] Constructed video URL with CDN domain: {video_url}")
                            return video_url
                        else:
                            cdn_domains = [
                                "nachos.bunkr.ru", "wings.bunkr.ru", "wiener.bunkr.ru",
                                "ramen.bunkr.ru", "pizza.bunkr.ru", "burger.bunkr.ru", 
                                "fries.bunkr.ru", "meatballs.bunkr.ru", "milkshake.bunkr.ru",
                                "kebab.bunkr.ru", "taquito.bunkr.ru", "soup.bunkr.ru",
                                "mlk-bk.cdn.gigachad-cdn.ru"
                            ]
                            
                            for cdn in cdn_domains:
                                video_url = URL(f"https://{cdn}/{file_id}.mp4")
                                print(f"[REINFORCED_LINK] Trying CDN URL: {video_url}")
                                return video_url
                
                # Check for video URLs in scripts
                for script in soup.select('script'):
                    if script.string and '.mp4' in script.string:
                        print("[REINFORCED_LINK] Found script with mp4 reference")
                        matches = re.findall(r'https?://[^"\']+\.mp4', script.string)
                        if matches:
                            video_url = URL(matches[0])
                            print(f"[REINFORCED_LINK] Extracted video URL from script: {video_url}")
                            return video_url
                
                # Check for data-t attributes in scripts
                scripts = soup.select('script[data-t]')
                for script in scripts:
                    data_t = script.get('data-t')
                    if data_t and ".mp4" in data_t:
                        print(f"[REINFORCED_LINK] Found data-t attribute with mp4: {data_t}")
                        video_url = URL(data_t)
                        return video_url
                
                # Last resort - use file ID from URL
                file_id = url.parts[-1] if url.parts else None
                if file_id:
                    print(f"[REINFORCED_LINK] Using file ID from URL: {file_id}")
                    cdn_domains = [
                        "nachos.bunkr.ru", "wings.bunkr.ru", "wiener.bunkr.ru",
                        "ramen.bunkr.ru", "pizza.bunkr.ru", "burger.bunkr.ru", 
                        "fries.bunkr.ru", "meatballs.bunkr.ru", "milkshake.bunkr.ru",
                        "kebab.bunkr.ru", "taquito.bunkr.ru", "soup.bunkr.ru",
                        "mlk-bk.cdn.gigachad-cdn.ru"
                    ]
                    
                    for cdn in cdn_domains:
                        video_url = URL(f"https://{cdn}/{file_id}.mp4")
                        print(f"[REINFORCED_LINK] Trying CDN URL: {video_url}")
                        return video_url
                
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
            "wiener", "i-wiener", "ramen", "i-ramen", "nachos", "i-nachos",
            "wings", "i-wings", "pizza", "i-pizza", "burger", "i-burger", 
            "fries", "i-fries", "meatballs", "i-meatballs", "milkshake", "i-milkshake",
            "kebab", "i-kebab", "taquito", "i-taquito", "soup", "i-soup",
            "cdn-wiener", "cdn-ramen", "cdn-pizza", "cdn-burger", "cdn-meatballs", 
            "cdn-milkshake", "cdn-kebab", "cdn-taquito", "cdn-soup", "cdn-nachos",
            "cdn-fries", "cdn-wings", "cdn", "c", "media-files", "mlk-bk.cdn.gigachad-cdn"
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

    async def get_album_results(self, album_id):
        print(f"[GET_ALBUM_RESULTS] Getting album results for: {album_id}")
        if not album_id:
            print("[GET_ALBUM_RESULTS] No album ID provided, returning empty results")
            return []
            
        try:
            results = []
            return results
        except Exception as e:
            print(f"[GET_ALBUM_RESULTS] Error: {str(e)}")
            print(f"Error getting album results: {str(e)}")
            return []

    async def check_album_results(self, url, results):
        print(f"[CHECK_ALBUM_RESULTS] Checking if URL already exists: {url}")
        try:
            return False
        except Exception as e:
            print(f"[CHECK_ALBUM_RESULTS] Error: {str(e)}")
            print(f"Error checking album results: {str(e)}")
            return False
