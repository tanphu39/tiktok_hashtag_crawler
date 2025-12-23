#!/usr/bin/env python3
"""
TikTok Hashtag Crawler - Using Undetected ChromeDriver
This version uses undetected-chromedriver which is designed to bypass bot detection.
"""

import json
import time
import re
from typing import Set, List
import requests
from urllib.parse import urljoin

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Note: undetected-chromedriver not installed. Install with: pip install undetected-chromedriver selenium")


class HashtagCrawler:
    """TikTok crawler using undetected-chromedriver."""
    
    def __init__(self, headless: bool = False, auto_fallback: bool = True):
        """
        Initialize the crawler.
        
        Args:
            headless: Run browser in headless mode
            auto_fallback: Automatically fall back to non-headless if headless fails
        """
        self.headless = headless
        self.auto_fallback = auto_fallback
        self.driver = None
        self.video_links: Set[str] = set()
        self.original_headless = headless  # Remember original setting
        
    def extract_video_ids_from_text(self, text: str) -> Set[str]:
        """Extract full video URLs (with username) from text content."""
        video_urls = set()
        
        # Pattern 1: Full URLs with username (most preferred)
        # https://www.tiktok.com/@username/video/1234567890
        full_url_patterns = [
            r'https?://(?:www\.|m\.|vm\.)?tiktok\.com/@([^/\s"\'<>]+)/video/(\d+)',
            r'"url":\s*"https?://(?:www\.|m\.|vm\.)?tiktok\.com/@([^/"]+)/video/(\d+)"',
            r'"shareUrl":\s*"https?://(?:www\.|m\.|vm\.)?tiktok\.com/@([^/"]+)/video/(\d+)"',
            r'href=["\']https?://(?:www\.|m\.|vm\.)?tiktok\.com/@([^/"\'<>]+)/video/(\d+)',
            r'https?://(?:www\.|m\.|vm\.)?tiktok\.com/@([^/\s"\'<>]+)/video/(\d+)',
            # Also match without protocol
            r'tiktok\.com/@([^/\s"\'<>]+)/video/(\d+)',
            r'@([^/\s"\'<>]+)/video/(\d+)',
        ]
        
        for pattern in full_url_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple) and len(match) == 2:
                    username, video_id = match[0].strip(), match[1].strip()
                    if username and video_id.isdigit() and len(video_id) > 8:
                        full_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
                        video_urls.add(full_url)
        
        # Pattern 2: Extract from structured data (JSON) that might have username + video ID
        # Look for patterns where username and video ID are in proximity
        json_patterns = [
            r'"uniqueId":\s*"([^"]+)".*?"id":\s*"(\d+)"',  # Username and ID in JSON
            r'"id":\s*"(\d+)".*?"uniqueId":\s*"([^"]+)"',  # ID and Username in JSON
            r'"nickname":\s*"([^"]+)".*?"id":\s*"(\d+)"',  # Nickname and ID
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, text, re.DOTALL)
            for match in matches:
                if isinstance(match, tuple) and len(match) == 2:
                    # Could be (username, id) or (id, username)
                    part1, part2 = match[0].strip(), match[1].strip()
                    
                    # Determine which is username and which is ID
                    if part1.isdigit() and len(part1) > 8:
                        video_id, username = part1, part2
                    elif part2.isdigit() and len(part2) > 8:
                        username, video_id = part1, part2
                    else:
                        continue
                    
                    if username and video_id and '/' not in username:  # Basic validation
                        full_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
                        video_urls.add(full_url)
        
        # Pattern 3: Extract video IDs only (fallback) - will create URL without username
        # This is less preferred but better than nothing
        video_id_patterns = [
            r'/video/(\d+)',
            r'"videoId":"(\d+)"',
            r'"id":"(\d+)"',
            r'video_id["\']?\s*:\s*["\']?(\d+)',
            r'"aweme_id":"(\d+)"',
        ]
        
        for pattern in video_id_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match else ''
                video_id = str(match).strip()
                if video_id.isdigit() and len(video_id) > 8:
                    # Only add if we don't already have this video ID in a full URL
                    video_id_exists = any(f'/video/{video_id}' in url for url in video_urls)
                    if not video_id_exists:
                        # Fallback: URL without username (will be normalized later)
                        fallback_url = f"https://www.tiktok.com/video/{video_id}"
                        video_urls.add(fallback_url)
        
        return video_urls
    
    def normalize_url(self, video_url: str) -> str:
        """Normalize TikTok video URL."""
        # If it's already a full URL, return as is
        if video_url.startswith('http'):
            return video_url
        # If it's just a video ID, create URL without username (fallback)
        if video_url.isdigit():
            return f"https://www.tiktok.com/video/{video_url}"
        return video_url
    
    def handle_error_page(self, max_retries: int = 3):
        """Check for error messages and click refresh if needed."""
        for attempt in range(max_retries):
            try:
                # Check for specific error element first (most reliable method)
                has_error = False
                error_element = None
                
                try:
                    # Check the specific error element path
                    error_xpath = "/html/body/div[1]/div[2]/div[2]/div/main/div/p[1]"
                    error_elements = self.driver.find_elements(By.XPATH, error_xpath)
                    
                    for elem in error_elements:
                        try:
                            if elem.is_displayed():
                                elem_text = elem.text.strip()
                                # Check if it contains "Something Went Wrong" (case insensitive)
                                if elem_text and "something went wrong" in elem_text.lower():
                                    has_error = True
                                    error_element = elem
                                    print(f"Found error message: '{elem_text}'")
                                    break
                        except:
                            continue
                except Exception as e:
                    # If element not found, that's fine - no error
                    pass
                
                # If specific error element found, proceed to refresh
                if has_error:
                    print(f"Error detected on page (attempt {attempt + 1}/{max_retries})")
                    
                    # Try to find and click refresh button
                    refresh_found = False
                    
                    # Try multiple selectors for refresh button
                    # Prioritize buttons that are likely to be the Refresh button
                    refresh_selectors = [
                        (By.XPATH, "//button[contains(., 'Refresh')]"),
                        (By.XPATH, "//button[contains(text(), 'Refresh')]"),
                        (By.XPATH, "//a[contains(., 'Refresh')]"),
                        (By.XPATH, "//a[contains(text(), 'Refresh')]"),
                        (By.XPATH, "//button[contains(., 'Try again')]"),
                        (By.XPATH, "//button[contains(text(), 'Try again')]"),
                        (By.XPATH, "//button[contains(., 'Reload')]"),
                        (By.XPATH, "//button[contains(text(), 'Reload')]"),
                        (By.CSS_SELECTOR, "button[data-e2e='refresh-button']"),
                        (By.CSS_SELECTOR, "a[data-e2e='refresh-button']"),
                        (By.XPATH, "//button[@type='button' and contains(., 'Refresh')]"),
                    ]
                    
                    for selector_type, selector_value in refresh_selectors:
                        try:
                            refresh_button = WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable((selector_type, selector_value))
                            )
                            # Make sure button is visible
                            if refresh_button.is_displayed():
                                print(f"Found refresh button, clicking...")
                                refresh_button.click()
                                refresh_found = True
                                break
                        except (TimeoutException, NoSuchElementException):
                            continue
                    
                    # If no button found, try JavaScript to reload
                    if not refresh_found:
                        print("No refresh button found, trying JavaScript reload...")
                        self.driver.execute_script("location.reload();")
                    
                    # Wait for page to reload
                    print("Waiting for page to reload...")
                    time.sleep(5)
                    
                    # Check if error is still present after refresh
                    time.sleep(3)  # Wait for page to fully reload
                    
                    # Check if we now have content
                    has_content_after = False
                    try:
                        video_links_after = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/video/')]")
                        video_ids_after = re.findall(r'/video/(\d+)', self.driver.page_source)
                        has_content_after = len(video_links_after) > 0 or len(video_ids_after) > 0
                    except:
                        pass
                    
                    # Check if the specific error element still exists
                    still_has_error = False
                    try:
                        error_xpath = "/html/body/div[1]/div[2]/div[2]/div/main/div/p[1]"
                        error_elements_after = self.driver.find_elements(By.XPATH, error_xpath)
                        for elem in error_elements_after:
                            if elem.is_displayed():
                                elem_text = elem.text.strip().lower()
                                if "something went wrong" in elem_text:
                                    still_has_error = True
                                    break
                    except:
                        pass
                    
                    if not still_has_error or has_content_after:
                        print("Error resolved after refresh!")
                        return True
                    else:
                        print("Error still present, will retry...")
                        time.sleep(2)
                else:
                    # No error found - page is fine
                    return False
                    
            except Exception as e:
                print(f"Error while handling error page: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
        
        return False
    
    def crawl_hashtag(self, hashtag: str, max_videos: int = None) -> List[str]:
        """Crawl TikTok hashtag using undetected ChromeDriver."""
        if not SELENIUM_AVAILABLE:
            raise ImportError("Please install required packages: pip install undetected-chromedriver selenium")
        
        hashtag = hashtag.strip('#').strip()
        url = f"https://www.tiktok.com/tag/{hashtag}"
        
        print(f"Starting crawl for hashtag: #{hashtag}")
        print(f"URL: {url}")
        print("Using undetected-chromedriver to bypass bot detection...\n")
        
        try:
            # Create undetected ChromeDriver instance
            options = uc.ChromeOptions()
            
            # Better headless mode settings - TikTok may detect headless, so use minimal headless
            # IMPORTANT: TikTok often blocks headless browsers. Consider using non-headless mode.
            if self.headless:
                print("⚠️  Running in headless mode - TikTok may not load video content.")
                print("   If no videos are found, try running without --headless flag.\n")

                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                # Important: Don't use --disable-features that might trigger detection
                options.add_argument('--disable-features=IsolateOrigins,site-per-process')
            else:
                options.add_argument('--start-maximized')
            
            # Anti-detection arguments
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--disable-infobars')
            options.add_argument('--disable-extensions')
            
            # User agent
            options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            print("Launching Chrome browser...")
            # Use undetected_chromedriver with better stealth
            self.driver = uc.Chrome(
                options=options, 
                version_main=None,
                use_subprocess=True  # Better for headless
            )
            self.driver.set_page_load_timeout(60)
            
            # Set window size explicitly
            if self.headless:
                try:
                    self.driver.set_window_size(1920, 1080)
                except:
                    pass
            
            # Execute JavaScript to simulate focus, visibility, and user presence
            try:
                self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': '''
                        // Override visibility and focus detection
                        Object.defineProperty(document, 'hidden', { 
                            get: () => false,
                            configurable: true
                        });
                        Object.defineProperty(document, 'visibilityState', { 
                            get: () => 'visible',
                            configurable: true
                        });
                        Object.defineProperty(document, 'hasFocus', {
                            value: () => true,
                            configurable: true
                        });
                        
                        // Remove webdriver flag
                        Object.defineProperty(navigator, 'webdriver', { 
                            get: () => undefined,
                            configurable: true
                        });
                        
                        // Add chrome object
                        window.chrome = { 
                            runtime: {},
                            loadTimes: function() {},
                            csi: function() {},
                            app: {}
                        };
                        
                        // Add plugins
                        Object.defineProperty(navigator, 'plugins', { 
                            get: () => [1, 2, 3, 4, 5],
                            configurable: true
                        });
                        
                        // Add languages
                        Object.defineProperty(navigator, 'languages', { 
                            get: () => ['en-US', 'en'],
                            configurable: true
                        });
                        
                        // Simulate focus events
                        window.addEventListener('focus', () => {}, true);
                        window.addEventListener('blur', () => {}, true);
                        
                        // Dispatch focus event
                        window.dispatchEvent(new Event('focus'));
                        document.dispatchEvent(new Event('visibilitychange'));
                    '''
                })
            except Exception as e:
                print(f"Warning: Could not set up stealth scripts: {e}")
            
            print(f"Loading page: {url}")
            
            # Simulate focus and user presence before loading
            if self.headless:
                try:
                    # Use CDP to simulate focus
                    self.driver.execute_cdp_cmd('Runtime.evaluate', {
                        'expression': 'window.focus(); document.hasFocus = () => true;'
                    })
                except:
                    pass
            
            self.driver.get(url)
            
            # Wait for page to load - longer wait for headless
            wait_time = 12 if self.headless else 8
            print(f"Waiting for page to load ({wait_time}s)...")
            time.sleep(wait_time)
            
            # In headless mode, try to trigger content loading by simulating interactions
            if self.headless:
                try:
                    # Try scrolling a bit to trigger lazy loading
                    self.driver.execute_script("window.scrollTo(0, 100);")
                    time.sleep(2)
                    self.driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(2)
                except:
                    pass
            
            # Simulate user interaction in headless mode
            if self.headless:
                # Simulate mouse movement, focus, and user activity
                try:
                    # Multiple interaction simulations
                    self.driver.execute_script("""
                        // Simulate focus
                        window.dispatchEvent(new Event('focus', { bubbles: true }));
                        document.dispatchEvent(new Event('visibilitychange', { bubbles: true }));
                        
                        // Simulate mouse movement
                        document.dispatchEvent(new MouseEvent('mousemove', {
                            bubbles: true,
                            cancelable: true,
                            view: window,
                            clientX: 100,
                            clientY: 100
                        }));
                        
                        // Simulate scroll
                        window.dispatchEvent(new Event('scroll', { bubbles: true }));
                        
                        // Ensure document appears focused
                        if (document.hasFocus) {
                            Object.defineProperty(document, 'hasFocus', {
                                value: () => true,
                                writable: false
                            });
                        }
                    """)
                    
                    # Also use CDP to simulate input
                    self.driver.execute_cdp_cmd('Input.dispatchMouseEvent', {
                        'type': 'mouseMoved',
                        'x': 100,
                        'y': 100
                    })
                except Exception as e:
                    print(f"Warning: Could not simulate interactions: {e}")
            
            # Check for "Something went wrong" error and handle it
            self.handle_error_page()
            
            # Try to wait for specific elements
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda d: len(d.find_elements(By.TAG_NAME, "a")) > 10
                )
                print("Page content loaded successfully")
            except TimeoutException:
                print("Warning: Page may not have loaded completely")
                # Continue anyway
            
            # Extract video links from page source
            print("Extracting video links from page...")
            
            # Debug: Check page content
            try:
                page_source = self.driver.page_source
                page_length = len(page_source)
                print(f"Page source length: {page_length} characters")
                
                # Check if page has TikTok content
                has_tiktok_content = 'tiktok' in page_source.lower() or 'video' in page_source.lower()
                print(f"Page contains TikTok/video content: {has_tiktok_content}")
                
                # Try to find any video-related elements
                try:
                    video_elements = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/video/')]")
                    print(f"Found {len(video_elements)} video link elements in DOM")
                except:
                    pass
            except Exception as e:
                print(f"Warning: Could not analyze page: {e}")
            
            scroll_count = 0
            max_scrolls = 100 if max_videos is None else min(max_videos // 5 + 10, 50)
            no_change_count = 0
            
            while scroll_count < max_scrolls:
                # Only check for errors if we haven't found any videos yet
                # This prevents false positives on pages with normal content
                if len(self.video_links) == 0 and scroll_count == 0:
                    self.handle_error_page(max_retries=1)
                
                # Get page source and extract video URLs
                page_source = self.driver.page_source
                video_urls = self.extract_video_ids_from_text(page_source)
                
                # Debug output
                if scroll_count == 0:
                    print(f"Initial extraction found {len(video_urls)} video URLs")
                    if len(video_urls) == 0:
                        # Try alternative extraction methods
                        print("Trying alternative extraction methods...")
                        
                        # Method 1: JavaScript DOM extraction
                        try:
                            js_urls = self.driver.execute_script("""
                                const links = new Set();
                                // Find all anchor tags
                                document.querySelectorAll('a').forEach(a => {
                                    const href = a.getAttribute('href') || a.href;
                                    if (href && href.includes('/video/')) {
                                        links.add(href);
                                    }
                                });
                                // Also check data attributes
                                document.querySelectorAll('[data-e2e*="video"], [class*="video"]').forEach(el => {
                                    const href = el.getAttribute('href') || el.closest('a')?.href;
                                    if (href && href.includes('/video/')) {
                                        links.add(href);
                                    }
                                });
                                return Array.from(links);
                            """)
                            if js_urls:
                                print(f"JavaScript DOM found {len(js_urls)} video links")
                                for url in js_urls:
                                    if url and '/video/' in url:
                                        normalized = self.normalize_url(url)
                                        if normalized:
                                            video_urls.add(normalized)
                        except Exception as e:
                            print(f"JavaScript DOM extraction failed: {e}")
                        
                        # Method 2: Extract from window objects
                        if len(video_urls) == 0:
                            try:
                                window_data = self.driver.execute_script("""
                                    let data = {};
                                    // Try to get data from window objects
                                    if (window.__UNIVERSAL_DATA_FOR_REHYDRATION__) {
                                        data.universal = JSON.stringify(window.__UNIVERSAL_DATA_FOR_REHYDRATION__);
                                    }
                                    if (window.SIGI_STATE) {
                                        data.sigi = JSON.stringify(window.SIGI_STATE);
                                    }
                                    return data;
                                """)
                                
                                if window_data:
                                    for key, json_str in window_data.items():
                                        if json_str:
                                            extracted = self.extract_video_ids_from_text(json_str)
                                            if extracted:
                                                print(f"Extracted {len(extracted)} URLs from {key} data")
                                                video_urls.update(extracted)
                            except Exception as e:
                                print(f"Window data extraction failed: {e}")
                        
                        # Method 3: Force wait and retry (headless might need more time)
                        if len(video_urls) == 0 and self.headless:
                            print("Headless mode: Waiting longer for content to load...")
                            time.sleep(5)
                            # Retry extraction
                            page_source_retry = self.driver.page_source
                            video_urls_retry = self.extract_video_ids_from_text(page_source_retry)
                            if video_urls_retry:
                                print(f"Retry extraction found {len(video_urls_retry)} URLs")
                                video_urls.update(video_urls_retry)
                            
                            # If still no videos and auto_fallback is enabled, suggest fallback
                            if len(video_urls) == 0 and self.auto_fallback and scroll_count == 0:
                                print("\n⚠️  WARNING: Headless mode detected - TikTok may not load content in headless mode.")
                                print("   TikTok's anti-bot protection often blocks headless browsers.")
                                print("   Consider running without --headless flag for better results.")
                                print("   Continuing with headless mode...\n")
                
                # Add new video URLs
                new_count = 0
                for video_url in video_urls:
                    normalized_url = self.normalize_url(video_url)
                    if normalized_url not in self.video_links:
                        self.video_links.add(normalized_url)
                        new_count += 1
                
                current_count = len(self.video_links)
                
                if new_count > 0:
                    print(f"Found {new_count} new videos (total: {current_count})")
                    no_change_count = 0
                else:
                    no_change_count += 1
                    if no_change_count >= 5:  # Increased threshold
                        print("No new videos found after multiple scrolls. Stopping.")
                        break
                
                if max_videos and current_count >= max_videos:
                    print(f"Reached max videos limit ({max_videos})")
                    break
                
                # Scroll down to load more - try different scroll strategies
                print(f"Scrolling down ({scroll_count + 1}/{max_scrolls})...")
                
                # Smooth scroll
                self.driver.execute_script("""
                    window.scrollBy({
                        top: window.innerHeight,
                        behavior: 'smooth'
                    });
                """)
                time.sleep(2)  # Wait for smooth scroll
                
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Wait for new content to load
                
                scroll_count += 1
            
            # Final extraction
            print("\nPerforming final extraction...")
            page_source = self.driver.page_source
            final_video_urls = self.extract_video_ids_from_text(page_source)
            for video_url in final_video_urls:
                normalized_url = self.normalize_url(video_url)
                self.video_links.add(normalized_url)
            
        except Exception as e:
            print(f"Error during crawling: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if self.driver:
                print("\nClosing browser...")
                self.driver.quit()
        
        result = sorted(list(self.video_links))
        if max_videos:
            result = result[:max_videos]
        
        print(f"\nCrawl complete! Found {len(result)} unique videos.")
        return result


def crawl_with_requests(hashtag: str) -> List[str]:
    """Alternative: Try direct API calls with requests library."""
    hashtag = hashtag.strip('#').strip()
    
    print(f"\nTrying direct API approach for hashtag: #{hashtag}")
    
    # Try to get challenge detail first
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': f'https://www.tiktok.com/tag/{hashtag}',
    }
    
    try:
        # Get challenge detail to get challenge ID
        challenge_url = f"https://www.tiktok.com/api/challenge/detail/"
        params = {
            'challengeName': hashtag,
            'aid': '1988',
        }
        
        response = requests.get(challenge_url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            challenge_id = data.get('challengeInfo', {}).get('challenge', {}).get('id')
            
            if challenge_id:
                print(f"Found challenge ID: {challenge_id}")
                print("Note: Getting video list requires authentication tokens.")
                print("This approach needs additional setup (cookies, tokens, etc.)")
        
    except Exception as e:
        print(f"API request failed: {e}")
    
    return []


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='TikTok Hashtag Crawler')
    parser.add_argument('hashtag', nargs='?', default='nhuabinhminh', help='Hashtag to crawl')
    parser.add_argument('--max-videos', type=int, default=None, help='Max videos to collect')
    parser.add_argument('--output', '-o', default=None, help='Output JSON file')
    parser.add_argument('--headless', action='store_true', 
                       help='Run in headless mode (Note: TikTok may not load content in headless mode)')
    parser.add_argument('--no-fallback', action='store_true',
                       help='Disable automatic fallback warnings')
    parser.add_argument('--method', choices=['selenium', 'api'], default='selenium',
                       help='Crawling method to use')
    
    args = parser.parse_args()
    
    if args.method == 'api':
        video_links = crawl_with_requests(args.hashtag)
    else:
        if not SELENIUM_AVAILABLE:
            print("ERROR: undetected-chromedriver not installed!")
            print("Install with: pip install undetected-chromedriver selenium")
            return
        
        crawler = HashtagCrawler(headless=args.headless, auto_fallback=not args.no_fallback)
        video_links = crawler.crawl_hashtag(args.hashtag, args.max_videos)
    
    # Save results
    output_file = args.output or f"tiktok_{args.hashtag}_videos.json"
    
    result = {
        'hashtag': args.hashtag,
        'total_videos': len(video_links),
        'video_links': video_links,
        'crawl_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'method': args.method
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {output_file}")
    if video_links:
        print(f"\nFirst 5 video links:")
        for i, link in enumerate(video_links[:5], 1):
            print(f"  {i}. {link}")
    else:
        print("\nNo video links found.")


if __name__ == '__main__':
    main()