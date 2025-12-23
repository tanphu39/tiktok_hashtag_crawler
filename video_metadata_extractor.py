#!/usr/bin/env python3
"""
TikTok Video Metadata Extractor
Extracts metadata from TikTok video links: title, description, username, engagement metrics.
"""

import json
import time
import re
import os
import tempfile
import random
from typing import Dict, List, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Thread
from xxlimited import Null

try:
    import undetected_chromedriver as uc
    # from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, 
        WebDriverException, InvalidSessionIdException
    )
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Error: Please install required packages: pip install undetected-chromedriver selenium")


class TikTokVideoMetadataExtractor:
    """Extracts metadata from TikTok video pages."""
    
    def __init__(self, headless: bool = False, delay: float = 2.0, num_threads: int = 3):
        """
        Initialize the extractor.
        
        Args:
            headless: Run browser in headless mode
            delay: Delay between requests in seconds
            num_threads: Number of parallel threads for processing (default: 3, reduced to prevent Chrome conflicts)
        """
        self.headless = headless
        self.delay = delay
        self.num_threads = num_threads
        self.driver = None
        self._progress_lock = Lock()
        self._file_save_lock = Lock()  # Separate lock for file operations
        self._driver_creation_lock = Lock()  # Lock to serialize driver creation
        self._processed_count = 0
        self._completed_count = 0  # Thread-safe completed counter

    def setup_driver(self):
        """Setup undetected ChromeDriver using webdriver_manager."""
        if not SELENIUM_AVAILABLE:
            raise ImportError("Please install required packages: pip install undetected-chromedriver selenium webdriver-manager")
        
        # Use webdriver_manager to get the ChromeDriver path
        # driver_path = ChromeDriverManager().install()
        
        options = uc.ChromeOptions()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        print("Launching Chrome browser...")
        # undetected_chromedriver will use the driver from webdriver_manager
        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(60)
    
    def _is_driver_alive(self, driver) -> bool:
        """Check if driver is still alive and responsive."""
        if driver is None:
            return False
        try:
            # Try to get current URL - this will fail if driver is dead
            driver.current_url
            return True
        except (InvalidSessionIdException, WebDriverException, AttributeError, Exception):
            return False
    
    def _create_driver(self, max_retries: int = 3):
        """
        Create a new browser driver instance (thread-safe - each thread gets its own driver).
        Includes retry logic for common driver creation errors.
        """
        if not SELENIUM_AVAILABLE:
            raise ImportError("Please install required packages: pip install undetected-chromedriver selenium webdriver-manager")
        
        # Use webdriver_manager to get the ChromeDriver path
        # driver_path = ChromeDriverManager().install()
        
        # Helper function to create ChromeOptions (must create new instance each time)
        def _create_options():
            options = uc.ChromeOptions()
            if self.headless:
                options.add_argument('--headless')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            return options
        
        # Retry logic for driver creation errors
        # Use lock to serialize driver creation and prevent too many simultaneous Chrome instances
        with self._driver_creation_lock:
            # Add longer random delay to stagger driver creation attempts and allow Chrome processes to settle
            time.sleep(random.uniform(0.5, 1.5))
            
            last_error = None
            driver = None
            for attempt in range(max_retries):
                try:
                    # Create new ChromeOptions for each attempt (cannot reuse)
                    options = _create_options()
                    
                    # Add unique user data dir to prevent conflicts
                    user_data_dir = tempfile.mkdtemp(prefix='chrome_profile_')
                    options.add_argument(f'--user-data-dir={user_data_dir}')
                    
                    # Each thread creates its own driver - no shared state, so no race condition
                    # Use subprocess for better isolation and stability
                    # Use webdriver_manager to get the driver path
                    driver = uc.Chrome(
                        options=options, 
                        version_main=None,
                        use_subprocess=True,  # Better isolation, helps with "not reachable" errors
                        # driver_executable_path=driver_path
                    )
                    driver.set_page_load_timeout(60)
                    
                    # Wait a bit for Chrome to fully start and become reachable
                    time.sleep(1.0)
                    
                    # Verify driver is working with retries
                    max_verify_attempts = 3
                    for verify_attempt in range(max_verify_attempts):
                        if self._is_driver_alive(driver):
                            # Double check by trying to get a property
                            try:
                                _ = driver.current_url
                                return driver
                            except:
                                if verify_attempt < max_verify_attempts - 1:
                                    time.sleep(0.5)
                                    continue
                                else:
                                    raise WebDriverException("Driver created but not responsive")
                        else:
                            if verify_attempt < max_verify_attempts - 1:
                                time.sleep(0.5)
                                continue
                            else:
                                # Clean up failed driver
                                try:
                                    driver.quit()
                                except:
                                    pass
                                raise WebDriverException("Driver created but not reachable")
                        
                except (WebDriverException, OSError, FileNotFoundError, Exception) as e:
                    last_error = e
                    error_msg = str(e).lower()
                    
                    # Clean up any partially created driver
                    if driver:
                        try:
                            driver.quit()
                        except:
                            pass
                        driver = None
                    
                    # Check for specific errors that might be recoverable
                    is_recoverable = any(keyword in error_msg for keyword in [
                        'no such file', 'chromedriver', 'can not connect', 
                        'cannot connect', 'session not created', 'connection refused',
                        'service', 'chromeoptions', 'reuse', '127.0.0.1',
                        'chrome not reachable', 'not reachable', 'unable to connect'
                    ])
                    
                    if is_recoverable and attempt < max_retries - 1:
                        # Longer wait time for "chrome not reachable" errors
                        base_wait = (attempt + 1) * 3  # Increased from 2 to 3 seconds
                        if 'not reachable' in error_msg or 'cannot connect' in error_msg:
                            base_wait += 2  # Extra wait for connection issues
                        wait_time = base_wait + random.uniform(0.5, 1.5)  # Exponential backoff with jitter
                        print(f"  Driver creation failed (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}...")
                        print(f"  Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    
                    # For other errors or final attempt, raise
                    if attempt == max_retries - 1:
                        raise
        
        # Should not reach here, but just in case
        raise last_error if last_error else WebDriverException("Failed to create driver after retries")
    
    def extract_metadata(self, video_url: str, driver=None, max_retries: int = 2) -> Dict:
        """
        Extract metadata from a TikTok video page.
        
        Args:
            video_url: URL of the TikTok video
            driver: Optional Selenium driver instance (if None, uses self.driver)
            max_retries: Maximum number of retries for driver errors (default: 2)
            
        Returns:
            Dictionary containing video metadata
        """
        use_local_driver = driver is not None
        if not driver:
            if not self.driver or not self._is_driver_alive(self.driver):
                self.setup_driver()
            driver = self.driver
        
        # Check if driver is alive before proceeding
        if not self._is_driver_alive(driver):
            if use_local_driver:
                # Try to recreate driver
                try:
                    driver = self._create_driver()
                except Exception as e:
                    return {
                        'url': video_url,
                        'title': None,
                        'description': None,
                        'username': None,
                        'like_count': None,
                        'comment_count': None,
                        'share_count': None,
                        'view_count': None,
                        'archive_count': None,
                        'hashtags': [],
                        'error': f"Driver creation failed: {str(e)}"
                    }
            else:
                self.setup_driver()
                driver = self.driver
        
        metadata = {
            'url': video_url,
            'title': None,
            'description': None,
            'username': None,
            'like_count': None,
            'comment_count': None,
            'share_count': None,
            'view_count': None,
            'archive_count': None,  # collectCount
            'hashtags': [],
            'error': None
        }
        
        hashtags_found = set()
        
        def _add_hashtags_from_text(text: Optional[str]):
            for tag in self._extract_hashtags_from_text(text):
                hashtags_found.add(tag)
        
        # Retry logic for driver errors
        last_error = None
        for retry_attempt in range(max_retries):
            try:
                # Check driver is still alive before each operation
                if not self._is_driver_alive(driver):
                    if use_local_driver:
                        # Recreate driver for thread-local case
                        try:
                            driver = self._create_driver()
                        except Exception as e:
                            last_error = f"Driver recreation failed: {str(e)}"
                            if retry_attempt < max_retries - 1:
                                time.sleep(2)
                                continue
                            break
                    else:
                        # Recreate shared driver
                        self.setup_driver()
                        driver = self.driver
                
                # Print only in sequential mode (when using shared driver)
                is_sequential = (driver is None or driver == self.driver)
                if is_sequential and retry_attempt == 0:
                    print(f"\nExtracting metadata from: {video_url}")
                
                
                driver.get(video_url)
                # Reduced wait time - page should load faster
                time.sleep(max(self.delay, 1.5))  # Minimum 1.5s, or use configured delay
                
                # Check driver is still alive after page load
                if not self._is_driver_alive(driver):
                    raise WebDriverException("Driver died after page load")
                
                # If we get here, the operation succeeded, continue with extraction
                # All extraction code is below, wrapped in try-except
                
            except (InvalidSessionIdException, WebDriverException) as e:
                error_msg = str(e).lower()
                last_error = str(e)
                
                # Check for specific recoverable errors
                is_recoverable = any(keyword in error_msg for keyword in [
                    'target window already closed',
                    'no such window',
                    'web view not found',
                    'session',
                    'connection',
                    'service'
                ])
                
                if is_recoverable and retry_attempt < max_retries - 1:
                    print(f"  Driver error (attempt {retry_attempt + 1}/{max_retries}), retrying...")
                    # Recreate driver
                    try:
                        if use_local_driver:
                            driver = self._create_driver()
                        else:
                            self.setup_driver()
                            driver = self.driver
                        time.sleep(2)  # Brief delay before retry
                        continue
                    except Exception as recreate_error:
                        last_error = f"Driver recreation failed: {str(recreate_error)}"
                        break
                else:
                    # Non-recoverable or final attempt
                    break
            except Exception as e:
                # Other errors - don't retry
                last_error = str(e)
                break
        
        # If we exhausted retries or got a non-recoverable error
        if last_error:
            metadata = {
                'url': video_url,
                'title': None,
                'description': None,
                'username': None,
                'like_count': None,
                'comment_count': None,
                'share_count': None,
                'view_count': None,
                'archive_count': None,
                'hashtags': [],
                'error': last_error
            }
            # Only delay if using shared driver (sequential mode)
            if not use_local_driver:
                time.sleep(self.delay)
            return metadata
        
        # If we get here, driver.get() succeeded, continue with extraction
        try:
            # Check driver is still alive before JavaScript execution
            if not self._is_driver_alive(driver):
                raise WebDriverException("Driver died during page load")
            
            # Execute JavaScript to extract data from TikTok's internal state
            try:
                # Check driver before executing script
                if not self._is_driver_alive(driver):
                    raise WebDriverException("Driver died before JavaScript execution")
                js_data = driver.execute_script("""
                    // Try to access TikTok's data structures
                    let result = {};
                    
                    // Try __UNIVERSAL_DATA_FOR_REHYDRATION__
                    if (window.__UNIVERSAL_DATA_FOR_REHYDRATION__) {
                        result.universal_data = window.__UNIVERSAL_DATA_FOR_REHYDRATION__;
                    }
                    
                    // Try to get video metadata from page
                    if (window.SIGI_STATE) {
                        result.sigi_state = window.SIGI_STATE;
                    }
                    
                    // Try to get from meta tags
                    let metaTags = {};
                    document.querySelectorAll('meta').forEach(meta => {
                        let prop = meta.getAttribute('property') || meta.getAttribute('name');
                        let content = meta.getAttribute('content');
                        if (prop && content) {
                            metaTags[prop] = content;
                        }
                    });
                    result.metaTags = metaTags;
                    
                    // Get page text for regex extraction
                    result.pageText = document.body.innerText;
                    
                    return result;
                """)
                
                # Process JavaScript extracted data
                if js_data.get('metaTags'):
                    meta = js_data['metaTags']
                    title_val = meta.get('og:title')
                    if not metadata['title'] and title_val:
                        metadata['title'] = title_val
                    _add_hashtags_from_text(title_val)
                    desc_val = meta.get('og:description')
                    if not metadata['description'] and desc_val:
                        metadata['description'] = desc_val
                    _add_hashtags_from_text(desc_val)
                page_text = js_data.get('pageText')
                if page_text:
                    _add_hashtags_from_text(page_text)
                
                # Try to extract from universal data
                if js_data.get('universal_data'):
                    # Search for video stats in the data structure
                    data_str = json.dumps(js_data['universal_data'])
                    print(data_str)
                    # Extract numbers from JSON
                    like_matches = re.findall(r'"diggCount":\s*(\d+)', data_str)
                    if like_matches:
                        metadata['like_count'] = int(min(like_matches, key=lambda x: int(x)))
                    
                    comment_matches = re.findall(r'"commentCount":\s*(\d+)', data_str)
                    if comment_matches:
                        metadata['comment_count'] = int(max(comment_matches, key=lambda x: int(x)))
                    
                    share_matches = re.findall(r'"shareCount":\s*(\d+)', data_str)
                    if share_matches:
                        metadata['share_count'] = int(max(share_matches, key=lambda x: int(x)))
                    
                    view_matches = re.findall(r'"playCount":\s*(\d+)', data_str)
                    if view_matches:
                        metadata['view_count'] = int(max(view_matches, key=lambda x: int(x)))
                    
                    # Extract archive count (collectCount) - try multiple field names
                    collect_patterns = [
                        r'"collectCount":\s*(\d+)',
                        r'"collectionCount":\s*(\d+)',
                        r'"savedCount":\s*(\d+)',
                        r'"bookmarkCount":\s*(\d+)',
                        r'"favoriteCount":\s*(\d+)',
                        r'"collect":\s*(\d+)',
                    ]
                    collect_matches = []
                    for pattern in collect_patterns:
                        matches = re.findall(pattern, data_str)
                        if matches:
                            collect_matches.extend(matches)
                    if collect_matches:
                        metadata['archive_count'] = int(max(collect_matches, key=lambda x: int(x)))
                    
                    # Extract username
                    username_matches = re.findall(r'"uniqueId":\s*"([^"]+)"', data_str)
                    if username_matches:
                        metadata['username'] = username_matches[0]
                    
                    # Extract description
                    desc_matches = re.findall(r'"text":\s*"([^"]+)"', data_str)
                    if desc_matches and not metadata['description']:
                        # Get the longest description (likely the video description)
                        metadata['description'] = max(desc_matches, key=len)
                        metadata['title'] = metadata['description'][:100]
                        _add_hashtags_from_text(metadata['description'])
                        
            except Exception as e:
                print(f"  Warning: JavaScript extraction failed: {e}")
            
            # Extract username from URL or page
            try:
                # Try to get username from URL first
                username_match = re.search(r'@([^/]+)', video_url)
                if username_match:
                    metadata['username'] = username_match.group(1)
                
                # Also try to extract from page
                if not metadata['username']:
                    # Try multiple methods to find username
                    username_selectors = [
                        "//a[contains(@href, '/@')]",
                        "//*[contains(@data-e2e, 'browse-username')]",
                        "//*[contains(@class, 'username')]",
                        "//h2[contains(@data-e2e, 'browse-username')]",
                    ]
                    
                    for selector in username_selectors:
                        try:
                            if not self._is_driver_alive(driver):
                                break
                            username_elements = driver.find_elements(By.XPATH, selector)
                            for elem in username_elements[:5]:
                                try:
                                    href = elem.get_attribute('href')
                                    if href and '/@' in href:
                                        match = re.search(r'@([^/]+)', href)
                                        if match:
                                            metadata['username'] = match.group(1)
                                            break
                                    
                                    # Also check text content
                                    text = elem.text.strip()
                                    if text.startswith('@'):
                                        metadata['username'] = text.replace('@', '').strip()
                                        break
                                except:
                                    continue
                            if metadata['username']:
                                break
                        except:
                            continue
            except Exception as e:
                print(f"  Warning: Could not extract username: {e}")
            
            # Extract description/title
            try:
                # Try multiple selectors for description
                description_selectors = [
                    "//*[@data-e2e='browse-video-desc']",
                    "//*[contains(@class, 'video-desc')]",
                    "//*[contains(@class, 'description')]",
                    "//h1",
                    "//h2",
                    "//meta[@property='og:description']",
                    "//meta[@name='description']",
                ]
                
                for selector in description_selectors:
                    try:
                        if not self._is_driver_alive(driver):
                            break
                        if selector.startswith("//meta"):
                            elem = driver.find_element(By.XPATH, selector)
                            description = elem.get_attribute('content')
                        else:
                            elem = driver.find_element(By.XPATH, selector)
                            description = elem.text.strip()
                        
                        if description and len(description) > 0:
                            # Use first line as title, full text as description
                            lines = description.split('\n')
                            metadata['title'] = lines[0].strip() if lines else description[:100]
                            metadata['description'] = description.strip()
                            _add_hashtags_from_text(metadata['description'])
                            break
                    except:
                        continue
                
                # Fallback: Get from page source
                if not metadata['description']:
                    if not self._is_driver_alive(driver):
                        raise WebDriverException("Driver died before getting page source")
                    page_source = driver.page_source
                    # Try to find in JSON-LD or meta tags
                    meta_desc = re.search(r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']', page_source)
                    if meta_desc:
                        metadata['description'] = meta_desc.group(1)
                        metadata['title'] = metadata['description'][:100]
                        _add_hashtags_from_text(metadata['description'])
            except Exception as e:
                print(f"  Warning: Could not extract description: {e}")
            
            # Extract engagement metrics (like, comment, share, view counts)
            try:
                if not self._is_driver_alive(driver):
                    raise WebDriverException("Driver died before extracting metrics")
                page_source = driver.page_source
                
                # Try to extract from structured data in page source
                # Look for JSON data with metrics - TikTok stores data in __UNIVERSAL_DATA_FOR_REHYDRATION__
                try:
                    # Try to extract from JavaScript JSON data
                    json_data_match = re.search(r'<script[^>]*id=["\']__UNIVERSAL_DATA_FOR_REHYDRATION__["\'][^>]*>(.*?)</script>', page_source, re.DOTALL)
                    if json_data_match:
                        json_str = json_data_match.group(1)
                        try:
                            data = json.loads(json_str)
                            
                            # Navigate through the data structure to find video stats
                            def find_in_dict(obj, key):
                                if isinstance(obj, dict):
                                    if key in obj:
                                        return obj[key]
                                    for v in obj.values():
                                        result = find_in_dict(v, key)
                                        if result is not None:
                                            return result
                                elif isinstance(obj, list):
                                    for item in obj:
                                        result = find_in_dict(item, key)
                                        if result is not None:
                                            return result
                                return None
                            
                            # Try to find video data
                            video_data = find_in_dict(data, 'videoData') or find_in_dict(data, 'itemInfo') or find_in_dict(data, 'stats')
                            
                            if video_data and isinstance(video_data, dict):
                                metadata['like_count'] = video_data.get('diggCount') or video_data.get('likeCount')
                                metadata['comment_count'] = video_data.get('commentCount')
                                metadata['share_count'] = video_data.get('shareCount')
                                metadata['view_count'] = video_data.get('playCount') or video_data.get('viewCount')
                                # Try multiple field names for archive count
                                metadata['archive_count'] = (
                                    video_data.get('collectCount') or
                                    video_data.get('collectionCount') or
                                    video_data.get('savedCount') or
                                    video_data.get('bookmarkCount') or
                                    video_data.get('favoriteCount')
                                )
                            
                            # Also try to find collectCount in stats object if it exists separately
                            if not metadata['archive_count']:
                                stats_data = find_in_dict(data, 'stats')
                                if stats_data and isinstance(stats_data, dict):
                                    metadata['archive_count'] = (
                                        stats_data.get('collectCount') or
                                        stats_data.get('collectionCount') or
                                        stats_data.get('savedCount') or
                                        stats_data.get('bookmarkCount') or
                                        stats_data.get('favoriteCount')
                                    )
                            
                            # Also try to find collectCount in the entire data structure recursively
                            if not metadata['archive_count']:
                                collect_field_names = ['collectCount', 'collectionCount', 'savedCount', 'bookmarkCount', 'favoriteCount']
                                for field_name in collect_field_names:
                                    collect_value = find_in_dict(data, field_name)
                                    if collect_value is not None:
                                        try:
                                            metadata['archive_count'] = int(collect_value)
                                            break
                                        except (ValueError, TypeError):
                                            continue
                        except:
                            pass
                except:
                    pass
                
                # Fallback: Extract from regex patterns in page source
                if not metadata['like_count']:
                    like_matches = re.findall(r'"diggCount":\s*(\d+)', page_source) + \
                                  re.findall(r'"likeCount":\s*(\d+)', page_source)
                    if like_matches:
                        metadata['like_count'] = int(min(like_matches, key=lambda x: int(x)))
                
                if not metadata['comment_count']:
                    comment_matches = re.findall(r'"commentCount":\s*(\d+)', page_source)
                    if comment_matches:
                        metadata['comment_count'] = int(max(comment_matches, key=lambda x: int(x)))
                
                if not metadata['share_count']:
                    share_matches = re.findall(r'"shareCount":\s*(\d+)', page_source)
                    if share_matches:
                        metadata['share_count'] = int(max(share_matches, key=lambda x: int(x)))
                
                if not metadata['view_count']:
                    view_matches = re.findall(r'"playCount":\s*(\d+)', page_source) + \
                                  re.findall(r'"viewCount":\s*(\d+)', page_source)
                    if view_matches:
                        metadata['view_count'] = int(max(view_matches, key=lambda x: int(x)))
                
                # Extract archive count (collectCount) from page source - try multiple field names
                # Also try case-insensitive and with/without quotes variations
                if not metadata['archive_count']:
                    collect_patterns = [
                        r'"collectCount":\s*(\d+)',
                        r'"collectionCount":\s*(\d+)',
                        r'"savedCount":\s*(\d+)',
                        r'"bookmarkCount":\s*(\d+)',
                        r'"favoriteCount":\s*(\d+)',
                        r'"collect":\s*(\d+)',
                        r'collectCount["\']?\s*:\s*(\d+)',  # Without quotes
                        r'collectionCount["\']?\s*:\s*(\d+)',
                        r'savedCount["\']?\s*:\s*(\d+)',
                        r'bookmarkCount["\']?\s*:\s*(\d+)',
                        r'favoriteCount["\']?\s*:\s*(\d+)',
                    ]
                    collect_matches = []
                    for pattern in collect_patterns:
                        matches = re.findall(pattern, page_source, re.IGNORECASE)
                        if matches:
                            collect_matches.extend(matches)
                    if collect_matches:
                        metadata['archive_count'] = int(max(collect_matches, key=lambda x: int(x)))
                
                # Also try to extract from visible elements
                if not metadata['like_count']:
                    try:
                        like_elements = driver.find_elements(By.XPATH, "//*[contains(@data-e2e, 'like') or contains(text(), 'Like')]")
                        for elem in like_elements[:5]:
                            try:
                                text = elem.text.strip()
                                # Look for numbers near "Like"
                                numbers = re.findall(r'(\d+(?:\.\d+)?[KMB]?)', text)
                                if numbers:
                                    metadata['like_count'] = self.parse_count(numbers[0])
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if not metadata['comment_count']:
                    try:
                        comment_elements = driver.find_elements(By.XPATH, "//*[contains(@data-e2e, 'comment') or contains(text(), 'Comment')]")
                        for elem in comment_elements[:5]:
                            try:
                                text = elem.text.strip()
                                numbers = re.findall(r'(\d+(?:\.\d+)?[KMB]?)', text)
                                if numbers:
                                    metadata['comment_count'] = self.parse_count(numbers[0])
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if not metadata['share_count']:
                    try:
                        share_elements = driver.find_elements(By.XPATH, "//*[contains(@data-e2e, 'share') or contains(text(), 'Share')]")
                        for elem in share_elements[:5]:
                            try:
                                text = elem.text.strip()
                                numbers = re.findall(r'(\d+(?:\.\d+)?[KMB]?)', text)
                                if numbers:
                                    metadata['share_count'] = self.parse_count(numbers[0])
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if not metadata['view_count']:
                    try:
                        view_elements = driver.find_elements(By.XPATH, "//*[contains(@data-e2e, 'view') or contains(text(), 'View')]")
                        for elem in view_elements[:5]:
                            try:
                                text = elem.text.strip()
                                numbers = re.findall(r'(\d+(?:\.\d+)?[KMB]?)', text)
                                if numbers:
                                    metadata['view_count'] = self.parse_count(numbers[0])
                                    break
                            except:
                                continue
                    except:
                        pass
                
                # Extract archive count from visible elements
                if not metadata['archive_count']:
                    try:
                        # Try multiple selectors for collect/archive/save buttons
                        collect_selectors = [
                            "//*[contains(@data-e2e, 'collect')]",
                            "//*[contains(@data-e2e, 'archive')]",
                            "//*[contains(@data-e2e, 'save')]",
                            "//*[contains(@data-e2e, 'bookmark')]",
                            "//*[contains(@aria-label, 'collect') or contains(@aria-label, 'save')]",
                            "//button[contains(., 'Collect') or contains(., 'Save')]",
                            "//*[contains(@class, 'collect') or contains(@class, 'save')]",
                        ]
                        for selector in collect_selectors:
                            try:
                                collect_elements = driver.find_elements(By.XPATH, selector)
                                for elem in collect_elements[:5]:
                                    try:
                                        # Get text from element and nearby elements
                                        text = elem.text.strip()
                                        # Also check parent element
                                        try:
                                            parent = elem.find_element(By.XPATH, "..")
                                            text += " " + parent.text.strip()
                                        except:
                                            pass
                                        # Look for numbers
                                        numbers = re.findall(r'(\d+(?:\.\d+)?[KMB]?)', text)
                                        if numbers:
                                            metadata['archive_count'] = self.parse_count(numbers[0])
                                            break
                                    except:
                                        continue
                                if metadata['archive_count']:
                                    break
                            except:
                                continue
                    except:
                        pass
                        
            except Exception as e:
                print(f"  Warning: Could not extract metrics: {e}")
            
            print(f"  ✓ Extracted: @{metadata['username']}, {metadata['like_count']} likes, {metadata['view_count']} views")
            
        except Exception as e:
            error_msg = str(e)
            metadata['error'] = error_msg
            print(f"  ✗ Error extracting metadata: {error_msg}")
        
        # Only delay if using shared driver (sequential mode)
            time.sleep(self.delay)  # Delay between requests
        
        if metadata.get('description'):
            _add_hashtags_from_text(metadata['description'])
        metadata['hashtags'] = sorted(hashtags_found)
        if metadata['archive_count'] is not None:
            try:
                metadata['archive_count'] = int(metadata['archive_count'])
            except (ValueError, TypeError):
                metadata['archive_count'] = None
        time.sleep(self.delay)  # Delay between requests
        return metadata
    
    def _process_single_video(self, video_url: str, index: int, total: int) -> Dict:
        """
        Process a single video in a thread-safe manner.
        Creates its own browser instance for thread safety.
        
        Args:
            video_url: URL of the video to process
            index: Index of the video (for progress tracking)
            total: Total number of videos
            
        Returns:
            Dictionary containing video metadata
        """
        driver = None
        try:
            # Create a new driver instance for this thread (with retry logic)
            driver = self._create_driver()
            
            # Extract metadata using thread-local driver (with retry logic)
            metadata = self.extract_metadata(video_url, driver=driver, max_retries=2)
            
            # Update progress in a thread-safe manner
            with self._progress_lock:
                self._processed_count += 1
                current = self._processed_count
            
            # Print outside lock to avoid blocking
            username = metadata.get('username', 'N/A')
            likes = metadata.get('like_count', 'N/A')
            views = metadata.get('view_count', 'N/A')
            print(f"[{current}/{total}] ✓ @{username} - {likes} likes, {views} views")
            
            return metadata
            
        except Exception as e:
            error_msg = str(e)
            with self._progress_lock:
                self._processed_count += 1
                current = self._processed_count
            
            # Print outside lock to avoid blocking
            print(f"[{current}/{total}] ✗ Error: {video_url} - {error_msg}")
            
            return {
                'url': video_url,
                'title': None,
                'description': None,
                'username': None,
                'like_count': None,
                'comment_count': None,
                'share_count': None,
                'view_count': None,
                'archive_count': None,
                'hashtags': [],
                'error': error_msg
            }
        finally:
            # Always close the driver to free resources
            if driver:
                try:
                    # Try graceful shutdown first
                    driver.quit()
                except:
                    try:
                        # Force close if quit() fails
                        driver.close()
                    except:
                        pass
                # Small delay to allow cleanup
                time.sleep(0.1)
    
    def parse_count(self, count_str: str) -> Optional[int]:
        """Parse count string like '1.2K', '5M' into integer."""
        try:
            count_str = count_str.upper().strip()
            if 'K' in count_str:
                return int(float(count_str.replace('K', '')) * 1000)
            elif 'M' in count_str:
                return int(float(count_str.replace('M', '')) * 1000000)
            elif 'B' in count_str:
                return int(float(count_str.replace('B', '')) * 1000000000)
            else:
                return int(float(count_str))
        except:
            return None
    
    def _extract_hashtags_from_text(self, text: Optional[str]) -> List[str]:
        """Extract unique hashtags (without #) from text."""
        if not text or not isinstance(text, str):
            return []
        hashtags = set()
        for match in re.findall(r'#([A-Za-z0-9_]+)', text):
            cleaned = match.strip().lower()
            if cleaned:
                hashtags.add(cleaned)
        return list(hashtags)
    
    def extract_from_links(self, video_links: List[str], output_file: str = None, use_threading: bool = True) -> List[Dict]:
        """
        Extract metadata from a list of video links.
        
        Args:
            video_links: List of TikTok video URLs
            output_file: Optional output JSON file path
            use_threading: Whether to use multi-threading (default: True)
            
        Returns:
            List of metadata dictionaries
        """
        print(f"\n{'='*60}")
        print(f"Extracting metadata from {len(video_links)} videos...")
        if use_threading:
            print(f"Using {self.num_threads} parallel threads")
        else:
            print("Using sequential processing (single thread)")
        print(f"{'='*60}\n")
        
        all_metadata = []
        self._processed_count = 0
        self._completed_count = 0
        
        if use_threading and len(video_links) > 1:
            # Multi-threaded processing
            all_metadata = [None] * len(video_links)  # Pre-allocate list
            
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                # Submit all tasks
                future_to_index = {
                    executor.submit(self._process_single_video, url, i, len(video_links)): i
                    for i, url in enumerate(video_links)
                }
                
                # Process completed tasks and save progress periodically
                for future in as_completed(future_to_index):
                    index = future_to_index[future]
                    try:
                        metadata = future.result()
                        # Thread-safe assignment (list assignment is atomic in Python)
                        all_metadata[index] = metadata
                        
                        # Thread-safe counter increment and save check
                        should_save = False
                        with self._progress_lock:
                            self._completed_count += 1
                            completed = self._completed_count
                            # Only save if we hit a milestone and no other thread is saving
                            if completed % 10 == 0 and output_file:
                                should_save = True
                        
                        # Save outside the progress lock to avoid blocking other threads
                        if should_save:
                            with self._file_save_lock:
                                # Filter out None values for partial save
                                completed_metadata = [m for m in all_metadata if m is not None]
                                self.save_results(completed_metadata, output_file, partial=True)
                                print(f"  Progress saved ({completed}/{len(video_links)})")
                                
                    except Exception as e:
                        error_msg = str(e)
                        print(f"  ✗ Thread error for index {index}: {error_msg}")
                        all_metadata[index] = {
                            'url': video_links[index] if index < len(video_links) else 'unknown',
                            'title': None,
                            'description': None,
                            'username': None,
                            'like_count': None,
                            'comment_count': None,
                            'share_count': None,
                            'view_count': None,
                            'archive_count': None,
                            'hashtags': [],
                            'error': error_msg
                        }
                        # Still increment counter on error
                        with self._progress_lock:
                            self._completed_count += 1
        else:
            # Sequential processing (fallback or single video)
            if not self.driver:
                self.setup_driver()
        
        try:
            for i, video_url in enumerate(video_links, 1):
                print(f"[{i}/{len(video_links)}] Processing...")
                metadata = self.extract_metadata(video_url)
                all_metadata.append(metadata)
                
                # Save progress periodically
                if i % 10 == 0 and output_file:
                    self.save_results(all_metadata, output_file, partial=True)
                    print(f"  Progress saved ({i}/{len(video_links)})")
        finally:
            if self.driver:
                self.driver.quit()
        
        # Filter out None values (shouldn't happen, but safety check)
        all_metadata = [m for m in all_metadata if m is not None]
        
        # Save final results (thread-safe)
        if output_file:
                self.save_results(all_metadata, output_file)
                self.save_results(all_metadata, output_file)
        
        return all_metadata
    
    def save_results(self, metadata: List[Dict], output_file: str, partial: bool = False):
        """
        Save metadata results to JSON file (thread-safe).
        Note: This method should be called with _file_save_lock when used in threaded context.
        """
        suffix = "_partial" if partial else ""
        file_path = output_file.replace('.json', f'{suffix}.json') if partial else output_file
        
        for video in metadata:
            if not isinstance(video, dict):
                continue
            if video.get('archive_count') is not None:
                try:
                    video['archive_count'] = int(video['archive_count'])
                except (ValueError, TypeError):
                    video['archive_count'] = None
            hashtags = video.get('hashtags')
            if hashtags is None:
                video['hashtags'] = []
            elif isinstance(hashtags, list):
                cleaned = []
                seen = set()
                for tag in hashtags:
                    if not isinstance(tag, str):
                        continue
                    cleaned_tag = tag.lstrip('#').lower()
                    if cleaned_tag and cleaned_tag not in seen:
                        seen.add(cleaned_tag)
                        cleaned.append(cleaned_tag)
                video['hashtags'] = cleaned
            else:
                video['hashtags'] = []
        
        result = {
            'total_videos': len(metadata),
            'extracted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'videos': metadata
        }
        
        # Use atomic write: write to temp file first, then rename
        temp_file = file_path + '.tmp'
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            # Atomic rename (works on Unix and Windows)
            if os.path.exists(file_path):
                os.replace(temp_file, file_path)
            else:
                os.rename(temp_file, file_path)
        except Exception as e:
            # Clean up temp file on error
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass
            raise e

    
    def finalize_and_retry_errors(self, output_file: str) -> Dict:
        """
        Check output file for items with errors and re-crawl them in single-threaded mode.
        
        Args:
            output_file: Path to the output JSON file to check and update
            
        Returns:
            Dictionary with retry statistics
        """
        print(f"\n{'='*60}")
        print("Finalizing: Checking for errors and retrying failed extractions...")
        print(f"{'='*60}\n")
        
        # Load existing results
        if not os.path.exists(output_file):
            print(f"Error: Output file not found: {output_file}")
            return {'retried': 0, 'successful': 0, 'still_failed': 0}
        
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error loading output file: {e}")
            return {'retried': 0, 'successful': 0, 'still_failed': 0}
        
        # Extract videos list
        if isinstance(data, dict):
            videos = data.get('videos', [])
        elif isinstance(data, list):
            videos = data
        else:
            print("Error: Unexpected JSON structure")
            return {'retried': 0, 'successful': 0, 'still_failed': 0}
        
        # Find videos with errors
        failed_videos = [v for v in videos if v.get('error') is not None and v.get('error') != '']
        
        if not failed_videos:
            print("✓ No errors found in output file. All extractions were successful!")
            return {'retried': 0, 'successful': 0, 'still_failed': 0}
        
        print(f"Found {len(failed_videos)} videos with errors. Retrying in single-threaded mode...\n")
        
        # Extract URLs of failed videos
        failed_urls = [v.get('url') for v in failed_videos if v.get('url')]
        
        if not failed_urls:
            print("No valid URLs found in failed videos.")
            return {'retried': 0, 'successful': 0, 'still_failed': 0}
        
        # Create a mapping of URL to index in the videos list for easy updates
        url_to_index = {}
        for idx, video in enumerate(videos):
            url = video.get('url')
            if url:
                url_to_index[url] = idx
        
        # Re-extract metadata in single-threaded mode (more reliable for retries)
        print("Retrying failed extractions (single-threaded mode)...")
        successful_retries = 0
        still_failed = 0
        
        # Setup driver for sequential processing
        if not self.driver:
            self.setup_driver()
        
        try:
            for i, url in enumerate(failed_urls, 1):
                print(f"\n[{i}/{len(failed_urls)}] Retrying: {url}")
                try:
                    # Extract metadata using sequential mode
                    new_metadata = self.extract_metadata(url)
                    
                    # Update the corresponding entry in the videos list
                    if url in url_to_index:
                        idx = url_to_index[url]
                        # Preserve the original entry but update with new data
                        videos[idx] = new_metadata
                        
                        if new_metadata.get('error'):
                            still_failed += 1
                            print(f"  ✗ Still failed: {new_metadata.get('error')}")
                        else:
                            successful_retries += 1
                            print(f"  ✓ Successfully retried!")
                    else:
                        # URL not found in mapping, append as new entry
                        videos.append(new_metadata)
                        if not new_metadata.get('error'):
                            successful_retries += 1
                        else:
                            still_failed += 1
                            
                except Exception as e:
                    print(f"  ✗ Error during retry: {e}")
                    still_failed += 1
                    # Update error message
                    if url in url_to_index:
                        idx = url_to_index[url]
                        videos[idx]['error'] = f"Retry failed: {str(e)}"
        
        finally:
            # Clean up driver
            if self.driver:
                self.driver.quit()
                self.driver = None
        
        # Update the data structure
        is_dict_format = isinstance(data, dict)
        if is_dict_format:
            data['videos'] = videos
            data['total_videos'] = len(videos)
            data['finalized_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
            # Update statistics
            successful_total = sum(1 for v in videos if not v.get('error'))
            data['successful_extractions'] = successful_total
            data['failed_extractions'] = len(videos) - successful_total
        
        # Save updated results
        print(f"\nSaving updated results to: {output_file}")
        with self._file_save_lock:
            # save_results expects a list of video dicts
            self.save_results(videos, output_file)
        
        # Print summary
        print(f"\n{'='*60}")
        print("Finalization Summary:")
        print(f"{'='*60}")
        print(f"Videos retried: {len(failed_urls)}")
        print(f"  ✓ Successfully retried: {successful_retries}")
        print(f"  ✗ Still failed: {still_failed}")
        print(f"\nUpdated file: {output_file}")
        print(f"{'='*60}\n")
        
        return {
            'retried': len(failed_urls),
            'successful': successful_retries,
            'still_failed': still_failed
        }

def load_links_from_json(json_file: str) -> List[str]:
    """Load video links from crawler output JSON file."""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle different JSON structures
    if isinstance(data, dict):
        if 'video_links' in data:
            return data['video_links']
        elif 'videos' in data:
            return [v.get('url') for v in data['videos'] if v.get('url')]
    elif isinstance(data, list):
        return data
    
    return []

def _process_chunk_in_thread(chunk_links: List[str], thread_id: int, headless: bool, delay: float, results_list: List[Dict], results_lock: Lock):
    """
    Process a chunk of video links in a separate thread with its own extractor instance.
    
    Args:
        chunk_links: List of video URLs to process in this thread
        thread_id: Identifier for this thread (for logging)
        headless: Whether to run browser in headless mode
        delay: Delay between requests
        results_list: Shared list to store results (thread-safe)
        results_lock: Lock for thread-safe access to results_list
    """
    print(f"Thread {thread_id}: Starting to process {len(chunk_links)} videos")
    
    # Create a separate extractor instance for this thread
    extractor = TikTokVideoMetadataExtractor(
        headless=headless,
        delay=delay,
        num_threads=1  # Each thread processes sequentially within its own instance
    )
    
    driver = None
    try:
        # Stagger driver creation to avoid simultaneous Chrome launches
        # Each thread waits a bit longer to reduce resource contention
        stagger_delay = (thread_id - 1) * 2.0  # 0s, 2s, 4s, etc.
        if stagger_delay > 0:
            print(f"Thread {thread_id}: Waiting {stagger_delay}s before creating driver...")
            time.sleep(stagger_delay)
        
        # Create a driver for this thread using the robust _create_driver method
        print(f"Thread {thread_id}: Creating browser driver...")
        driver = extractor._create_driver()
        print(f"Thread {thread_id}: Browser driver created successfully")
        
        # Process each link sequentially in this thread
        for i, video_url in enumerate(chunk_links, 1):
            print(f"Thread {thread_id}: [{i}/{len(chunk_links)}] Processing {video_url}")
            # Pass the driver to extract_metadata to avoid creating new instances
            metadata = extractor.extract_metadata(video_url, driver=driver)
            
            # Thread-safe append to results
            with results_lock:
                results_list.append(metadata)
            
            # Print progress
            username = metadata.get('username', 'N/A')
            likes = metadata.get('like_count', 'N/A')
            views = metadata.get('view_count', 'N/A')
            print(f"Thread {thread_id}: [{i}/{len(chunk_links)}] ✓ @{username} - {likes} likes, {views} views")
            time.sleep(1)
    except Exception as e:
        print(f"Thread {thread_id}: Error during processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up driver if it exists
        if driver:
            try:
                driver.quit()
            except:
                try:
                    driver.close()
                except:
                    pass
    
    print(f"Thread {thread_id}: Completed processing {len(chunk_links)} videos")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract metadata from TikTok video links')
    parser.add_argument('input', nargs='?', default=None,
                       help='Input JSON file with video links (required unless using --finalize with --output)')
    parser.add_argument('--output', '-o', type=str, default=None,
                       help='Output JSON file path (default: input_file_metadata.json)')
    parser.add_argument('--headless', action='store_true',
                       help='Run browser in headless mode')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between requests in seconds (default: 2.0)')
    parser.add_argument('--threads', '-t', type=int, default=3,
                       help='Number of parallel threads for processing (default: 3, recommended max: 4-5 to prevent Chrome conflicts)')
    parser.add_argument('--no-threading', action='store_true',
                       help='Disable multi-threading (use sequential processing)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of videos to process')
    parser.add_argument('--finalize', action='store_true',
                       help='Check output file for errors and retry failed extractions in single-threaded mode')
    
    args = parser.parse_args()
    
    if not SELENIUM_AVAILABLE:
        print("ERROR: Please install required packages:")
        print("  pip install undetected-chromedriver selenium")
        return
    
    # If --finalize is used without input, use output file directly
    if args.finalize and not args.input:
        if not args.output:
            print("Error: --output is required when using --finalize without --input")
            parser.print_help()
            return
        # Run finalize on the output file
        extractor = TikTokVideoMetadataExtractor(
            headless=args.headless,
            delay=args.delay,
            num_threads=1  # Single thread for finalize
        )
        extractor.finalize_and_retry_errors(args.output)
        return
    
    # Input is required for normal extraction
    if not args.input:
        print("Error: Input file is required (or use --finalize with --output)")
        parser.print_help()
        return
    
    # Determine output file
    if not args.output:
        input_path = Path(args.input)
        args.output = str(input_path.parent / f"{input_path.stem}_metadata.json")
    
    # Load video links
    print(f"Loading video links from: {args.input}")
    
    try:
        video_links = load_links_from_json(args.input)
        print(f"Loaded {len(video_links)} video links")
        
        if args.limit:
            video_links = video_links[:args.limit]
            print(f"Limited to {len(video_links)} videos")
        
        if not video_links:
            print("No video links found in input file!")
            return
        
        # Create extractor instance for finalize if needed (will be created later if not finalizing)
        extractor = None
        
        # Divide video_links into 5 chunks
        num_threads = args.threads
        chunk_size = len(video_links) // num_threads
        remainder = len(video_links) % num_threads
        
        chunks = []
        start_idx = 0
        for i in range(num_threads):
            # Distribute remainder across first few chunks
            current_chunk_size = chunk_size + (1 if i < remainder else 0)
            end_idx = start_idx + current_chunk_size
            chunks.append(video_links[start_idx:end_idx])
            start_idx = end_idx
        
        print(f"\n{'='*60}")
        print(f"Dividing {len(video_links)} videos into {num_threads} threads:")
        for i, chunk in enumerate(chunks, 1):
            print(f"  Thread {i}: {len(chunk)} videos")
        print(f"{'='*60}\n")
        
        # Create shared results list and lock
        all_metadata = []
        results_lock = Lock()
        
        # Create and start 5 threads, each with its own extractor instance
        threads = []
        for thread_id in range(1, num_threads + 1):
            if len(chunks[thread_id - 1]) > 0:  # Only create thread if chunk is not empty
                thread = Thread(
                    target=_process_chunk_in_thread,
                    args=(
                        chunks[thread_id - 1],
                        thread_id,
                        args.headless,
                        args.delay,
                        all_metadata,
                        results_lock
                    )
                )
                threads.append(thread)
                thread.start()
                # Small delay between thread starts to avoid simultaneous driver creation
                time.sleep(1.5)
        # Wait for all threads to complete
        print(f"\nWaiting for all {len(threads)} threads to complete...")
        for thread in threads:
            thread.join()
        
        print(f"\nAll threads completed. Total results: {len(all_metadata)}")
        
        # Save results
        if args.output:
            extractor = TikTokVideoMetadataExtractor(
                headless=args.headless,
                delay=args.delay,
                num_threads=1
            )
            extractor.save_results(all_metadata, args.output)
        
        metadata = all_metadata
        
        # Print summary
        print(f"\n{'='*60}")
        print("Extraction Summary:")
        print(f"{'='*60}")
        print(f"Total videos processed: {len(metadata)}")
        successful = sum(1 for m in metadata if not m.get('error'))
        print(f"Successfully extracted: {successful}")
        print(f"Errors: {len(metadata) - successful}")
        print(f"\nResults saved to: {args.output}")
        
        # Show sample
        if metadata:
            print("\nSample extracted data:")
            sample = [m for m in metadata if not m.get('error')][:3]
            for m in sample:
                title = m.get('title') or 'N/A'
                title_display = title[:50] + '...' if isinstance(title, str) and len(title) > 50 else title
                print(f"  - @{m.get('username', 'N/A')}: {title_display}")
                print(f"    Likes: {m.get('like_count', 'N/A')}, Views: {m.get('view_count', 'N/A')}")
        
        # Finalize: retry errors if requested
        if args.finalize:
            if not extractor:
                extractor = TikTokVideoMetadataExtractor(
                    headless=args.headless,
                    delay=args.delay,
                    num_threads=1
                )
            extractor.finalize_and_retry_errors(args.output)
    
    except FileNotFoundError:
        print(f"Error: Input file not found: {args.input}")
        print("Please run the crawler first to generate video links.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

