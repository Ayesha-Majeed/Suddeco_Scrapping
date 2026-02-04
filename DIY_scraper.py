import os
import json
import time
import pandas as pd
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import concurrent.futures
import random
import db_utils


# ==========================================
# 1. LOGGING & GLOBAL CONFIG
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("diy_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DIYScraper:
    def __init__(self, headless=False):
        self.base_url = "https://www.diy.com"
        self.current_region = "UK (Default)"
        self.headless = headless
        self.all_scraped_data = [] 
        self.last_save_count = 0
        self.next_break_at = 0
        self._init_driver()

    def _init_driver(self):
        """Initialize or restart the Chrome driver"""
        logger.info("🔧 Initializing Chrome Driver...")
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        
        # User-Agent strictly modern
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
        
        chrome_options.add_argument("--remote-debugging-pipe")
        chrome_options.add_argument("--disable-extensions")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except:
            self.driver = webdriver.Chrome(options=chrome_options)
        
        # Disable webdriver signature
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        self.wait = WebDriverWait(self.driver, 25)

    # ==========================================
    # 2. HUMAN INTERACTION METHODS
    # ==========================================
    def safe_click(self, element):
        try:
            element.click()
        except:
            self.driver.execute_script("arguments[0].click();", element)

    def human_action_simulator(self):
        """
        Sophisticated human behavior simulation:
        1. Random small mouse movements with Bezier-like curve smoothing.
        2. Random scrolls (e.g. reading behavior).
        3. Random idle times.
        """
        try:
            # 1. MOUSE MOVEMENT
            action = ActionChains(self.driver)
            body = self.driver.find_element(By.TAG_NAME, "body")
            
            # Start from a random position within typical viewport
            current_x = random.randint(100, 800)
            current_y = random.randint(100, 600)
            
            # Move to start
            try:
                action.move_to_element_with_offset(body, current_x, current_y).perform()
            except: pass
            
            # Perform a series of small, "nervous" human-like movements (micro-corrections)
            # This makes the mouse path look less linear
            for _ in range(random.randint(3, 7)):
                offset_x = random.randint(-50, 50)
                offset_y = random.randint(-50, 50)
                action.move_by_offset(offset_x, offset_y)
                action.pause(random.uniform(0.05, 0.2))
                
            try:
                action.perform()
            except: pass
            
            # 2. READING SCROLL (Scroll down a bit, maybe scroll up a tiny bit)
            if random.random() < 0.7:
                scroll_amount = random.randint(100, 400)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.5, 1.5))
                
                # Occasional scroll up (checking something)
                if random.random() < 0.3:
                    self.driver.execute_script(f"window.scrollBy(0, -{random.randint(50, 150)});")
                    time.sleep(random.uniform(0.5, 1.0))

            # 3. HOVER OVER RANDOM ELEMENT (if feasible)
            try:
                # Find tangible elements like links or images in viewport
                elements = self.driver.find_elements(By.CSS_SELECTOR, "a, h1, h2, h3, p")
                # Filter visible ones roughly (simple check)
                visible_elements = [e for e in elements if e.is_displayed()]
                
                if visible_elements:
                    target = random.choice(visible_elements[:10]) # Pick from top results for speed
                    action.move_to_element(target).pause(random.uniform(0.5, 1.5)).perform()
            except: pass

        except Exception as e:
             # logger.debug(f"Human simulation error: {e}") 
             pass

    def human_pause(self, min_s=0.5, max_s=1.8):
        # Slightly more variability
        time.sleep(random.uniform(min_s, max_s) * random.uniform(0.9, 1.2))

    def human_move_mouse(self, moves=2):
        # Legacy wrapper calling the new simulator
        self.human_action_simulator()

    def scroll_to_bottom(self, pause_time=1):
        """Progressive scrolling identical to Screwfix."""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        for _ in range(10):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.human_pause(pause_time, pause_time+1)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: break
            last_height = new_height
        self.driver.execute_script("window.scrollTo(0, 0);")

    # ==========================================
    # 3. SET LOCATION (Postcode Support)
    # ==========================================
    def set_location(self, postcode="E1 6AN"):
        """DIY Specific Store Locator logic."""
        logger.info(f"Setting B&Q store to: {postcode}")
        try:
            self.driver.get(f"{self.base_url}/store-finder")
            self.handle_cookies()
            search_box = self.wait.until(EC.presence_of_element_located((By.ID, "store-finder-search")))
            search_box.clear()
            search_box.send_keys(postcode)
            search_box.send_keys(Keys.ENTER)
            time.sleep(3)
            # Find the first 'Set as my store' button
            set_btns = self.driver.find_elements(By.XPATH, "//button[contains(., 'Set as')]")
            if set_btns:
                self.safe_click(set_btns[0])
                logger.info("Store set successfully.")
        except Exception as e:
            logger.warning(f"Could not set postcode: {e}")

    def handle_cookies(self):
        try:
            btn = self.wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            self.safe_click(btn)
            time.sleep(1)
        except: pass

    # ==========================================
    # 4. NAVIGATION & LISTING
    # ==========================================
    def navigate_all_categories(self, max_products=None):
        target_categories = [
            "https://www.diy.com/departments/heating-plumbing-cooling/DIY1652280.cat",
            "https://www.diy.com/offers.cat",
            "https://www.diy.com/departments/painting-decorating/DIY779142.cat",
            "https://www.diy.com/departments/home-furniture-storage/DIY1726650.cat",
            "https://www.diy.com/departments/building-hardware/DIY774984.cat",
            "https://www.diy.com/departments/outdoor-garden/DIY780276.cat",
            "https://www.diy.com/kitchen.cat",
            "https://www.diy.com/departments/bathroom/DIY822106.cat",
            "https://www.diy.com/departments/flooring-tiling/DIY764939.cat",
            "https://www.diy.com/departments/lighting-electrical/DIY775079.cat",
            "https://www.diy.com/departments/tools-equipment/DIY779463.cat",
        ]
        
        all_data = []
        for url in target_categories:
            if max_products and len(all_data) >= max_products: break
            logger.info(f"--- STARTING CATEGORY: {url} ---")
            cat_data = self.scrape_category_recursive(url, max_products=(max_products - len(all_data)) if max_products else None)
            all_data.extend(cat_data)
        return all_data


    def scrape_category_recursive(self, url, max_products=None, depth=0):
        """FIXED: Better detection of product pages vs category pages"""
        if depth > 5:  # Increased depth limit
            logger.warning(f"Max depth reached at {url}")
            return []
        
        all_data = []
        try:
            self.driver.get(url)
            self.human_pause(2.0, 3.5)
            self.handle_cookies()
            
            # Remove redundant scroll_to_bottom here as parse_listing_page handles it
            # ✅ FIX 1: Check if this is a PRODUCT listing page (has product cards)
            product_cards = self.parse_listing_page()
            
            if product_cards:
                logger.info(f"✅ PRODUCT PAGE FOUND! Initial products: {len(product_cards)} at {url}")
                
                # 🔄 PROGRESSIVE LINK COLLECTION: Store links in a set to avoid loss
                links_cache = {} # Using dict to store {url: initial_data}
                
                def capture_visible_links():
                    found_cards = self.parse_listing_page()
                    for card in found_cards:
                        if card['Link'] not in links_cache:
                            links_cache[card['Link']] = card

                # Capture first page links
                capture_visible_links()

                # 🎯 Get the total goal
                total_goal = self.get_total_products_count()
                if total_goal:
                    logger.info(f"   🎯 TARGET FOUND: This category has {total_goal} total products.")
                else:
                    logger.warning("   ⚠️  Could not find total product count, will scrape until button disappears.")

                logger.info("   ⏳ Loading and capturing links progressively...")
                consecutive_fails = 0
                while True:
                    current_count = len(links_cache)
                    
                    # 1. Check if we reached the total goal (STRICT)
                    if total_goal and current_count >= total_goal:
                        logger.info(f"   ✨ Goal reached! Captured {current_count}/{total_goal} products.")
                        break
                    
                    # 2. Reached user's max_products limit
                    if max_products and current_count >= max_products:
                        logger.info(f"   🛑 Reached user-defined limit ({current_count}/{max_products})")
                        break
                    
                    # 3. Try to navigate to next page
                    if self.go_to_next_page():
                        self.human_pause(2.0, 4.0)
                        capture_visible_links()
                        
                        # RE-CHECK TOTAL GOAL (If it was ??? before)
                        if not total_goal:
                            total_goal = self.get_total_products_count()
                            if total_goal:
                                logger.info(f"   🎯 TARGET UPDATED: Found {total_goal} total products.")

                        logger.info(f"   🔄 Progress: Captured {len(links_cache)} of {total_goal if total_goal else '???'} products.")
                        consecutive_fails = 0
                    else:
                        # RETRY LOGIC: If button missing but goal not reached
                        if total_goal and len(links_cache) < total_goal:
                            consecutive_fails += 1
                            logger.warning(f"   ⚠️  Button missing but only have {len(links_cache)}/{total_goal}. Retrying scroll {consecutive_fails}/3...")
                            self.scroll_to_bottom()
                            time.sleep(3)
                            if consecutive_fails >= 3:
                                logger.error(f"   ❌ Forced stop: Could only find {len(links_cache)} links despite retries.")
                                break
                        else:
                            logger.info(f"   🏁 No more 'Load More' buttons. Total captured: {len(links_cache)}")
                            break
                
                # Now work with the final list from memory
                final_cards = list(links_cache.values())
                
                if final_cards:
                    logger.info(f"   🚀 Starting detail extraction for {len(final_cards)} products...")
                    
                    if max_products:
                        final_cards = final_cards[:max_products]
                    
                    # Enrich products with details (parallel)
                    enriched = self.scrape_products_parallel(final_cards, max_workers=2)
                    all_data.extend(enriched)
                    self.check_and_save_incrementally(enriched)
                    
                    logger.info(f"   💾 Finished. Category Total: {len(all_data)}")
                
                return all_data
            
            # ✅ FIX 2: This is a CATEGORY page - find subcategories
            logger.info(f"📂 Category page detected at depth {depth}. Looking for subcategories...")
            
            # Multiple strategies to find subcategory links
            sub_links = self.find_subcategory_links()
            
            if sub_links:
                logger.info(f"Found {len(sub_links)} subcategories to explore")
                for link in sub_links:
                    if max_products and len(all_data) >= max_products: 
                        break
                    
                    remaining = (max_products - len(all_data)) if max_products else None
                    sub_data = self.scrape_category_recursive(link, max_products=remaining, depth=depth+1)
                    all_data.extend(sub_data)
            else:
                logger.warning(f"Dead end - no products or subcategories found at {url}")
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
        
        return all_data

    def find_subcategory_links(self):
        """IMPROVED: Better subcategory detection including 'Shop all' buttons"""
        links = set()
        
        # Strategy 0: User Specific "Shop all" Buttons (High Priority)
        # Button: <div data-test-id="hyperlink" ...><a href="...">Shop all...</a></div>
        try:
            shop_all_btns = self.driver.find_elements(By.CSS_SELECTOR, "div[data-test-id='hyperlink'] a")
            for elem in shop_all_btns:
                href = elem.get_attribute('href')
                if href and ('.cat' in href or 'offers' in href):
                    logger.info(f"   🎯 Found 'Shop all' link: {href}")
                    links.add(href)
        except: pass

        # Strategy 1: Shop by type sections
        try:
            shop_sections = self.driver.find_elements(By.CSS_SELECTOR, "div[class*='category'] a, section a[href*='/departments/']")
            for elem in shop_sections:
                href = elem.get_attribute('href')
                if href and '.cat' in href and 'departments' in href:
                    links.add(href)
        except: pass
        
        # Strategy 2: Navigation menu links
        try:
            # Exclude header/footer to stay focused
            nav_links = self.driver.find_elements(By.CSS_SELECTOR, "nav a[href*='/departments/'], aside a[href*='/departments/']")
            for elem in nav_links:
                href = elem.get_attribute('href')
                if href and '.cat' in href:
                    links.add(href)
        except: pass
        
        # Strategy 3: Category card links
        try:
            cards = self.driver.find_elements(By.CSS_SELECTOR, "a[data-test-id*='category'], a[class*='category']")
            for elem in cards:
                href = elem.get_attribute('href')
                if href and '.cat' in href:
                    links.add(href)
        except: pass
        
        # Strategy 4: Any link with .cat extension in main content
        try:
            # Narrow down to main content to avoid clutter
            main_area = self.driver.find_elements(By.CSS_SELECTOR, "main a[href*='.cat'], article a[href*='.cat']")
            for elem in main_area:
                href = elem.get_attribute('href')
                if href and ('departments' in href or 'offers' in href):
                    links.add(href)
        except: pass
        
        # Filter out unwanted links
        filtered = []
        for link in links:
            # Skip these
            if any(skip in link.lower() for skip in ['offers', 'clearance', 'trending', 'new-in']):
                continue
            # Skip if it's the same as current URL
            if link == self.driver.current_url:
                continue
            filtered.append(link)
        
        return list(set(filtered))[:20]  # Limit to 20 subcategories max

    def go_to_next_page(self):
        """
        Navigate to next page of product listing.
        DIY.com uses "Load more" button/link with href to next page.
        Returns True if successful, False if no more pages.
        """
        try:
            # DIY.com specific: Look for "Load more" or "Next" links
            next_selectors = [
                "a[data-testid='next-page']",                       # Primary selector
                "a[aria-label='Next page']",                        # Aria label
                "//a[contains(., 'Load more')]",                    # Text-based
                "//a[contains(., 'Next')]",                         # Next text
                "//button[contains(., 'Load more')]",               # Button variant
                "a[class*='pagination'][class*='next']"             # Generic pagination
            ]
            
            for selector in next_selectors:
                try:
                    # Use XPath for text-based selectors
                    if selector.startswith("//"):
                        next_btn = self.driver.find_element(By.XPATH, selector)
                    else:
                        next_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if next_btn and next_btn.is_displayed():
                        # Get the href (DIY uses links, not buttons for pagination)
                        href = next_btn.get_attribute('href')
                        
                        if href and 'page=' in href:
                            # CLICK instead of GET to handle AJAX Load More
                            logger.info(f"   🔄 Clicking 'Load More' (Page {href.split('page=')[-1].split('&')[0]})...")
                            
                            # Scroll button into view first
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_btn)
                            time.sleep(1)
                            
                            try:
                                self.safe_click(next_btn)
                                self.human_pause(3.0, 5.5)
                                return True
                            except:
                                # Fallback to GET if click fails
                                self.driver.get(href)
                                self.human_pause(4.0, 6.0)
                                return True
                        
                        elif href:
                            # Has href but no page parameter - try clicking
                            logger.info(f"   🔄 Clicking 'Load More'...")
                            self.safe_click(next_btn)
                            self.human_pause(3.0, 5.0)
                            return True
                        else:
                            # No href - try clicking (AJAX load more)
                            logger.info(f"   🔄 Clicking 'Load More' button...")
                            self.safe_click(next_btn)
                            self.human_pause(3.0, 5.0)
                            return True
                            
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            # No "Load More" button found
            logger.debug("   ℹ️  No 'Load More' button found - end of products")
            return False
            
        except Exception as e:
            logger.debug(f"Pagination check failed: {e}")
            return False

    def get_total_products_count(self):
        """Robustly extract total product count (Y) handling HTML comments and specific classes"""
        try:
            # 1. Target the specific class user found
            elements = self.driver.find_elements(By.CSS_SELECTOR, "p.pt-lg.text-center")
            for el in elements:
                # Use innerHTML to see through React/Next.js comments <!-- -->
                html_content = el.get_attribute('innerHTML')
                # Find all numbers in the HTML (e.g., 26 and 694)
                nums = re.findall(r'>(\d[\d,]*)\s*<', ">" + html_content + "<")
                if not nums: # Fallback to standard digit extraction
                    nums = re.findall(r'[\d,]+', el.text)
                
                if len(nums) >= 2:
                    return int(nums[1].replace(',', ''))

            # 2. General fallback (Case Insensitive)
            paragraphs = self.driver.find_elements(By.TAG_NAME, "p")
            for p in paragraphs:
                text = p.text.upper()
                if "SHOWING" in text and "OF" in text:
                    nums = re.findall(r'[\d,]+', text)
                    if len(nums) >= 2:
                        return int(nums[1].replace(',', ''))
        except:
            pass
        return None

    def parse_listing_page(self):
        """
        Extract product cards from current page.
        DIY.com uses: <ul data-testid="product-list"> with <li> items inside
        """
        self.scroll_to_bottom()
        
        # Anti-Bot / Block Detection (Navigation Layer)
        try:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            if "Sorry, our techies are currently working" in page_text or "Access Denied" in self.driver.title:
                logger.warning(f"⚠️  BLOCK DETECTED. Restarting driver and cooling down...")
                self.driver.quit()
                time.sleep(random.randint(45, 90))
                self._init_driver()
                self.driver.get(self.driver.current_url)
                self.human_pause(5.0, 8.0)
        except: pass

        products = []
        
        # STRATEGY 1: UL[data-testid='product-list'] > LI (PRIMARY for DIY.com)
        try:
            # Wait for list to appear
            wait = WebDriverWait(self.driver, 15)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul[data-testid='product-list']")))
            
            product_list = self.driver.find_element(By.CSS_SELECTOR, "ul[data-testid='product-list']")
            items = product_list.find_elements(By.TAG_NAME, 'li')
            
            if items and len(items) >= 1:
                logger.info(f"✅ Found {len(items)} products total on page")
                
                for itm in items:
                    try:
                        # Find all links in the LI item
                        all_links = itm.find_elements(By.TAG_NAME, 'a')
                        
                        # Find the product detail link (.prd)
                        product_link = None
                        for link in all_links:
                            href = link.get_attribute('href')
                            if href and '.prd' in href:
                                product_link = link
                                break
                        
                        if not product_link:
                            continue
                        
                        link = product_link.get_attribute('href')
                        
                        # Skip category links (should never happen but safety check)
                        if '.cat' in link:
                            continue
                        
                        # Extract product name - try multiple methods
                        name = (product_link.get_attribute('aria-label') or 
                               product_link.get_attribute('title') or 
                               product_link.text.strip())
                        
                        # Fallback: Find any text element with content
                        if not name or len(name) < 3:
                            try:
                                text_elems = itm.find_elements(By.CSS_SELECTOR, "h2, h3, p, span")
                                for elem in text_elems:
                                    text = elem.text.strip()
                                    if text and len(text) > 3 and text not in ['Compare', 'Filter', 'Sort']:
                                        name = text
                                        break
                            except:
                                pass
                        
                        if not name:
                            name = "Product"
                        
                        # Extract SKU from URL (e.g., /product-name/3663602792062.prd -> 3663602792062)
                        sku = link.split('/')[-1].replace('.prd', '').split('_')[0] if '.prd' in link else "N/A"
                        
                        # skip sponsored products
                        if "sponsored" in itm.text.lower():
                            continue

                        if link:
                            products.append({
                                "Name": name,
                                "Link": link,
                                "SKU": sku,
                                "Supplier": "B&Q"
                            })
                            
                    except Exception as e:
                        logger.debug(f"Error parsing LI item: {e}")
                        continue
                
                # Return immediately if we found products
                if products:
                    logger.info(f"   ✅ Extracted {len(products)} product links")
                    return products
                    
        except Exception as e:
            logger.debug(f"Strategy 1 (UL > LI) failed: {e}")
        
        # STRATEGY 2: Fallback selectors for other page structures
        fallback_selectors = [
            "a[data-test-id='product-primary-image-link']",  # Image links
            "div[data-component='ProductCard']",
            "article[data-component='ProductCard']",
            "div[class*='ProductCard']"
        ]
        
        for selector in fallback_selectors:
            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                if items and len(items) >= 3:
                    logger.info(f"Found {len(items)} items using fallback: {selector}")
                    
                    for itm in items:
                        try:
                            # Find product link
                            link_el = None
                            if itm.tag_name == 'a':
                                link_el = itm
                            else:
                                possible_links = itm.find_elements(By.TAG_NAME, 'a')
                                for plink in possible_links:
                                    href = plink.get_attribute('href')
                                    if href and '.prd' in href:
                                        link_el = plink
                                        break
                            
                            if not link_el:
                                continue
                            
                            link = link_el.get_attribute('href')
                            
                            if '.cat' in link or '.prd' not in link:
                                continue
                            
                            name = link_el.get_attribute('aria-label') or link_el.get_attribute('title')
                            if not name:
                                try:
                                    name_el = itm.find_element(By.CSS_SELECTOR, "p, span, h3, h2")
                                    name = name_el.text.strip()
                                except:
                                    name = "Product"
                            
                            sku = link.split('/')[-1].split('.prd')[0] if '.prd' in link else "N/A"
                            
                            if link and name:
                                products.append({
                                    "Name": name,
                                    "Link": link,
                                    "SKU": sku,
                                    "Supplier": "B&Q"
                                })
                        except Exception as e:
                            continue
                    
                    if products:
                        logger.info(f"   ✅ Extracted {len(products)} products (fallback)")
                        return products
                        
            except Exception as e:
                logger.debug(f"Fallback selector {selector} failed: {e}")
                continue
        
        # If we reach here, no products found
        logger.warning("   ⚠️  No products found with any selector")
        return products

    # ==========================================
    # 5. DATA EXTRACTION & CLEANING
    # ==========================================
    def get_product_details(self, url):
        """Fetch details from a single product page with Block-Detection"""
        try:
            self.driver.get(url)
            self.human_pause(2.0, 4.0)
            
            # 🛡️ CHECK FOR BLOCK SCREEN
            page_source = self.driver.page_source.lower()
            if "techies are currently working" in page_source or "access denied" in self.driver.title.lower():
                logger.warning(f"🛡️  B&Q BLOCKED ACCESS on {url}. Restarting driver...")
                self.driver.quit()
                time.sleep(random.randint(60, 120))
                self._init_driver()
                self.driver.get(url)
                self.human_pause(5.0, 8.0)
            
            # Only handle cookies if banner is visible (saves time)
            try:
                if self.driver.find_elements(By.ID, "onetrust-banner-sdk"):
                    self.handle_cookies()
            except: pass

            self.human_move_mouse(1)
        except:
            pass

        details = {
            "Name": 'N/A', "Price_Inc_VAT": 0.0, "All_Images": "N/A", "Region": self.current_region,
            "SKU": "N/A", "Supplier": "B&Q", "Brand": "N/A", "Quantity": "N/A",
            "Pieces_in_Pack": "N/A", "Pack_Size": "N/A", "Coverage_M2": "N/A", "Volume_M3": "N/A",
            "Product_Length_M": "N/A", "Product_Width": "N/A", "Product_Thickness": "N/A",
            "Product_Weight_Kg": "N/A", "Product_Type": "N/A", "Material": "N/A", 
            "description": "N/A", "Pack_Type": "N/A"
        }

        # 5a. JSON-LD (Power Move)
        try:
            scripts = self.driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
            for script in scripts:
                data = json.loads(script.get_attribute('innerHTML'))
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "Product":
                        details["Name"] = item.get("name", details["Name"])
                        details["SKU"] = item.get("sku", details["SKU"])
                        if "brand" in item: 
                            details["Brand"] = item["brand"].get("name") if isinstance(item["brand"], dict) else item["brand"]
                        if "offers" in item:
                            off = item["offers"]
                            details["Price_Inc_VAT"] = float(off[0].get("price", 0)) if isinstance(off, list) else float(off.get("price", 0))
                        if "image" in item: 
                            img_data = item["image"]
                            if isinstance(img_data, list):
                                cleaned_imgs = [self._clean_image_url(img) for img in img_data]
                                details["All_Images"] = ", ".join(cleaned_imgs)
                            else:
                                details["All_Images"] = self._clean_image_url(img_data)
        except: pass

        # 5a.2. Fallback Image Extraction (User specific structure)
        if details["All_Images"] == "N/A":
            try:
                images = []
                # Broader selector to include images with 'object-contain' or within 'picture'
                pics = self.driver.find_elements(By.XPATH, "//img[contains(@class, 'object-contain')] | //picture//img | //div[contains(@id, 'product-media')]//img")
                for img in pics:
                    # Check srcset for high-res versions first, then src, then data-src
                    src = img.get_attribute("srcset") or img.get_attribute("src") or img.get_attribute("data-src")
                    
                    if src and "placeholder" not in src:
                        # Clean and add
                        cleaned = self._clean_image_url(src)
                        if cleaned not in images and cleaned.startswith("http"):
                            images.append(cleaned)
                if images:
                    details["All_Images"] = ", ".join(images)
            except: pass

        # 5b. Description & Quantity

        try:
            # User provided specific class for description
            desc_elem = self.driver.find_element(By.XPATH, "//div[@data-testid='product-details-description'] | //div[contains(@class, 'product-details-description')]")
            details["description"] = desc_elem.text.strip()
        except: 
             try:
                details["description"] = self.driver.find_element(By.CSS_SELECTOR, "p[data-test-id='product-description']").text.strip()
             except: pass
        
        try:
            # Updated to use aria-label or name as provided by user
            qty_input = self.driver.find_element(By.XPATH, "//input[@aria-label='Quantity'] | //input[@name='quantity'] | //input[contains(@class, 'text-center') and @type='number']")
            details["Quantity"] = qty_input.get_attribute("value")
        except: pass

        # 5c. Name & Price (Fallback if JSON-LD fails)
        if details["Name"] == "N/A":
             try:
                 details["Name"] = self.driver.find_element(By.XPATH, "//h1[@data-testid='product-name']").text.strip()
             except: pass
             
        if details["Price_Inc_VAT"] == 0.0:
             try:
                 p_text = self.driver.find_element(By.XPATH, "//span[@data-testid='product-price']").text.strip()
                 details["Price_Inc_VAT"] = float("".join(c for c in p_text if c.isdigit() or c == '.'))
             except: pass

        # 5c. Specifications Table - FIXED VERSION
        raw_dims = {"length": "N/A", "width": "N/A", "height": "N/A", "depth": "N/A", "thickness": "N/A"}
        try:
            rows = self.driver.find_elements(By.CSS_SELECTOR, "tr[class*='specification'], table tr")
            for row in rows:
                # Robust extraction: get all cells (TH and TD)
                cells = row.find_elements(By.TAG_NAME, "th") + row.find_elements(By.TAG_NAME, "td")
                
                header = None
                value = None

                if len(cells) >= 2:
                    header = cells[0]
                    value = cells[1]

                if header and value:
                    k, v = header.text.strip().lower(), value.text.strip()
                    
                    # 1. Product Code -> SKU
                    if "product code" in k and details["SKU"] in ["N/A", ""]: 
                        details["SKU"] = v
                    
                    # 2. Brand
                    elif "brand" in k:
                        details["Brand"] = v
                        
                    # 3. Material
                    elif "material" in k:
                        details["Material"] = v
                        
                    # 4. Coverage
                    elif "coverage" in k:
                        details["Coverage_M2"] = self._clean_val(v)
                        
                    # 5. Pack Quantity -> Pieces
                    elif "pack quantity" in k:
                        details["Pieces_in_Pack"] = v
                        # Sometimes Quantity is also 1 if it's a single item pack
                    
                    # 6. Weight Extraction (Consolidated & Independent)
                    elif "weight" in k:
                        details["Product_Weight_Kg"] = self._clean_weight(v)
                    
                    # 7. Dimensions (Refined to exclude sub-components like 'hose' or 'spout')
                    else:
                        is_sub_component = any(x in k for x in ["hose", "spout", "cable", "handle"])
                        
                        if not is_sub_component:
                            if "height" in k:
                                raw_dims["height"] = self._clean_dim(v)
                            elif "width" in k or "diameter" in k:
                                raw_dims["width"] = self._clean_dim(v)
                            elif "depth" in k:
                                raw_dims["depth"] = self._clean_dim(v)
                            elif "thickness" in k:
                                raw_dims["thickness"] = self._clean_dim(v)
                            elif "length" in k:
                                raw_dims["length"] = self._clean_dim(v)

                        # 8. Height x Width (cm) - Composite
                        if "height x width" in k:
                            try:
                                # Extract all numbers
                                nums = re.findall(r"(\d+(?:\.\d+)?)", v)
                                if len(nums) >= 2:
                                    raw_dims["height"] = self._clean_dim(f"{nums[0]}cm")
                                    raw_dims["width"] = self._clean_dim(f"{nums[1]}cm")
                            except: pass

                    # 9. Other Fields
                    if "product type" in k: details["Product_Type"] = v
                    elif "pack size" in k: details["Pack_Size"] = v
                    elif any(x in k for x in ["pack type", "container", "packaging"]):
                        details["Pack_Type"] = v.upper()
                    elif "volume" in k or "capacity" in k: details["Volume_M3"] = self._clean_vol(v)

        except: pass

        # Apply User's Specific Dimension Logic (Refined for Safety)
        details["Product_Width"] = raw_dims["width"]
        
        # 1. Length Priority
        if raw_dims["length"] != "N/A":
            details["Product_Length_M"] = raw_dims["length"]
        else:
            details["Product_Length_M"] = raw_dims["height"]

        # 2. Thickness Priority
        if raw_dims["depth"] != "N/A":
            details["Product_Thickness"] = raw_dims["depth"]
        elif raw_dims["height"] != "N/A" and details["Product_Length_M"] != raw_dims["height"]:
            details["Product_Thickness"] = raw_dims["height"]
        else:
            details["Product_Thickness"] = raw_dims["thickness"]

        # 5d. Post-Process (Same math as Screwfix)
        details["raw_dims_for_math"] = raw_dims # Temporary for post-process
        self._post_process(details)
        if "raw_dims_for_math" in details: del details["raw_dims_for_math"]
        
        return details

    def _post_process(self, d):
        text_blob = f"{d['Name']} {d['description']}".lower()
        raw = d.get("raw_dims_for_math", {})
        
        # 1. Identify all numeric values
        L = raw.get("length") if isinstance(raw.get("length"), float) else None
        W = raw.get("width") if isinstance(raw.get("width"), float) else None
        H = raw.get("height") if isinstance(raw.get("height"), float) else None
        T = (raw.get("thickness") if isinstance(raw.get("thickness"), float) else 
             (raw.get("depth") if isinstance(raw.get("depth"), float) else None))

        # 2. Coverage & Volume Logic based on your specific combinations
        if d["Pieces_in_Pack"] != "N/A":
            try:
                pieces = float(self._clean_val(d["Pieces_in_Pack"]))
                
                # Case 1: Length, Height, Thickness
                if L and H and T:
                    if d["Coverage_M2"] == "N/A": d["Coverage_M2"] = pieces * L * H
                    if d["Volume_M3"] == "N/A": d["Volume_M3"] = L * H * T
                
                # Case 2: Length, Width, Thickness
                elif L and W and T:
                    if d["Coverage_M2"] == "N/A": d["Coverage_M2"] = pieces * L * W
                    if d["Volume_M3"] == "N/A": d["Volume_M3"] = L * W * T
                
                # Case 3: Height, Width, Thickness
                elif H and W and T:
                    if d["Coverage_M2"] == "N/A": d["Coverage_M2"] = pieces * H * W
                    if d["Volume_M3"] == "N/A": d["Volume_M3"] = H * W * T
                
                # Case 4: Length, Width, Height
                elif L and W and H:
                    if d["Coverage_M2"] == "N/A": d["Coverage_M2"] = pieces * L * W
                    if d["Volume_M3"] == "N/A": d["Volume_M3"] = L * W * H
                
                # Fallback for just 2 dims (Simple Coverage)
                elif d["Coverage_M2"] == "N/A":
                    dim1 = L or H
                    dim2 = W or T
                    if dim1 and dim2:
                        d["Coverage_M2"] = pieces * dim1 * dim2
            except: pass

        # 3. Packaging Logic (Removed keyword fallback as requested)
        pass

        # 1. Identify 3 primary numeric dimensions (Meters)
        L = d["Product_Length_M"] if isinstance(d["Product_Length_M"], float) else None
        W = d["Product_Width"] if isinstance(d["Product_Width"], float) else None
        T = d["Product_Thickness"] if isinstance(d["Product_Thickness"], float) else None
        
        # 2. Coverage Math (M2) - Minimum 2 dims + Pieces
        if d["Coverage_M2"] == "N/A" and d["Pieces_in_Pack"] != "N/A":
            try:
                pieces = float(self._clean_val(d["Pieces_in_Pack"]))
                # If we have L and either W or T, we can find coverage
                dim1 = L
                dim2 = W if W else T
                if dim1 and dim2:
                    d["Coverage_M2"] = pieces * dim1 * dim2
            except: pass

        # 3. Volume Math (M3) - Requires 3 dims
        if d["Volume_M3"] == "N/A":
            if L and W and T:
                d["Volume_M3"] = L * W * T
            elif L and W: # Fallback if T is missing but we have L and W
                pass # Can't do volume without 3rd dim

    # Helpers
    def _clean_dim(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        if not m: return "N/A"
        val = float(m.group(1))
        if 'mm' in str(v).lower(): return val/1000
        if 'cm' in str(v).lower(): return val/100
        return val

    def _clean_weight(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        if not m: return "N/A"
        val = float(m.group(1))
        if 'kg' in str(v).lower(): return val
        if 'g' in str(v).lower() and 'kg' not in str(v).lower(): return val / 1000
        return val

    def _clean_val(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        return float(m.group(1)) if m else "N/A"

    def _clean_vol(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        if not m: return "N/A"
        val = float(m.group(1))
        if 'ml' in str(v).lower(): return val/1e6
        if any(x in str(v).lower() for x in ['ltr', 'liter', ' l']): return val/1000
        return val

    def _clean_image_url(self, url):
        """Decodes HTML entities, handles srcset safely, and forces High-Res"""
        if not url or not isinstance(url, str): return "N/A"
        
        # 1. Decode &amp; to &
        url = url.replace("&amp;", "&")
        
        # 2. Handle srcset strings (B&Q specific)
        # ONLY split if the comma is followed by a space and a density descriptor (1x, 2x, etc)
        if "," in url and re.search(r",\s*\S+\s+\d+x", url):
            # Pick the last (highest res) entry
            url = url.split(",")[-1].strip().split(" ")[0]
        elif "," in url and url.count("http") > 1:
            # Fallback for comma-separated URLs without descriptors
            url = url.split(",")[-1].strip()
            
        # 3. Strip existing query parameters to avoid conflicts
        if "?" in url:
            url = url.split("?")[0]
            
        # 4. Return Clean Base URL (B&Q / Scene7)
        # Simply stripping query parameters (done in step 3) already gives the full-size original image.
        # No extra modifiers (like wid=1000 or fit) are added to avoid parsing errors and low-res defaults.
        return url
    
    # ==========================================
    # 6. PARALLEL EXECUTION (Twin of Screwfix)
    # ==========================================
    # ==========================================
    # 6. PARALLEL EXECUTION -> SEQUENTIAL (Anti-Bot Safe)
    # ==========================================
    def scrape_products_parallel(self, products, max_workers=2):
        """
        MODIFIED: Now runs SEQUENTIALLY to avoid bot detection.
        Ignores max_workers.
        """
        results = []
        total = len(products)
        logger.info(f"🛡️  Running SEQUENTIAL scrape for {total} products (Anti-Bot Mode)...")
        
        for idx, p in enumerate(products, 1):
            # 1. Skip if already in DB
            if db_utils.product_exists(p['Link']):
                logger.info(f"⏭️  Skipping (DB): {p.get('Name')}")
                continue

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # ✅ Check if driver is still alive, restart if needed
                    try:
                        _ = self.driver.current_window_handle
                    except:
                        logger.warning("🔄 Browser session lost. Restarting driver...")
                        try: self.close()
                        except: pass
                        self._init_driver()
                        self.driver.get(self.base_url)
                        self.handle_cookies()

                    if idx == 1:
                        # Set a random target for the next long break
                        self.next_break_at = random.randint(7, 14)

                    logger.info(f"[{idx}/{len(products)}] Fetching details: {p.get('Name')}")
                    
                    self.human_pause(2.5, 6.0) # Normal inter-product pause
                    
                    # ☕ FULLY DYNAMIC RANDOM BREAKS
                    if idx >= self.next_break_at:
                        sleep_time = random.uniform(20, 45)
                        logger.info(f"☕ Taking a dynamic human break ({sleep_time:.1f}s)...")
                        
                        # Instead of just sleeping, perform human actions
                        end_time = time.time() + sleep_time
                        while time.time() < end_time:
                            self.human_move_mouse()
                            self.driver.execute_script(f"window.scrollBy(0, {random.randint(-200, 300)});")
                            time.sleep(random.uniform(3, 7))
                        
                        # Set next random target
                        self.next_break_at = idx + random.randint(7, 15)

                    details = self.get_product_details(p['Link'])
                    
                    # Smart Update
                    for key, value in details.items():
                        if key not in p: p[key] = value
                        elif value != "N/A": p[key] = value

                    # Save immediately
                    db_utils.save_product(p)
                    results.append(p)
                    break # Success, move to next product

                except Exception as e:
                    logger.error(f"❌ Attempt {attempt+1}/{max_retries} failed for {p.get('Name')}: {e}")
                    if "reset" in str(e).lower() or "connection" in str(e).lower() or "refused" in str(e).lower():
                        logger.warning("🌐 Network error detected. Waiting 60s before retry...")
                        time.sleep(60)
                    else:
                        break # Other errors don't necessarily need retry
                    
            if idx % 20 == 0:
                logger.info(f"📊 Progress: Scraped {idx}/{len(products)} products in this category.")

        return results

    def check_and_save_incrementally(self, new_data):
        """Save progress incrementally to both CSV and Excel"""
        self.all_scraped_data.extend(new_data)
        
        # Save to CSV every 15 products
        if len(self.all_scraped_data) - self.last_save_count >= 15:
            pd.DataFrame(self.all_scraped_data).to_csv("diy_products_incremental.csv", index=False)
            logger.info(f"💾 CSV: Saved {len(self.all_scraped_data)} products")
            self.last_save_count = len(self.all_scraped_data)
        
        # Save to Excel every 10 products
        if len(self.all_scraped_data) % 10 == 0 and len(self.all_scraped_data) > 0:
            try:
                df = pd.DataFrame(self.all_scraped_data)
                df.to_excel("diy_products_incremental.xlsx", index=False, engine='openpyxl')
                logger.info(f"📊 EXCEL: Saved {len(self.all_scraped_data)} products to diy_products_incremental.xlsx")
            except Exception as e:
                logger.warning(f"Could not save Excel file: {e}")

    def close(self):
        self.driver.quit()

if __name__ == "__main__":
    db_utils.create_table()
    scraper = DIYScraper(headless=False)
    try:
        scraper.driver.get(scraper.base_url)
        scraper.handle_cookies()
        
        # Final Test: Scrape ONE specific small sub-category for quick verification
        target_url = "https://www.diy.com/kitchen/kitchen-taps.cat?Navigation+type=Mixer"
        logger.info(f"🚀 QUICK VERIFICATION: Starting scraper for sub-category: {target_url}")
        
        # Scrape ALL products in this sub-category
        data = scraper.scrape_category_recursive(target_url, max_products=None) 
        
        # Final save
        if data:
            pd.DataFrame(data).to_csv("diy_building_materials_final.csv", index=False)
            logger.info(f"✅ DIY Production Job Finished. Total products: {len(data)}")
        else:
            logger.warning("⚠️  No products found!")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
    finally:
        scraper.close()