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

        self.all_scraped_data = [] 
        self.last_save_count = 0
        
        # BROWSERS OPTIONS (Same as Screwfix)
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        w = random.randint(1400, 1920)
        h = random.randint(900, 1080)
        chrome_options.add_argument(f"--window-size={w},{h}")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--remote-debugging-pipe")
        
        # ROTATING USER AGENTS (Premium Protection)
        # ROTATING USER AGENTS (Premium Protection - Expanded)
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36"
        ]
        chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
        chrome_options.add_argument("--disable-extensions")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 25)
        except:
            self.driver = webdriver.Chrome(options=chrome_options)
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
            self.scroll_to_bottom()
            
            # ✅ FIX 1: Check if this is a PRODUCT listing page (has product cards)
            product_cards = self.parse_listing_page()
            
            if product_cards:
                logger.info(f"✅ PRODUCT PAGE FOUND! {len(product_cards)} products at {url}")
                
                # 🔄 PAGINATION LOOP: Get ALL pages of products
                page_num = 1
                max_pages = 50  # Safety limit
                
                while page_num <= max_pages:
                    logger.info(f"   📄 Scraping page {page_num}...")
                    
                    # Get products from current page
                    if page_num == 1:
                        # Already got first page above
                        current_page_products = product_cards
                    else:
                        current_page_products = self.parse_listing_page()
                    
                    if current_page_products:
                        logger.info(f"   ✅ Page {page_num}: Found {len(current_page_products)} products")
                        
                        # Check product limit
                        if max_products:
                            space_left = max_products - len(all_data)
                            if space_left <= 0:
                                logger.info(f"   🛑 Product limit reached ({max_products})")
                                break
                            current_page_products = current_page_products[:space_left]
                        
                        # Enrich products with details (parallel)
                        enriched = self.scrape_products_parallel(current_page_products, max_workers=2)
                        all_data.extend(enriched)
                        self.check_and_save_incrementally(enriched)
                        
                        logger.info(f"   💾 Collected {len(enriched)} products (Total: {len(all_data)})")
                        
                        # Check if we've reached the limit
                        if max_products and len(all_data) >= max_products:
                            logger.info(f"   🛑 Reached product limit of {max_products}")
                            break
                    else:
                        logger.warning(f"   ⚠️  No products found on page {page_num}")
                        break
                    
                    # Try to navigate to next page
                    if not self.go_to_next_page():
                        logger.info(f"   ✅ All pages scraped. Total pages: {page_num}")
                        break
                    
                    page_num += 1
                
                if page_num > max_pages:
                    logger.warning(f"   ⚠️  Reached safety limit of {max_pages} pages")
                
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
                            # Extract page number for logging
                            page_num = href.split('page=')[-1].split('&')[0]
                            logger.info(f"   🔄 Loading page {page_num}...")
                            
                            # Navigate to next page URL
                            self.driver.get(href)
                            self.human_pause(2.5, 4.0)
                            self.scroll_to_bottom()
                            
                            return True
                        elif href:
                            # Has href but no page parameter - still navigate
                            logger.info(f"   🔄 Loading more products...")
                            self.driver.get(href)
                            self.human_pause(2.5, 4.0)
                            self.scroll_to_bottom()
                            return True
                        else:
                            # No href - try clicking (AJAX load more)
                            logger.info(f"   🔄 Clicking 'Load More' button...")
                            self.safe_click(next_btn)
                            self.human_pause(3.0, 5.0)
                            self.scroll_to_bottom()
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
                logger.warning(f"⚠️  BLOCK DETECTED during navigation. Sleeping for 120s...")
                time.sleep(120)
                self.driver.refresh()
                self.human_pause(5.0, 8.0)
        except: pass

        products = []
        
        # STRATEGY 1: UL[data-testid='product-list'] > LI (PRIMARY for DIY.com)
        try:
            product_list = self.driver.find_element(By.CSS_SELECTOR, "ul[data-testid='product-list']")
            items = product_list.find_elements(By.TAG_NAME, 'li')
            
            if items and len(items) >= 1:
                logger.info(f"✅ Found {len(items)} products using ul[data-testid='product-list'] > li")
                
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
        self.driver.get(url)
        # Anti-Bot / Block Detection
        try:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            if "Sorry, our techies are currently working" in page_text or "Access Denied" in self.driver.title:
                logger.warning(f"⚠️  BLOCK DETECTED on {url}. Sleeping for 60s...")
                time.sleep(60)
                self.driver.get(url) # Retry once
                self.human_pause(3.0, 5.0)
        except: pass

        self.human_pause(2.5, 4.5) # Increased pause
        self.handle_cookies()
        self.human_move_mouse()

        details = {
            "Name": 'N/A', "Price_Inc_VAT": 0.0, "All_Images": "N/A", "Region": self.current_region,
            "SKU": "N/A", "Supplier": "B&Q", "Brand": "N/A", "Quantity": "N/A",
            "Pieces_in_Pack": "N/A", "Pack_Size": "N/A", "Coverage_M2": "N/A", "Volume_M3": "N/A",
            "Product_Length_M": "N/A", "Product_Width": "N/A", "Product_Thickness": "N/A",
            "Product_Weight_Kg": "N/A", "Product_Type": "N/A", "Material": "N/A", 
            "description": "N/A"
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
                            details["All_Images"] = ", ".join(item["image"]) if isinstance(item["image"], list) else item["image"]
        except: pass

        # 5a.2. Fallback Image Extraction (User specific structure)
        if details["All_Images"] == "N/A":
            try:
                images = []
                # Look for picture tags first (high res)
                pics = self.driver.find_elements(By.XPATH, "//picture//img | //div[contains(@class, 'product-media')]//img")
                for img in pics:
                    src = img.get_attribute("src") or img.get_attribute("data-src")
                    if src and "placeholder" not in src:
                        # Clean up URL params for DIY.com images if needed
                        if "?" in src: src = src.split("?")[0]
                        if src not in images:
                            images.append(src)
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
            qty_input = self.driver.find_element(By.CSS_SELECTOR, "input[data-test-id='quantity-input']")
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
                    
                    # 6. Dimensions (Simple)
                    elif k == "height (cm)" or k == "product height":
                        raw_dims["height"] = self._clean_dim(v)
                    elif k == "width (cm)" or k == "product width":
                        raw_dims["width"] = self._clean_dim(v)
                    elif k == "depth (cm)" or k == "product depth":
                        raw_dims["depth"] = self._clean_dim(v)
                    elif k == "thickness" or k == "product thickness":
                        raw_dims["thickness"] = self._clean_dim(v)
                    
                    # 7. Weight
                    elif "weight" in k:
                         details["Product_Weight_Kg"] = self._clean_val(v)

                    # 8. Length (mm) specific
                    elif "length" in k:
                        raw_dims["length"] = self._clean_dim(v)

                    # 7. Height x Width (cm) - Composite
                    elif "height x width" in k:
                        # (H) 240cm x (W) 60cm
                        try:
                            # Extract all numbers
                            nums = re.findall(r"(\d+(?:\.\d+)?)", v)
                            if len(nums) >= 2:
                                raw_dims["height"] = self._clean_dim(f"{nums[0]}cm")
                                raw_dims["width"] = self._clean_dim(f"{nums[1]}cm")
                        except: pass

                    # 8. Standard Dims
                    if "product width" in k or "width" == k:
                         raw_dims["width"] = self._clean_dim(v)
                    
                    if "product height" in k or "height" == k:
                        raw_dims["height"] = self._clean_dim(v)
                        
                    if "product thickness" in k or "thickness" == k:
                         raw_dims["thickness"] = self._clean_dim(v)

                    if "product weight" in k:
                        details["Product_Weight_Kg"] = v

                    # Existing logic for other fields
                    if "product type" in k: details["Product_Type"] = v
                    elif "pack size" in k: details["Pack_Size"] = v
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
        self._post_process(details)
        
        return details

    def _post_process(self, d):
        text_blob = f"{d['Name']} {d['description']}".lower()
        
        # Packaging Logic (Word Boundary fix applied)
        if re.search(r"\bbulk bag\b|\bbulk\b", text_blob): d["Pack_Type"] = "BULK"
        elif re.search(r"\bbox\b", text_blob): d["Pack_Type"] = "BOX"
        elif re.search(r"\bbag\b", text_blob): d["Pack_Type"] = "BAG"
        elif re.search(r"\broll\b", text_blob): d["Pack_Type"] = "ROLL"
        elif re.search(r"\bcase\b|\bpack\b", text_blob): d["Pack_Type"] = "PACK"

        # Math Calcs (Coverage & Volume)
        if d["Volume_M3"] == "N/A" and all(isinstance(d[x], float) for x in ["Product_Length_M", "Product_Width", "Product_Thickness"]):
             try:
                d["Volume_M3"] = d["Product_Length_M"] * d["Product_Width"] * d["Product_Thickness"]
             except: pass

        # Clean Weight if it got through dirty
        if isinstance(d["Product_Weight_Kg"], str) and d["Product_Weight_Kg"] != "N/A":
             d["Product_Weight_Kg"] = self._clean_val(d["Product_Weight_Kg"])
            
        if d["Coverage_M2"] == "N/A" and all(isinstance(d[x], float) for x in ["Product_Length_M", "Product_Width"]):
            d["Coverage_M2"] = d["Product_Length_M"] * d["Product_Width"]

    # Helpers
    def _clean_dim(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        if not m: return "N/A"
        val = float(m.group(1))
        if 'mm' in str(v).lower(): return val/1000
        if 'cm' in str(v).lower(): return val/100
        return val

    def _clean_val(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        return float(m.group(1)) if m else "N/A"

    def _clean_vol(self, v):
        m = re.search(r"(\d+(?:\.\d+)?)", str(v))
        if not m: return "N/A"
        val = float(m.group(1))
        if 'ml' in str(v).lower(): return val/1e6
        if 'ltr' in str(v).lower() or 'liter' in str(v).lower(): return val/1000
        return val

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
            try:
                # 1. Skip if already in DB
                if db_utils.product_exists(p['Link']):
                    logger.info(f"⏭️  Skipping (DB): {p.get('Name')}")
                    continue

                logger.info(f"[{idx}/{total}] Fetching details: {p.get('Name')}...")
                
                # 2. Fetch Details (Re-using current driver instance if possible? 
                # No, the original design expected a list of dicts and main thread did nothing.
                # Only Workers had drivers. 
                # We can instantate a helper scraper OR just use self.get_product_details since we are in the class instance!
                # Wait, 'self' has a driver open. We can use it!
                
                self.human_pause(2.5, 6.0) # Pause between products
                
                # Randomized longer breaks
                if idx % 10 == 0:
                     sleep_time = random.uniform(10, 20)
                     logger.info(f"☕ Taking a short break ({sleep_time:.1f}s)...")
                     time.sleep(sleep_time)

                details = self.get_product_details(p['Link'])
                
                # Smart Update
                for key, value in details.items():
                    if key not in p: p[key] = value
                    elif value != "N/A": p[key] = value

                # Save immediately
                db_utils.save_product(p)
                results.append(p)
                
            except Exception as e:
                logger.error(f"❌ Error scraping {p.get('Name')}: {e}")
                # If error is blocking related, wait longer
                if "block" in str(e).lower():
                    time.sleep(60)

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

# Standalone Worker Batch (Parallelism)
def _run_diy_worker_batch(product):
    worker_scraper = DIYScraper(headless=False)
    try:
        if db_utils.product_exists(product['Link']): 
            logger.info(f"⏭️  Skipping (already in DB): {product['Name']}")
            return None
        
        logger.info(f"🔍 Worker deep-scanning: {product['Name']}")
        time.sleep(random.uniform(2, 5))
        
        details = worker_scraper.get_product_details(product["Link"])
        product.update(details)
        db_utils.save_product(product)
        
        logger.info(f"✅ Saved: {product['Name']} - £{product.get('Price_Inc_VAT', 'N/A')}")
        return product
    except Exception as e:
        logger.error(f"❌ Error processing {product['Name']}: {str(e)}")
        return None
    finally:
        worker_scraper.close()

if __name__ == "__main__":
    db_utils.create_table()
    scraper = DIYScraper(headless=False)
    try:
        scraper.driver.get(scraper.base_url)
        scraper.handle_cookies()
        
        # Sample Run: Get products from all categories
        logger.info("🚀 Starting DIY scraper...")
        data = scraper.navigate_all_categories(max_products=10)  # Change to None for unlimited
        
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