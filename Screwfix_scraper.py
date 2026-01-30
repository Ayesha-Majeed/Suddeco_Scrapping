import os
import json
import time
import pandas as pd
import logging
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


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("screwfix_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScrewfixScraper:
    def __init__(self, headless=True, margin=0.20):
        self.base_url = "https://www.screwfix.com"
        self.margin = margin
        self.current_region = "UK (Default)"
        self.all_scraped_data = [] # Tracker for incremental saving
        self.last_save_count = 0
        
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new") # Modern headless mode
            chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--remote-debugging-pipe")
        
        # ROTATING USER AGENTS
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        chosen_ua = random.choice(user_agents)
        chrome_options.add_argument(f"user-agent={chosen_ua}")
        
        logger.info("Initializing Chrome Driver...")
        
        # GPU and Performance Optimization Flags
        chrome_options.add_argument("--enable-gpu") 
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-extensions")
        
        try:
            # Prefer ChromeDriverManager for version compatibility
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 30)
            logger.info("Driver initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize driver: {e}")
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 30)

    def safe_click(self, element):
        """Attempts normal click, falls back to JS click."""
        try:
            element.click()
        except:
            logger.info("Normal click failed, trying JS click...")
            self.driver.execute_script("arguments[0].click();", element)

    def scroll_to_bottom(self, pause_time=1):
        """Scrolls to bottom of page using multiple methods for reliability."""
        logger.info("Scrolling to load all elements...")
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            for _ in range(15): # Max 15 attempts to reach bottom
                # Method 1: JS scrollTo
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # Method 2: END key
                body.send_keys(Keys.END)
                time.sleep(pause_time)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception as e:
            logger.warning(f"Scroll to bottom failed: {e}")
        
        # Scroll back to top
        self.driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)

    def _collect_links_with_progressive_scroll(self, xpath, scroll_increment=600):
        """
        Progressively scroll down and collect links using keyboard and JS.
        """
        logger.info("Progressive scrolling to collect all links...")
        collected_links = set()
        current_scroll = 0
        no_new_links_count = 0
        
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            for i in range(20): # Max 20 scrolls
                # Scroll via keys (more reliable for triggering events)
                body.send_keys(Keys.PAGE_DOWN)
                time.sleep(1.0) # Wait for load
                
                # Also JS scroll to be sure
                current_scroll += scroll_increment
                self.driver.execute_script(f"window.scrollTo(0, {current_scroll});")
                
                elements = self.driver.find_elements(By.XPATH, xpath)
                previous_count = len(collected_links)
                
                for elem in elements:
                    try:
                        href = elem.get_attribute('href')
                        if href and '/p/' in href:
                            collected_links.add(href)
                        elif href and 'cat' in href: # Category links
                            collected_links.add(href)
                    except: continue
                
                if len(collected_links) == previous_count and i > 5:
                    no_new_links_count += 1
                    if no_new_links_count >= 3: break
                else:
                    no_new_links_count = 0
                
                # Check height to avoid infinite loop
                max_h = self.driver.execute_script("return document.body.scrollHeight")
                if current_scroll > max_h: break
        except Exception as e:
            logger.warning(f"Progressive scroll failed: {e}")
            
        return list(collected_links)

    def handle_cookies(self):
        """Clicks the 'Accept Cookies' button if it appears."""
        logger.info("Checking for cookie consent...")
        try:
            selectors = [
                 (By.XPATH, "//button[contains(text(), 'Accept Cookies')]"), # From user Screenshot
                 (By.XPATH, "//*[@id='onetrust-accept-btn-handler']"),
                 (By.XPATH, "//button[contains(text(), 'Accept All')]"),
                 (By.XPATH, "//a[contains(text(), 'Accept Cookies')]")
            ]
            
            for by, val in selectors:
                try:
                    btn = self.driver.find_element(by, val)
                    if btn.is_displayed():
                        self.safe_click(btn)
                        logger.info("Cookies accepted.")
                        try:
                            # Wait for banner to disappear
                            self.wait.until(EC.invisibility_of_element_located(by))
                        except: pass
                        return
                except:
                    continue
            logger.info("Cookie banner check done.")
        except Exception as e:
            logger.warning(f"Cookie check ignored: {e}")

    def set_location(self, postcode="E1 6AN"):
        """Sets the store location using precise IDs from user HTML."""
        logger.info(f"Setting location to: {postcode}")
        try:
            # Re-check cookies before location as it might block
            self.handle_cookies()
            
            # 1. Click Store Locator Link
            try:
                store_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@id='header_find_store_link'] | //a[@id='header_find_store_link'] | //span[contains(text(), 'Store locator')]")))
                self.safe_click(store_btn)
            except Exception as e:
                logger.warning(f"Store locator button click failed: {e}")
                # Fallback: navigate directly to store locator if possible
                self.driver.get(f"{self.base_url}/stores")
            
            # 2. Enter Postcode
            search_input = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='store-locator-search']")))
            search_input.clear()
            search_input.send_keys(postcode)
            search_input.send_keys(u'\ue007') # Enter key
            
            time.sleep(2) # Wait for list to update
            
            # 3. Select Store ('Select' or 'Set as my store')
            try:
                select_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Set as') or contains(., 'Collection')]")))
                self.safe_click(select_btn)
                logger.info(f"Store for {postcode} selected successfully.")
                time.sleep(1)
            except:
                logger.warning("Could not find 'Set as' button, might already be selected.")

        except Exception as e:
            logger.error(f"Failed to set location: {e}")

    def _scrape_listing_and_handle_pagination(self):
        """Scrapes products from current page and all subsequent pages via pagination."""
        all_listing_products = []
        page_cnt = 1
        while True:
            logger.info(f"Scraping Listing Page {page_cnt}...")
            self.scroll_to_bottom(pause_time=1)
            
            items = self.parse_results_page()
            if items:
                all_listing_products.extend(items)
                logger.info(f"Collected {len(items)} products from page {page_cnt} (Total: {len(all_listing_products)})")
            
            # Pagination Logic
            try:
                # User provided snippet: <a data-qaid="pagination-button-next" ...>
                next_btn = self.driver.find_element(By.XPATH, "//a[@data-qaid='pagination-button-next']")
                href = next_btn.get_attribute('href')
                
                # Check if it's a valid link (sometimes disabled button exists)
                if href and 'javascript' not in href and next_btn.is_displayed():
                    logger.info(f"Navigating to Page {page_cnt + 1}...")
                    self.driver.get(href)
                    # Wait for product grid updates
                    try:
                        self.wait.until(EC.staleness_of(next_btn))
                        self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x1__pJ') or starts-with(@id, 'product-card-')]")))
                    except: pass
                    page_cnt += 1
                    continue
            except:
                logger.info("No next page found or pagination ended.")
            
            break
        return all_listing_products

    def navigate_all_categories(self, max_products=None):
        """Navigates through all 13 main Screwfix departments."""
        target_categories = [
            "https://www.screwfix.com/c/tools/cat830034",
            "https://www.screwfix.com/c/heating-plumbing/cat830950",
            "https://www.screwfix.com/c/electrical-lighting/cat840780",
            "https://www.screwfix.com/c/bathrooms-kitchens/cat810412",
            "https://www.screwfix.com/c/outdoor-gardening/cat840458",
            "https://www.screwfix.com/c/screws-nails-fixings/cat840002",
            "https://www.screwfix.com/c/security-ironmongery/cat4190012",
            "https://www.screwfix.com/c/building-doors/cat850188",
            "https://www.screwfix.com/c/safety-workwear/cat850322",
            "https://www.screwfix.com/c/sealants-adhesives/cat850030",
            "https://www.screwfix.com/c/storage-ladders/cat831422",
            "https://www.screwfix.com/c/auto-cleaning/cat7360001",
            "https://www.screwfix.com/c/painting-decorating/cat850130"
        ]
        
        all_data = []
        for url in target_categories:
            if max_products and len(all_data) >= max_products:
                logger.info(f"Reached global limit of {max_products} products. Stopping.")
                break
                
            logger.info(f"--- STARTING DEPARTMENT: {url} ---")
            # Calculate remaining products needed
            remaining = max_products - len(all_data) if max_products else None
            cat_data = self.scrape_category_recursive(url, max_products=remaining)
            all_data.extend(cat_data)
        return all_data

    def scrape_category_recursive(self, category_url, max_products=None, depth=0):
        """Recursively scrapes products from a category page or its sub-categories up to 3 levels deep."""
        if depth > 3: 
            logger.info(f"Max depth reached for {category_url}. Returning.")
            return []
            
        all_data = []
        try:
            self.driver.get(category_url)
            time.sleep(1.5)
            
            # 1. Check if products exist on this page right now
            initial_products = self.parse_results_page()
            if initial_products:
                logger.info(f"    Products found at {category_url}. Scraping pagination...")
                basic_products = self._scrape_listing_and_handle_pagination()
                
                if basic_products:
                    if max_products:
                        space_left = max_products - len(all_data)
                        if space_left <= 0: return all_data
                        basic_products = basic_products[:space_left]
                        
                    enriched_data = self.scrape_products_parallel(basic_products, max_workers=2)
                    all_data.extend(enriched_data)
                    self.check_and_save_incrementally(enriched_data)
                
                # If we found products, we usually don't need to look for sub-cats on the same page
                return all_data

            # 2. If no products, collect sub-categories
            sub_cat_links = self._collect_links_with_progressive_scroll(
                xpath="""
                    //div[@data-qaid="image-grid-tile"]//a |
                    //a[contains(@class, 'sub-cat')] |
                    //div[contains(@class, 'category')]//a |
                    //div[contains(@class, 'range-list')]//a |
                    //div[contains(@class, 'sl-visual-nav')]//a |
                    //a[h3] |
                    //a[contains(@class, 'dxKLYq')]
                """,
                scroll_increment=500
            )

            if sub_cat_links:
                logger.info(f"Depth {depth}: Found {len(sub_cat_links)} sub-links at {category_url}")
                for sub_link in sub_cat_links:
                    if max_products and len(all_data) >= max_products:
                        break
                    
                    # Prevent circular or duplicate navigation
                    if sub_link == category_url or '/p/' in sub_link:
                        continue
                        
                    remaining = max_products - len(all_data) if max_products else None
                    sub_data = self.scrape_category_recursive(sub_link, max_products=remaining, depth=depth+1)
                    all_data.extend(sub_data)
            else:
                logger.info(f"No products or sub-categories found at {category_url}")

        except Exception as e:
            logger.error(f"Error scraping category {category_url}: {e}")
            
        return all_data

    def scrape_all_pages(self, start_url):
        """Scrapes all paginated product pages starting from start_url."""
        self.driver.get(start_url)
        all_products = []
        page_num = 1
        while True:
            self.scroll_to_bottom(pause_time=1)
            products = self.parse_results_page()
            logger.info(f"Page {page_num}: Found {len(products)} products.")
            all_products.extend(products)
    
            # Pagination: Find "Next Page" button
            try:
                next_btn = self.driver.find_element(By.XPATH, "//a[@data-qaid='pagination-button-next']")
                if next_btn.is_displayed():
                    next_url = next_btn.get_attribute('href')
                    if next_url and not next_url.startswith('javascript'):
                        full_next_url = next_url
                        if not next_url.startswith("http"):
                            full_next_url = self.base_url + next_url
                        self.driver.get(full_next_url)
                        page_num += 1
                        time.sleep(2)
                        continue
            except Exception as e:
                logger.info("No more pages found or error: " + str(e))
            break
        return all_products
        
    def parse_results_page(self):
        """Extracts basic product info (Name, Link, SKU) from a listing page."""
        products = []
        try:
            # Flexible selector for product containers
            product_cards_xpath = """
                //div[contains(@id, 'product-card')] | 
                //div[contains(@class, 'product-card')] |
                //div[@data-qaid='product-listing-item'] |
                //li[contains(@class, 'product-listing')]
            """
            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, product_cards_xpath)))
            except: pass
            
            items = self.driver.find_elements(By.XPATH, product_cards_xpath)
            
            if not items:
                # Fallback: maybe they aren't in cards, just links?
                logger.info("No product cards found, searching for direct links...")
                links = self.driver.find_elements(By.XPATH, "//a[@data-qaid='product_description'] | //a[contains(@class, 'product-link')]")
                for link_el in links:
                    try:
                        name = self.driver.execute_script("return arguments[0].innerText;", link_el).strip()
                        link = link_el.get_attribute('href')
                        if link and '/p/' in link:
                            products.append({
                                "Name": name or "Product",
                                "Link": link,
                                "SKU": link.split('/')[-1],
                                "Supplier": "Screwfix"
                            })
                    except: continue
                return products

            logger.info(f"Found {len(items)} product elements.")
        except Exception as e:
            logger.warning(f"Failed to find product elements: {e}")
            return []

        for item in items:
            try:
                # Robust link/name extraction within card
                name_elem = item.find_element(By.XPATH, ".//a[@data-qaid='product_description'] | .//h3//a | .//a[contains(@class, 'product-title')]")
                name = self.driver.execute_script("return arguments[0].innerText;", name_elem).strip()
                link = name_elem.get_attribute('href')

                if not link or '/p/' not in link: continue

                try:
                    sku = item.get_attribute('id').replace('product-card-', '')
                    if not sku or len(sku) > 10: raise Exception()
                except:
                    sku = link.split('/')[-1]
                
                products.append({
                    "Name": name,
                    "Link": link,
                    "SKU": sku,
                    "Supplier": "Screwfix"
                })
            except: continue
        return products

    def clean_dim(self, val_str):
        """Extract numeric value and convert to meters."""
        if not val_str or val_str == 'N/A': return "N/A"
        val_str = str(val_str).lower().strip()
        factor = 1.0
        
        if 'mm' in val_str: factor = 0.001
        elif 'cm' in val_str: factor = 0.01
        elif 'm' in val_str and 'mm' not in val_str: factor = 1.0
        elif '"' in val_str or 'inch' in val_str or 'imperial' in val_str: factor = 0.0254
        
        clean_val = ""
        for c in val_str:
            if c.isdigit() or c == '.':
                clean_val += c
        
        if not clean_val: return "N/A"
        return float(clean_val) * factor

    def clean_vol(self, val_str):
        """Extract numeric value and convert to cubic meters (m3)."""
        if not val_str or val_str == 'N/A': return "N/A"
        val_str = str(val_str).lower().strip()
        factor = 1.0
        
        # 1 ml = 1e-6 m3
        # 1 litre = 0.001 m3
        if 'ml' in val_str: factor = 1e-6
        elif 'ltr' in val_str or 'liter' in val_str or 'litre' in val_str: factor = 0.001
        elif 'm3' in val_str: factor = 1.0
        
        clean_val = ""
        for c in val_str:
            if c.isdigit() or c == '.':
                clean_val += c
                
        if not clean_val: return "N/A"
        return float(clean_val) * factor

    def clean_area(self, val_str):
        """Extract numeric value and convert to square meters (m2)."""
        if not val_str or val_str == 'N/A': return "N/A"
        val_str = str(val_str).lower().strip()
        factor = 1.0
        
        if 'mm2' in val_str or 'mm²' in val_str: factor = 1e-6
        elif 'cm2' in val_str or 'cm²' in val_str: factor = 0.0001
        elif 'm2' in val_str or 'm²' in val_str: factor = 1.0
        
        clean_val = ""
        for c in val_str:
            if c.isdigit() or c == '.':
                clean_val += c
                
        if not clean_val: return "N/A"
        return float(clean_val) * factor

    def get_product_details(self, url):
        logger.info(f"Deep scanning: {url}")
        
        # 1. 403 handling: Try direct, if blocked, go to home then try again
        self.driver.get(url)
        time.sleep(2)
        try:
            self.handle_cookies()
        except:
            pass
        try:
            self.wait.until(lambda d: d.execute_script('return document.readyState')=='complete')
        except:
            time.sleep(1)
        
        # 1. 403/504 handling: Detection of blocks/timeouts
        if "Access Denied" in self.driver.title or "403" in self.driver.title or "504" in self.driver.title or "the request could not be satisfied" in self.driver.page_source.lower():
            logger.warning(f"Block/Timeout (504/403) detected on {url}. Waiting 60s before homepage bypass...")
            time.sleep(60) # Longer wait for cooldown
            self.driver.get("https://www.screwfix.com")
            time.sleep(5)
            self.handle_cookies()
            self.driver.get(url)
            time.sleep(3)

        # 2. Wait for page title/basic layout
        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, "//h1")))
            time.sleep(1) # Small settle
        except:
            logger.warning(f"Main product header presence timeout for {url}.")
    
        # 3. Check for "Specifications" tab/button and click it
        # This is often needed on mobile/tablet views or certain PDP layouts
        try:
            spec_triggers = [
                "//button[contains(., 'Specifications')]",
                "//a[contains(., 'Specifications')]",
                "//span[contains(., 'Specifications')]",
                "//*[@id='specifications-label']"
            ]
            for trigger in spec_triggers:
                elements = self.driver.find_elements(By.XPATH, trigger)
                for el in elements:
                    if el.is_displayed():
                        self.driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.5)
                        break
        except: pass

        # 4. Human-like Scroll to trigger all lazy-loaded content
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            for _ in range(8):
                body.send_keys(Keys.PAGE_DOWN)
                time.sleep(0.3)
            
            # Final scroll to ensure spec is in DOM
            self.driver.execute_script("window.scrollTo(0, 2500);")
            time.sleep(1.0)
        except: pass
        
        # Final scan scroll
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)
        
        # 5. Wait for specification section specifically
        try:
             # Look for table with partial class 'specification' or just any table if in a spec container
             self.wait.until(EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'specification')] | //div[contains(@id, 'specification')]//table | //table")))
             time.sleep(0.5)
        except: 
             logger.warning("Specification table presence timeout.")
    
        # ==========================================
        # UPDATED DETAILS DICTIONARY
        # ==========================================
        details = {
            "Name": 'N/A',
            "Price_Inc_VAT": 0.0,
            "All_Images": "N/A",
            "Region": self.current_region,
            "SKU": "N/A",
            "Supplier": "Screwfix",
            "Brand": "N/A",
            "Quantity": "N/A",
            "Pieces_in_Pack": "N/A",
            "Coverage_M2": "N/A",
            "Volume_M3": "N/A",
            "Product_Length_M": "N/A",
            "Product_Width": "N/A",
            "Product_Thickness": "N/A",
            "Product_Weight_Kg": "N/A",
            "Product_Type": "N/A",
            "Material": "N/A",
            "description": "N/A"
        }

        # ==========================================
        # 1. BASIC INFO EXTRACTION (High Priority)
        # ==========================================
    
        try:
            # Broad XPath for name
            name_xpath = "//h1[@itemprop='name'] | //h1[contains(@class, 'product-name')] | //h1[@data-qaid='pdp-product-name']"
            name_elem = self.driver.find_element(By.XPATH, name_xpath)
            full_text = self.driver.execute_script("return arguments[0].innerText;", name_elem).strip()
            
            if '(' in full_text and ')' in full_text:
                details["Name"] = full_text.split('(')[0].strip()
            else:
                details["Name"] = full_text
        except:
            logger.warning(f"Could not extract product name for {url}. Capturing screenshot.")
            try:
                self.driver.save_screenshot(f"error_{details['SKU'] or 'notsku'}.png")
                details["Name"] = self.driver.find_element(By.XPATH, "//meta[@property='og:title']").get_attribute("content")
            except:
                pass

        # SKU
        try:
            sku_elem = self.driver.find_element(By.XPATH, "//span[@data-qaid='pdp-product-id'] | //h1//span[contains(., '(')]")
            details["SKU"] = sku_elem.get_attribute("innerText").replace('(', '').replace(')', '').strip()
        except:
            try:
                sku_meta = self.driver.find_element(By.XPATH, "//meta[@itemprop='sku']")
                details["SKU"] = sku_meta.get_attribute("content")
            except:
                details["SKU"] = url.split('/')[-1]

        # Brand
        try:
            brand_img = self.driver.find_element(By.XPATH, "//img[@data-qaid='pdp-brand-logo'] | //img[contains(@class, 'brand-logo')]")
            details["Brand"] = brand_img.get_attribute("alt").strip()
        except:
            pass

        # Price (Inner Text)
        try:
            price_elem = self.driver.find_element(By.XPATH, "//span[@itemprop='price'] | //div[@data-qaid='pdp-price']//span")
            price_text = price_elem.get_attribute("innerText").strip()
            clean_price = "".join(c for c in price_text if c.isdigit() or c == '.')
            if clean_price:
                details["Price_Inc_VAT"] = float(clean_price)
        except:
            pass

        # ==========================================
        # 2. JSON-LD EXTRACTION (Most Reliable)
        # ==========================================
        try:
            scripts = self.driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
            for script in scripts:
                try:
                    inner_html = script.get_attribute('innerHTML')
                    data = json.loads(inner_html)
                    if isinstance(data, list):
                        for item in data: self._extract_json_ld(item, details)
                    else:
                        self._extract_json_ld(data, details)
                except: continue
        except: pass

        # ==========================================
        # 3. IMAGES EXTRACTION
        # ==========================================
        try:
            images = []
        
            # Try thumbnail gallery first
            img_elems = self.driver.find_elements(By.XPATH, 
            "//div[@data-qaid='product-images_thumbnails']//img | " +
            "//div[contains(@class, 'product-image')]//img | " +
            "//div[@class='image-gallery']//img"
            )
        
            for img in img_elems:
                src = img.get_attribute("src") or img.get_attribute("data-src")
                if src:
                    # Remove query parameters and get high-res version
                    clean_src = src.split('?')[0].rstrip(',')
                    clean_src = clean_src.replace('_small', '').replace('_medium', '').replace('_thumbnail', '')
                    if clean_src and clean_src not in images and 'placeholder' not in clean_src.lower():
                        images.append(clean_src)
        
            if images:
                details["All_Images"] = ", ".join(images)
        except Exception as e:
            logger.warning(f"Image extraction failed: {e}")

        # ==========================================
        # 4. SPECIFICATION TABLE SCRAPING (Enhanced)
        # ==========================================
        try:
            # Wait for specification table to be present
            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, "//table | //div[contains(@class, 'specification')]")))
            except:
                pass
        
            # Find all table rows across different table structures
            rows = self.driver.find_elements(By.XPATH, "//table[contains(@class, 'specification')]//tr | //div[contains(@class, 'specification')]//div[contains(@class, 'row')] | //dl[contains(@class, 'specification')]//div | //table//tr")
        
            logger.info(f"Found {len(rows)} specification rows")
        
            for row in rows:
                try:
                    # Use execute_script to get text from columns to bypass visibility issues
                    cols = row.find_elements(By.XPATH, ".//td")
                    if not cols:
                         cols = row.find_elements(By.XPATH, ".//dt | .//dd")
                    
                    if len(cols) >= 2:
                        key = self.driver.execute_script("return arguments[0].innerText;", cols[0]).strip().lower()
                        val = self.driver.execute_script("return arguments[0].innerText;", cols[1]).strip()
                    else:
                        continue
                    
                    if not val or val == '-': continue
                    
                    # ===== MAPPING LOGIC =====
                    
                    # VOLUME / WEIGHT TRAP
                    if "volume" in key:
                        val_lower = val.lower()
                        if "kg" in val_lower:
                            # It's actually weight!
                            details["Product_Weight_Kg"] = val
                            
                            # Only estimate Volume if it's an aggregate (sand, gravel, stone, topsoil)
                            item_name = details["Name"].lower()
                            material_types = ["sand", "gravel", "aggregate", "ballast", "stone", "cobble", "pebble", "topsoil", "chippings", "bulk bag"]
                            
                            if any(m in item_name for m in material_types):
                                try:
                                    numeric_kg = float("".join(c for c in val if c.isdigit() or c == '.'))
                                    # Use differentiated factors if needed, but 0.0006 is standard for aggregate bulk bags (~1.6 ton/m3)
                                    if details["Volume_M3"] == "N/A":
                                        details["Volume_M3"] = f"{numeric_kg * 0.0006:.4f} m3 (Est. from density)"
                                        logger.info(f"Smart Calc: Estimated volume for aggregate {item_name}")
                                except: pass
                        else:
                            details["Volume_M3"] = self.clean_vol(val)

                    # PIECES IN PACK
                    elif "pieces in pack" in key:
                         details["Pieces_in_Pack"] = val
                    
                    # LENGTH
                    elif any(x in key.lower() for x in ["product length", "length", "roll length", "cable length"]):
                        val_dim = self.clean_dim(val)
                        if "(metric)" in key.lower() or details["Product_Length_M"] == "N/A":
                            details["Product_Length_M"] = val_dim
                    
                    # WIDTH
                    elif "width" in key.lower():
                        val_dim = self.clean_dim(val)
                        if "(metric)" in key.lower() or details["Product_Width"] == "N/A":
                            details["Product_Width"] = val_dim
                    
                    # THICKNESS / HEIGHT / DEPTH
                    elif any(x in key.lower() for x in ["thickness", "depth", "height"]):
                        # Only accept if it has 'product' OR it's the specific '(metric)' field
                        if "product" in key.lower() or "(metric)" in key.lower():
                            val_dim = self.clean_dim(val)
                            if "(metric)" in key.lower() or details["Product_Thickness"] == "N/A":
                                details["Product_Thickness"] = val_dim
                    
                    # WEIGHT
                    elif "weight" in key:
                        if "shipping" not in key and "package" not in key:
                            details["Product_Weight_Kg"] = val
                    
                    # TYPE
                    elif "product type" in key or "type" == key:
                        details["Product_Type"] = val
                    
                    # COVERAGE
                    elif "coverage" in key:
                        details["Coverage_M2"] = self.clean_area(val)
                    
                    # MATERIAL
                    elif "material" in key:
                        details["Material"] = val
            
                except Exception as e:
                    logger.debug(f"Error processing row: {e}")
                    continue
                
        except Exception as e:
            logger.warning(f"Spec table scraping failed: {e}")

        # ==========================================
        # 4.5 EXTRA FIELDS (User Requested)
        # ==========================================
        # Quantity from Input
        try:
            qty_input = self.driver.find_element(By.XPATH, "//input[@id='qty'] | //input[@data-qaid='pdp-product-quantity']")
            details["Quantity"] = qty_input.get_attribute("value")
        except:
            pass

        # ==========================================
        # 5. DESCRIPTION & BULLETS EXTRACTION
        # ==========================================
        desc_parts = []
        
        # Keep any description already found (e.g., from JSON-LD)
        current_desc = details.get("description", "N/A")
        if current_desc != "N/A" and current_desc.strip():
            desc_parts.append(current_desc.strip())

        # Try to find more paragraph text
        desc_selectors = [
            "//p[@data-qaid='pdp-product-overview']",  # New primary selector
            "//*[@id='product_additional_details_container']",
            "//*[@itemprop='description']",
            "//div[contains(@class, 'product-description')]",
            "//div[@data-qaid='pdp-description']"
        ]
        for selector in desc_selectors:
            try:
                # Use find_element to avoid multiple matches
                desc_elem = self.driver.find_element(By.XPATH, selector)
                text = self.driver.execute_script("return arguments[0].innerText;", desc_elem).strip()
                if text and len(text) > 30:
                    # Check if this text is already mostly covered by current desc
                    is_duplicate = False
                    for existing in desc_parts:
                        if text[:50] in existing or existing[:50] in text:
                            is_duplicate = True
                            break
                    if not is_duplicate:
                        desc_parts.append(text)
                        break
            except:
                continue

        # Extract bullets - Use find_elements but take only the first visible one
        try:
            bullet_xpath = "//ul[@data-qaid='pdp-product-bullets']//li | //ul[contains(@class, '_5QgGW8')]//li"
            bullet_elems = self.driver.find_elements(By.XPATH, bullet_xpath)
            if bullet_elems:
                # Group by parent to identify different instances, take first parent's children
                first_parent = bullet_elems[0].find_element(By.XPATH, "..")
                bullets_list = [self.driver.execute_script("return arguments[0].innerText;", b).strip() for b in bullet_elems if b.find_element(By.XPATH, "..") == first_parent]
                bullets_list = [b for b in bullets_list if b]
                
                if bullets_list:
                    bullet_text = "\n".join([f"• {b}" for b in bullets_list])
                    # Check if bullets are already in the description parts
                    first_bullet_sample = bullets_list[0][:30]
                    if not any(first_bullet_sample in existing for existing in desc_parts):
                        desc_parts.append("Key Features:\n" + bullet_text)
        except:
            pass

        if desc_parts:
            # Join with double newline for readability
            details["description"] = "\n\n".join(desc_parts)

        # ==========================================
        # 6. AUTOMATIC VOLUME CALCULATION
        # ==========================================
        if details["Volume_M3"] == "N/A":
            if all(details[x] != "N/A" for x in ["Product_Length_M", "Product_Width", "Product_Thickness"]):
                try:
                    # Values are already converted to meters (floats)
                    l = details["Product_Length_M"]
                    w = details["Product_Width"]
                    t = details["Product_Thickness"]
                
                    if isinstance(l, (int, float)) and isinstance(w, (int, float)) and isinstance(t, (int, float)):
                        if l > 0 and w > 0 and t > 0:
                            calc_vol = l * w * t
                            details["Volume_M3"] = f"{calc_vol:.6f} m3 (Calculated)"
                            logger.info(f"[v] Calculated Volume: {calc_vol:.6f} m3")
                except Exception as e:
                    logger.debug(f"Volume calculation failed: {e}")

        # ==========================================
        # 7. FINAL VALIDATION & LOGGING
        # ==========================================
        extracted_fields = [k for k, v in details.items() if v != "N/A" and v != 0.0]
        logger.info(f"[v] Extracted {len(extracted_fields)} fields: {', '.join(extracted_fields)}")
    
        missing_fields = [k for k, v in details.items() if v == "N/A"]
        if missing_fields:
            logger.warning(f"[x] Missing fields: {', '.join(missing_fields)}")

        return details

    def _extract_json_ld(self, data, details):
        """Helper to extract fields from a JSON-LD dict."""
        if not isinstance(data, dict): return
        
        if data.get("@type") == "Product":
            # SKU
            if "sku" in data:
                details["SKU"] = data["sku"]
            
            # Brand
            if "brand" in data:
                if isinstance(data["brand"], dict):
                    details["Brand"] = data["brand"].get("name", "N/A")
                else:
                    details["Brand"] = str(data["brand"])
            
            # Description (JSON-LD often has a clean description)
            if "description" in data:
                details["description"] = data["description"]

            # Images - Use COMMA separator
            if "image" in data:
                imgs = data["image"]
                if isinstance(imgs, list):
                    details["All_Images"] = ", ".join(imgs)
                elif isinstance(imgs, str):
                    details["All_Images"] = imgs
            
            # Price
            if "offers" in data:
                offers = data["offers"]
                if isinstance(offers, list) and len(offers) > 0:
                    details["Price_Inc_VAT"] = float(offers[0].get("price", 0.0))
                elif isinstance(offers, dict):
                    details["Price_Inc_VAT"] = float(offers.get("price", 0.0))

    def check_and_save_incrementally(self, new_data):
        """Adds new data to the master list and saves every 20 items."""
        self.all_scraped_data.extend(new_data)
        current_total = len(self.all_scraped_data)
        
        # Save if we've reached a new multiple of 20
        if current_total - self.last_save_count >= 20:
            logger.info("*" * 50)
            logger.info(f"--- BATCH REACHED: Saving {current_total} items to CSV/Excel ---")
            logger.info("*" * 50)
            self.save_to_file(self.all_scraped_data, "screwfix_building_materials_london.csv")
            self.save_to_file(self.all_scraped_data, "screwfix_building_materials_london.xlsx")
            self.last_save_count = current_total

    def save_to_file(self, data, filename="screwfix_products.csv"):
        if not data: return
        df = pd.DataFrame(data)
        if filename.endswith('.xlsx'):
            df.to_excel(filename, index=False)
        else:
            df.to_csv(filename, index=False)
        logger.info(f"Saved {len(data)} items to {filename}")

    def close(self):
        if hasattr(self, 'driver'):
            self.driver.quit()

    def scrape_products_parallel(self, products_list, max_workers=5):
        """
        Takes a list of basic product dicts (Name, Link, SKU) and fetches details in parallel.
        """
        logger.info(f"Starting parallel scraping for {len(products_list)} products using {max_workers} workers...")
        
        fully_scraped_data = []
        
        # Split products into chunks for each worker
        # Determine chunk size roughly
        chunk_size = max(1, len(products_list) // max_workers)
        chunks = [products_list[i:i + chunk_size] for i in range(0, len(products_list), chunk_size)]
        
        # Define the task for each worker
        # We pass the class configuration to replicate the environment
        postcode_to_use = "E1 6AN" # defaulting to what is used in main
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(run_worker_batch, chunk, postcode_to_use): chunk 
                for chunk in chunks
            }
            
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    result_batch = future.result()
                    fully_scraped_data.extend(result_batch)
                    logger.info(f"Worker batch finished. Total collected so far: {len(fully_scraped_data)}")
                except Exception as exc:
                    logger.error(f"Worker generated an exception: {exc}")

        return fully_scraped_data

# --- Worker Function (Standalone) ---
def run_worker_batch(products_chunk, postcode):
    """
    Independent worker function that creates its own browser instance,
    sets location, and scrapes a batch of products.
    """
    worker_results = []
    # Initialize a new scraper instance for this thread
    # We use headless=True for workers to keep it cleaner, limit GPU use to background
    scraper = ScrewfixScraper(headless=True) 
    
    try:
        scraper.driver.get(scraper.base_url)
        # Random sleep to stagger start
        time.sleep(random.uniform(2, 5)) 
        
        scraper.handle_cookies()
        time.sleep(random.uniform(1, 2))

        try:
             scraper.set_location(postcode)
        except:
             logger.warning("Worker failed to set location - proceeding with default.")
             
        time.sleep(random.uniform(1, 3))
        
        total_in_chunk = len(products_chunk)
        for idx, item in enumerate(products_chunk, 1):
            try:
                # 1. Skip if already in DB
                if db_utils.product_exists(item['Link']):
                    logger.info(f"[{idx}/{total_in_chunk}] Skipping (Already in DB): {item.get('Name')}")
                    continue

                logger.info(f"[{idx}/{total_in_chunk}] Fetching details for: {item.get('Name')}...")
                # Random delay between products
                time.sleep(random.uniform(3.0, 6.0))
                details = scraper.get_product_details(item['Link'])
                
                # Smart Update: Don't overwrite existing valid data with "N/A"
                for key, value in details.items():
                    # If the key doesn't exist in item, add it (even if N/A)
                    if key not in item:
                        item[key] = value
                    # If the key exists, only overwrite if the new value is NOT "N/A"
                    elif value != "N/A":
                        item[key] = value
                
                # Region fix
                if item.get("Region") == "UK (Default)":
                    item["Region"] = scraper.current_region
                
                worker_results.append(item)
                
                # 2. Save to DB immediately
                db_utils.save_product(item)
                logger.info(f"[{idx}/{total_in_chunk}] Saved to DB: {item.get('Name')}")
            except Exception as e:
                logger.error(f"[{idx}/{total_in_chunk}] Worker failed on {item.get('Link')}: {e}")
                
    except Exception as e:
        logger.error(f"Worker setup failed: {e}")
    finally:
        scraper.close()
        
    return worker_results

if __name__ == "__main__":
    # Initialize Database and Tables
    try:
        db_utils.create_products_db()
    except Exception as e:
        logger.info(f"Database 'Products' info: {e}")
    db_utils.create_table()

    scraper = ScrewfixScraper(headless=True)
    try:
        scraper.driver.get("https://www.screwfix.com")
        # 1. Handle Cookies (Critical)
        scraper.handle_cookies()
        # 2. Set Location (London)
        scraper.set_location("E1 6AN")
        # 3. Scrape All Departments
        logger.info("Starting Full Production Scrape (All Departments)...")
        data = scraper.navigate_all_categories(max_products=None)
        # 4. Save
        scraper.save_to_file(data, "screwfix_building_materials_london.csv")
        scraper.save_to_file(data, "screwfix_building_materials_london.xlsx")
        logger.info("Task Completed!")
    except Exception as e:
        logger.error(f"Critical Error: {e}")
    finally:
        scraper.close()