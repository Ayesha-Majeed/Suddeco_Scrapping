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
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--remote-debugging-pipe")
        
        # ROTATING USER AGENTS (Premium Protection)
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
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

    def human_pause(self, min_s=0.5, max_s=1.8):
        time.sleep(random.uniform(min_s, max_s))

    def human_move_mouse(self, moves=2):
        """Simulates mouse movement to bypass detection."""
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            actions = ActionChains(self.driver)
            for _ in range(random.randint(1, moves)):
                x = random.randint(10, 1000)
                y = random.randint(10, 800)
                actions.move_to_element_with_offset(body, x, y)
                actions.pause(random.uniform(0.1, 0.3))
            actions.perform()
        except:
            pass

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
        except: pass

    # ==========================================
    # 4. NAVIGATION & LISTING
    # ==========================================
    def navigate_all_categories(self, max_products=None):
        target_categories = [
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
            "https://www.diy.com/departments/heating-plumbing-cooling/DIY1652280.cat"
        ]
        
        all_data = []
        for url in target_categories:
            if max_products and len(all_data) >= max_products: break
            logger.info(f"--- STARTING CATEGORY: {url} ---")
            cat_data = self.scrape_category_recursive(url, max_products=(max_products - len(all_data)) if max_products else None)
            all_data.extend(cat_data)
        return all_data

    def scrape_category_recursive(self, url, max_products=None, depth=0):
        if depth > 3: return []
        all_data = []
        try:
            self.driver.get(url)
            self.human_pause(1.5, 3.0)
            self.handle_cookies()
            
            # Check for Products first
            cards = self.parse_listing_page()
            if cards:
                logger.info(f"Products found at {url}. Scraping parallel batch...")
                if max_products: cards = cards[:max_products]
                enriched = self.scrape_products_parallel(cards, max_workers=2)
                all_data.extend(enriched)
                self.check_and_save_incrementally(enriched)
                # If we found products, we don't necessarily need to go deeper into sub-cats of this specific page
                return all_data

            # If NO products, it's likely a Department/Category Landing Page
            logger.info(f"Stepping into landing page: {url}")
            self.scroll_to_bottom()
            self.human_pause(1.0, 2.0)
            
            sub_selectors = [
                "a[data-test-id='category-link']",
                "a[data-testid='category-link']",
                "a[href*='/departments/'][class*='flex']",
                "div[data-component='CategoryCard'] a",
                "ul[data-test-id='category-list'] a"
            ]
            
            links = []
            for sel in sub_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if elements:
                    batch = [l.get_attribute('href') for l in elements if l.get_attribute('href')]
                    links.extend(batch)
            
            unique_links = list(set([l for l in links if l and url.split('.cat')[0] in l]))
            
            if unique_links:
                logger.info(f"Found {len(unique_links)} sub-categories to explore.")
                for link in unique_links:
                    if max_products and len(all_data) >= max_products: break
                    sub_data = self.scrape_category_recursive(link, max_products=(max_products - len(all_data)) if max_products else None, depth=depth+1)
                    all_data.extend(sub_data)
            else:
                logger.debug(f"End of branch at {url}")
        except: pass
        return all_data

    def parse_listing_page(self):
        self.scroll_to_bottom()
        # Try multiple selectors for B&Q product cards
        selectors = [
            "li.group.flex-1", 
            "div[data-component='ProductCard']",
            "div[data-testid='product-card']",
            "div.product-card",
            "li[data-test-id='product-card-container']"
        ]
        
        items = []
        for selector in selectors:
            items = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if items:
                logger.info(f"Found {len(items)} products using selector: {selector}")
                break
        
        if not items:
            logger.info("Landing page detected (no direct product cards). Looking for sub-categories...")
            # Backup: Just find all links that look like products
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/departments/'][data-test-id='product-primary-image-link']")
            if links:
                logger.info(f"Fallback: Found {len(links)} product links via image test-id.")
                products = []
                for l in links:
                    try:
                        href = l.get_attribute('href')
                        name = l.get_attribute('aria-label') or l.text.strip()
                        if not name: # Try to find name in nearby element
                            name = self.driver.execute_script("return arguments[0].closest('li, div').innerText;", l).split('\n')[0]
                        products.append({"Name": name, "Link": href, "SKU": href.split('/')[-1].split('_')[0], "Supplier": "B&Q"})
                    except: continue
                return products
            return []

        products = []
        for itm in items:
            try:
                # B&Q uses data-test-id for key elements
                link_el = itm.find_element(By.CSS_SELECTOR, "a[data-test-id='product-primary-image-link'], a[href*='/departments/']")
                name_el = itm.find_elements(By.CSS_SELECTOR, "p[class*='product-card-title'], span[data-test-id='product-title'], h3")
                
                link = link_el.get_attribute('href')
                name = name_el[0].text.strip() if name_el else link_el.get_attribute('aria-label') or "N/A"
                
                if link and name:
                    products.append({"Name": name, "Link": link, "SKU": link.split('/')[-1].split('_')[0], "Supplier": "B&Q"})
            except: continue
        return products

    # ==========================================
    # 5. DATA EXTRACTION & CLEANING
    # ==========================================
    def get_product_details(self, url):
        self.driver.get(url)
        self.human_pause(2.0, 4.0)
        self.handle_cookies()
        self.human_move_mouse()

        details = {
            "Name": 'N/A', "Price_Inc_VAT": 0.0, "All_Images": "N/A", "Region": self.current_region,
            "SKU": "N/A", "Supplier": "B&Q", "Brand": "N/A", "Quantity": "N/A",
            "Pieces_in_Pack": "N/A", "Pack_Size": "N/A", "Coverage_M2": "N/A", "Volume_M3": "N/A",
            "Product_Length_M": "N/A", "Product_Width": "N/A", "Product_Thickness": "N/A",
            "Product_Weight_Kg": "N/A", "Product_Type": "N/A", "Material": "N/A", 
            "Unit_Type": "N/A", "Pack_Type": "N/A", "Coverage_Per_Item": "N/A", "description": "N/A"
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
                        if "brand" in item: details["Brand"] = item["brand"].get("name") if isinstance(item["brand"], dict) else item["brand"]
                        if "offers" in item:
                            off = item["offers"]
                            details["Price_Inc_VAT"] = float(off[0].get("price", 0)) if isinstance(off, list) else float(off.get("price", 0))
                        if "image" in item: details["All_Images"] = ", ".join(item["image"]) if isinstance(item["image"], list) else item["image"]
        except: pass

        # 5b. Description & Quantity
        try:
            details["description"] = self.driver.find_element(By.CSS_SELECTOR, "p[data-test-id='product-description']").text.strip()
            qty_input = self.driver.find_element(By.CSS_SELECTOR, "input[data-test-id='quantity-input']")
            details["Quantity"] = qty_input.get_attribute("value")
        except: pass

        # 5c. Specifications Table
        raw_dims = {"length": "N/A", "width": "N/A", "height": "N/A", "depth": "N/A", "thickness": "N/A"}
        try:
            rows = self.driver.find_elements(By.CSS_SELECTOR, "tr[class*='specification'], table tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 2: cols = row.find_elements(By.TAG_NAME, "th") + row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    k, v = cols[0].text.strip().lower(), cols[1].text.strip()
                    if "product type" in k: details["Product_Type"] = v
                    elif "material" in k: details["Material"] = v
                    elif "weight" in k: details["Product_Weight_Kg"] = v
                    elif "coverage" in k: details["Coverage_M2"] = self._clean_val(v)
                    elif "volume" in k or "capacity" in k: details["Volume_M3"] = self._clean_vol(v)
                    elif "width" in k: 
                        if not any(x in k for x in ["cutting", "bore", "packaging"]):
                            raw_dims["width"] = self._clean_dim(v)
                    elif "length" in k: raw_dims["length"] = self._clean_dim(v)
                    elif "height" in k: raw_dims["height"] = self._clean_dim(v)
                    elif "depth" in k: raw_dims["depth"] = self._clean_dim(v)
                    elif "thickness" in k: raw_dims["thickness"] = self._clean_dim(v)
                    elif "pack size" in k: details["Pack_Size"] = v
                    elif "pieces in pack" in k: details["Pieces_in_Pack"] = v
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

    def _post_process(self, details):
        # Packaging Logic for Machines/Tools/Generators
        item_type = str(details.get("Product_Type", "")).lower()
        item_name = details["Name"].lower()
        machine_keywords = ["generator", "tool", "machine", "chaser", "drill", "saw", "vacuum", "pump", "inverter"]
        
        if details["Pack_Size"] != "1" or details["Pieces_in_Pack"] != "1":
            details["Packaging_Type"] = "PACK"
        else:
            details["Packaging_Type"] = "EACH"
            
        if any(k in item_type or k in item_name for k in machine_keywords):
            details["Packaging_Type"] = "EACH"

        # (Existing cleaning/math would follow here if needed)
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
            d["Volume_M3"] = d["Product_Length_M"] * d["Product_Width"] * d["Product_Thickness"]
            
        if d["Coverage_M2"] == "N/A" and all(isinstance(d[x], float) for x in ["Product_Length_M", "Product_Width"]):
            d["Coverage_M2"] = d["Product_Length_M"] * d["Product_Width"]

        # Coverage Per Item Logic (Strict)
        if d["Coverage_M2"] != "N/A": d["Coverage_Per_Item"] = f"{d['Coverage_M2']} m2"
        elif d["Volume_M3"] != "N/A": d["Coverage_Per_Item"] = f"{d['Volume_M3']} m3"
        elif d["Pieces_in_Pack"] != "N/A" and d["Pieces_in_Pack"] != "1":
            d["Coverage_Per_Item"] = f"{d['Pieces_in_Pack']} pcs"
        else:
            d["Coverage_Per_Item"] = "N/A"

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
    def scrape_products_parallel(self, products, max_workers=2):
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_run_diy_worker_batch, p) for p in products]
            results = []
            for f in concurrent.futures.as_completed(futures):
                res = f.result()
                if res: results.append(res)
            return results

    def check_and_save_incrementally(self, new_data):
        self.all_scraped_data.extend(new_data)
        if len(self.all_scraped_data) - self.last_save_count >= 15:
            pd.DataFrame(self.all_scraped_data).to_csv("diy_products_incremental.csv", index=False)
            self.last_save_count = len(self.all_scraped_data)

    def close(self):
        self.driver.quit()

# Standalone Worker Batch (Parallelism)
def _run_diy_worker_batch(product):
    worker_scraper = DIYScraper(headless=False)
    try:
        if db_utils.product_exists(product['Link']): return None
        logger.info(f"Worker deep-scanning: {product['Name']}")
        time.sleep(random.uniform(2, 5))
        details = worker_scraper.get_product_details(product["Link"])
        product.update(details)
        db_utils.save_product(product)
        return product
    finally:
        worker_scraper.close()

if __name__ == "__main__":
    db_utils.create_table()
    scraper = DIYScraper(headless=False)
    try:
        scraper.driver.get(scraper.base_url)
        scraper.handle_cookies()
        # Sample Run: 50 products across categories
        data = scraper.navigate_all_categories(max_products=2)
        pd.DataFrame(data).to_csv("diy_building_materials_sample.csv", index=False)
        logger.info("DIY Production Job Finished.")
    finally:
        scraper.close()
