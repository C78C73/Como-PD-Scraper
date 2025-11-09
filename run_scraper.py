"""
Como PD Incident Scraper & Geocoder
Scrapes incident data from como.gov, geocodes addresses, and generates data.json for the map.
Handles 6-hour delay by auto-retrying with yesterday's date if no data found.
Improved intersection geocoding to find where two streets meet.
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
import time
import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta

URL = "https://www.como.gov/CMS/911dispatch/police.php"
import platform
# Set chromedriver path for Windows (local) or Linux (CI)
if platform.system() == 'Linux':
    CHROMEDRIVER_PATH = "/usr/bin/chromedriver"
else:
    CHROMEDRIVER_PATH = "chromedriver.exe"
USER_AGENT = "gageishere53@gmail.com - incident-mapper (respecting Nominatim policy)"
GEOCACHE_FILE = "geocache.json"

def load_geocache():
    if os.path.exists(GEOCACHE_FILE):
        try:
            with open(GEOCACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_geocache(cache):
    try:
        with open(GEOCACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception:
        pass

def geocode_address(address, cache, sleep_time=1.0):
    """Geocode an address using Nominatim with caching.
    Handles intersections by finding where two streets meet.
    """
    if not address:
        return None, None

    key = address.strip().upper()
    if key in cache:
        return cache[key]

    def nominatim_query(q):
        base = "https://nominatim.openstreetmap.org/search?"
        params = {"q": q + ", Columbia, MO, USA", "format": "json", "limit": 1}
        url = base + urlencode(params)
        r = requests.get(url, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
        return None, None

    # Try raw address first
    try:
        latlon = nominatim_query(address)
    except Exception:
        latlon = (None, None)

    # If not found, try intersection handling
    if latlon == (None, None):
        a2 = address.replace("BLOCK", "").replace(" OF ", " ").strip()
        
        # Detect intersection patterns: "/" or multiple street indicators
        seps = ["/", " & ", " AND ", " @ ", " AT "]
        for sep in seps:
            if sep in a2:
                parts = [p.strip() for p in a2.split(sep) if p.strip()]
                if len(parts) >= 2:
                    # Try multiple intersection formats
                    queries = [
                        f"{parts[0]} & {parts[1]}",
                        f"{parts[0]} and {parts[1]}",
                        f"intersection of {parts[0]} and {parts[1]}"
                    ]
                    for inter in queries:
                        try:
                            latlon = nominatim_query(inter)
                            if latlon != (None, None):
                                print(f"  Found intersection: {inter}")
                                break
                        except Exception:
                            continue
                    if latlon != (None, None):
                        break

        # Final fallback: try trimmed version
        if latlon == (None, None):
            try:
                latlon = nominatim_query(a2)
            except Exception:
                latlon = (None, None)

    cache[key] = latlon
    time.sleep(sleep_time)
    return latlon

def scrape_incidents(target_date_str):
    """Scrape incidents for a specific date using Selenium."""
    service = Service(CHROMEDRIVER_PATH)
    options = webdriver.ChromeOptions()
    if platform.system() == 'Linux':
        # Use the correct chromium binary if available
        if os.path.exists('/usr/bin/chromium-browser'):
            options.binary_location = '/usr/bin/chromium-browser'
        elif os.path.exists('/usr/bin/chromium'):
            options.binary_location = '/usr/bin/chromium'
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
    else:
        # Local dev: show browser, but allow headless for silent runs
        # options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(service=service, options=options)

    driver.get(URL)
    print("Waiting for page to load...")
    
    # Wait for the date input field to be present
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "Start_Date"))
        )
        time.sleep(20)  # Extra time for date fields to be ready
    except Exception as e:
        print(f"Date fields did not load: {e}")
        driver.quit()
        return []

    # Set Start_Date using JavaScript
    print(f"Setting Start_Date to: {target_date_str}")
    try:
        driver.execute_script(f"document.querySelector('input[id=\"Start_Date\"]').value = '{target_date_str}';")
        driver.execute_script(f"document.querySelector('input[name=\"Start_Date\"]').value = '{target_date_str}';")
        print("Set Start_Date using JavaScript.")
        time.sleep(20)  # Let the date value settle
    except Exception as e:
        print("Could not set Start_Date: ", e)
        driver.quit()
        return []

    # Click Filter
    try:
        filter_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@value='Filter']"))
        )
        filter_button.click()
        print("Clicked Filter button.")
        time.sleep(20)
    except Exception as e:
        print("Could not click Filter button: ", e)
        driver.quit()
        return []

    # Wait for table
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table911"))
        )
        print("Table found!")
    except Exception as e:
        print("Table not found: ", e)
        driver.quit()
        return []

    # Check the results count to see if there's data and how many pages
    try:
        results_elem = driver.find_element(By.CSS_SELECTOR, "span.resultscount")
        results_text = results_elem.text
        print(f"Results: {results_text}")
        
        # Parse "276 records found, displaying page 1 of 12."
        if "0 records found" in results_text or results_text.strip() == "":
            print("No records found on this date.")
            driver.quit()
            return []
        
        # Extract total pages if available
        import re
        page_match = re.search(r'page \d+ of (\d+)', results_text)
        total_pages = int(page_match.group(1)) if page_match else None
        if total_pages:
            print(f"Total pages to scrape: {total_pages}")
    except Exception as e:
        print(f"Could not read results count: {e}")
        total_pages = None

    # Collect all incidents from all pages
    all_items = []
    seen_incidents = set()  # Track incident IDs to detect duplicates/loops
    page_num = 1
    
    while True:
        print(f"Scraping page {page_num}...")
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table.table911 tr")
        
        page_items = []
        duplicates_found = False
        
        for tr in rows:
            cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if len(cols) >= 4:
                dt, inc, loc, typ = cols[0], cols[1], cols[2], cols[3]
                if dt.lower().startswith("date") or dt == "":
                    continue
                
                # Check for duplicate incident ID (means we've looped back)
                if inc in seen_incidents:
                    duplicates_found = True
                    print(f"  Duplicate incident {inc} detected - pagination loop, stopping.")
                    break
                
                seen_incidents.add(inc)
                page_items.append({
                    "datetime": dt,
                    "incident": inc,
                    "location_txt": loc,
                    "type": typ
                })
        
        print(f"  Found {len(page_items)} new incidents on page {page_num}")
        
        # If first page has no data, stop immediately
        if page_num == 1 and len(page_items) == 0:
            print("No data on first page, stopping.")
            break
        
        # If we found duplicates, stop (pagination looped back)
        if duplicates_found:
            break
        
        all_items.extend(page_items)
        
        # If current page has no data, we've gone past the last page
        if len(page_items) == 0:
            print("Empty page encountered, stopping.")
            break
        
        # Check if there's a "Next >" link to continue
        try:
            time.sleep(20)  # Wait before checking for Next button
            next_link = driver.find_element(By.LINK_TEXT, "Next >")
            next_link.click()
            print(f"Clicked 'Next >' button, waiting for page {page_num + 1} to load...")
            time.sleep(20)  # Increased wait time for page to fully load
            page_num += 1
            # Safety check: if we know total pages, stop when we reach it
            if total_pages and page_num > total_pages:
                print(f"Reached total page count ({total_pages}), stopping.")
                break
        except:
            print("No more pages (no Next > link found).")
            break
    
    print(f"Total unique incidents collected: {len(all_items)}")
    
    driver.quit()
    return all_items

def main():
    # Try today first
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"Scraping incidents for {today_str}...")
    items = scrape_incidents(today_str)

    # If no data, retry with yesterday (6-hour delay handling)
    if len(items) == 0:
        print("No data found for today. Trying yesterday (6-hour delay)...")
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        items = scrape_incidents(yesterday_str)

    if len(items) == 0:
        print("No incidents found.")
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump([], f, indent=2)
        return

    print(f"Found {len(items)} incidents. Geocoding addresses...")
    geocache = load_geocache()
    geocoded = 0
    skipped = 0

    for i, item in enumerate(items):
        addr = item.get("location_txt", "")
        lat, lon = geocode_address(addr, geocache, sleep_time=1.0)
        if lat is not None and lon is not None:
            geocoded += 1
        else:
            skipped += 1
        print(f"{i+1}/{len(items)}: {addr} -> {lat},{lon}")
        item["lat"] = lat
        item["lon"] = lon

    save_geocache(geocache)
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)
    print(f"\nSaved data.json. Geocoded: {geocoded}, Skipped: {skipped}")

if __name__ == "__main__":
    main()
