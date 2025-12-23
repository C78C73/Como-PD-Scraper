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
import csv
import io
from urllib.parse import urlencode
from datetime import datetime, timedelta

POLICE_URL = "https://www.como.gov/CMS/911dispatch/police.php"
FIRE_URL = "https://www.como.gov/CMS/911dispatch/fire.php"
POLICE_CSV_URL = "https://www.como.gov/CMS/911dispatch/police_csvexport.php"
FIRE_CSV_URL = "https://www.como.gov/CMS/911dispatch/fire_csvexport.php"
import platform
# Set chromedriver path for Windows (local) or Linux (CI)
if platform.system() == 'Linux':
    CHROMEDRIVER_PATH = "/usr/bin/chromedriver"
else:
    CHROMEDRIVER_PATH = "chromedriver.exe"
USER_AGENT = "gageishere53@gmail.com - incident-mapper (respecting Nominatim policy)"
CSV_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
GEOCACHE_FILE = "geocache.json"


def log_section(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def log_subsection(title: str) -> None:
    print("\n--- " + title + " ---")

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

        def normalize_street_name(raw: str) -> str:
            """Expand common abbreviations (HWY, RD, ST, BLVD, etc.) and directions.
            This often helps Nominatim resolve tricky intersections.
            """
            mapping = {
                "HWY": "Highway",
                "RD": "Road",
                "ST": "Street",
                "BLVD": "Boulevard",
                "AVE": "Avenue",
                "AV": "Avenue",
                "CT": "Court",
                "DR": "Drive",
                "LN": "Lane",
                "PL": "Place",
                "PKWY": "Parkway",
                "PKY": "Parkway",
                "SQ": "Square",
                "EXPY": "Expressway",
                "EXPRESS": "Expressway",
            }
            directions = {
                "N": "North",
                "S": "South",
                "E": "East",
                "W": "West",
                "NE": "Northeast",
                "NW": "Northwest",
                "SE": "Southeast",
                "SW": "Southwest",
            }
            drop_tokens = {"NB", "SB", "EB", "WB", "OFFR", "OFF", "RAMP"}

            tokens = raw.replace("-", " ").split()
            norm = []
            for t in tokens:
                up = t.upper()
                if up in drop_tokens:
                    continue
                if up in directions:
                    norm.append(directions[up])
                elif up in mapping:
                    norm.append(mapping[up])
                else:
                    norm.append(t)
            return " ".join(norm)

        # Detect intersection patterns: "/" or multiple street indicators
        seps = ["/", " & ", " AND ", " @ ", " AT "]
        for sep in seps:
            if sep in a2:
                parts = [p.strip() for p in a2.split(sep) if p.strip()]
                if len(parts) >= 2:
                    p1, p2 = parts[0], parts[1]
                    p1n, p2n = normalize_street_name(p1), normalize_street_name(p2)
                    # Try multiple intersection formats, including normalized street names
                    queries = [
                        f"{p1} & {p2}",
                        f"{p1} and {p2}",
                        f"intersection of {p1} and {p2}",
                        f"{p1n} & {p2n}",
                        f"{p1n} and {p2n}",
                        f"intersection of {p1n} and {p2n}",
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


def fetch_incidents_from_csv(csv_url, is_fire_ems, target_date_str):
    """Fetch incidents from a CSV export endpoint for a specific date.

    This prefers structured CSV over HTML scraping. If the CSV returns no
    usable rows or errors, callers should fall back to scrape_incidents_for_url.
    """
    log_subsection(f"[CSV] Fetching from {csv_url} for {target_date_str}")

    # Try to look like a normal browser request and also mimic the
    # sequence the user triggers: load the main page (with Start_Date)
    # and then click the Export CSV button, all in one session.
    headers = {
        "User-Agent": CSV_USER_AGENT,
        "Accept": "text/csv, text/plain, */*;q=0.2",
    }

    session = requests.Session()
    try:
        if "police_csvexport" in csv_url:
            headers["Referer"] = POLICE_URL
            # Hit the police page first to set any server-side state
            base_params = {}
            if target_date_str:
                base_params["Start_Date"] = target_date_str
            session.get(POLICE_URL, headers={"User-Agent": CSV_USER_AGENT}, params=base_params, timeout=30)
        elif "fire_csvexport" in csv_url:
            headers["Referer"] = FIRE_URL
            base_params = {}
            if target_date_str:
                base_params["Start_Date"] = target_date_str
            session.get(FIRE_URL, headers={"User-Agent": CSV_USER_AGENT}, params=base_params, timeout=30)

        # Now request the CSV in the same session, optionally also
        # passing Start_Date to mirror query-string usage.
        params = {}
        if target_date_str:
            params["Start_Date"] = target_date_str

        resp = session.get(
            csv_url,
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"Error requesting CSV {csv_url}: {e}")
        return []

    text = resp.text.strip()
    if not text:
        print("CSV response was empty.")
        return []

    f = io.StringIO(text)
    try:
        reader = csv.DictReader(f)
    except Exception as e:
        print(f"Error parsing CSV header from {csv_url}: {e}")
        return []

    items = []
    for row in reader:
        # Skip completely empty rows
        if not row or not any((v or "").strip() for v in row.values()):
            continue

        dt = (
            (row.get("CallDateTime") or "").strip()
            or (row.get("DateTime") or "").strip()
            or (row.get("Date") or "").strip()
        )
        inc = (
            (row.get("InNum") or "").strip()
            or (row.get("Incident") or "").strip()
            or (row.get("IncidentNumber") or "").strip()
            or (row.get("IncNum") or "").strip()
        )
        addr = (
            (row.get("Address") or "").strip()
            or (row.get("Location") or "").strip()
            or (row.get("LOCATION") or "").strip()
        )
        typ = (
            (row.get("ExtNatureDisplayName") or "").strip()
            or (row.get("Nature") or "").strip()
            or (row.get("Type") or "").strip()
        )

        if is_fire_ems:
            agency = (
                (row.get("AGENCY") or "").strip()
                or (row.get("Agency") or "").strip()
                or "Fire/EMS"
            )
        else:
            agency = "Columbia Police Department"

        # If the row has essentially no identifying info, skip it
        if not dt and not inc and not addr:
            continue

        # Filter by date locally: many CSV exports don't take a Start_Date
        # param, they just dump recent calls. We keep only rows whose
        # date portion matches target_date_str.
        if target_date_str:
            row_date = None
            # Try splitting off the date part and normalizing.
            try:
                # e.g. "12/15/2025 7:47:56 PM" -> "12/15/2025"
                date_token = dt.split()[0]
                # Common US-style date first
                try:
                    row_date = datetime.strptime(date_token, "%m/%d/%Y").strftime("%Y-%m-%d")
                except Exception:
                    # Fallback to ISO-style if they ever change it
                    try:
                        row_date = datetime.strptime(date_token, "%Y-%m-%d").strftime("%Y-%m-%d")
                    except Exception:
                        row_date = None
            except Exception:
                row_date = None

            if row_date is None or row_date != target_date_str:
                # Not part of the requested day
                continue

        items.append(
            {
                "datetime": dt,
                "incident": inc,
                "agency": agency,
                "location_txt": addr,
                "type": typ,
                "service": "FIRE_EMS" if is_fire_ems else "PD",
            }
        )

    print(f"[CSV] Parsed {len(items)} incidents from CSV for {target_date_str}.")

    if len(items) == 0:
        # Helpful debug: show a short sample so we can see what
        # the server actually returned (often HTML if blocked).
        sample = text.splitlines()[:5]
        print("[CSV] Appeared to have no usable rows. First few lines returned:")
        for line in sample:
            print("  ", line[:200])
    return items

def scrape_incidents_for_url(url, is_fire_ems, target_date_str):
    """Scrape incidents for a specific date from a specific dispatch URL.

    When is_fire_ems is True, parse the extra AGENCY column and tag service
    as FIRE/EMS; otherwise assume Police.
    """
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
    # Set a realistic user-agent string for all platforms
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    driver = webdriver.Chrome(service=service, options=options)

    driver.get(url)
    log_subsection("[HTML] Waiting for page to load")
    
    # Wait for the date input field to be present
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "Start_Date"))
        )
        time.sleep(20)  # Extra time for date fields to be ready
    except Exception as e:
        print(f"[HTML] Date fields did not load: {e}")
        # Take screenshot for diagnostics
        try:
            driver.save_screenshot("date_field_error.png")
            print("Screenshot saved as date_field_error.png")
        except Exception as se:
            print(f"Screenshot failed: {se}")
        driver.quit()
        return []

    # Set Start_Date using JavaScript
    print(f"[HTML] Setting Start_Date to: {target_date_str}")
    try:
        driver.execute_script(f"document.querySelector('input[id=\"Start_Date\"]').value = '{target_date_str}';")
        driver.execute_script(f"document.querySelector('input[name=\"Start_Date\"]').value = '{target_date_str}';")
        print("[HTML] Set Start_Date using JavaScript.")
        time.sleep(20)  # Let the date value settle
    except Exception as e:
        print("[HTML] Could not set Start_Date: ", e)
        driver.quit()
        return []

    # Click Filter
    try:
        filter_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@value='Filter']"))
        )
        filter_button.click()
        print("[HTML] Clicked Filter button.")
        time.sleep(20)
    except Exception as e:
        print("[HTML] Could not click Filter button: ", e)
        driver.quit()
        return []

    # Wait for table
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table911"))
        )
        print("[HTML] Table found!")
    except Exception as e:
        print("[HTML] Table not found: ", e)
        driver.quit()
        return []

    # Check the results count to see if there's data and how many pages
    try:
        results_elem = driver.find_element(By.CSS_SELECTOR, "span.resultscount")
        results_text = results_elem.text
        print(f"[HTML] Results: {results_text}")
        
        # Parse "276 records found, displaying page 1 of 12."
        if "0 records found" in results_text or results_text.strip() == "":
            print("[HTML] No records found on this date.")
            driver.quit()
            return []
        
        # Extract total pages if available
        import re
        page_match = re.search(r'page \d+ of (\d+)', results_text)
        total_pages = int(page_match.group(1)) if page_match else None
        if total_pages:
            print(f"[HTML] Total pages to scrape: {total_pages}")
    except Exception as e:
        print(f"[HTML] Could not read results count: {e}")
        total_pages = None

    # Collect all incidents from all pages
    all_items = []
    seen_incidents = set()  # Track incident IDs to detect duplicates/loops
    page_num = 1
    
    while True:
        print(f"[HTML] Scraping page {page_num}...")
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table.table911 tr")
        
        page_items = []
        duplicates_found = False
        
        for tr in rows:
            cols = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            # Police: DATE/TIME, INCIDENT #, LOCATION, TYPE
            # Fire/EMS: DATE/TIME, INCIDENT #, AGENCY, LOCATION, TYPE
            if is_fire_ems and len(cols) >= 5:
                dt, inc, agency, loc, typ = cols[0], cols[1], cols[2], cols[3], cols[4]
            elif not is_fire_ems and len(cols) >= 4:
                dt, inc, loc, typ = cols[0], cols[1], cols[2], cols[3]
                agency = "Columbia Police Department"
            else:
                continue

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
                "agency": agency,
                "location_txt": loc,
                "type": typ,
                "service": "FIRE_EMS" if is_fire_ems else "PD"
            })
        
        print(f"  [HTML] Found {len(page_items)} new incidents on page {page_num}")
        
        # If first page has no data, stop immediately
        if page_num == 1 and len(page_items) == 0:
            print("[HTML] No data on first page, stopping.")
            break
        
        # If we found duplicates, stop (pagination looped back)
        if duplicates_found:
            break
        
        all_items.extend(page_items)
        
        # If current page has no data, we've gone past the last page
        if len(page_items) == 0:
            print("[HTML] Empty page encountered, stopping.")
            break
        
        # Check if there's a "Next >" link to continue
        try:
            time.sleep(20)  # Wait before checking for Next button
            next_link = driver.find_element(By.LINK_TEXT, "Next >")
            next_link.click()
            print(f"[HTML] Clicked 'Next >' button, waiting for page {page_num + 1} to load...")
            time.sleep(20)  # Increased wait time for page to fully load
            page_num += 1
            # Safety check: if we know total pages, stop when we reach it
                if total_pages and page_num > total_pages:
                print(f"[HTML] Reached total page count ({total_pages}), stopping.")
                break
            except:
                print("[HTML] No more pages (no Next > link found).")
                break

            print(f"[HTML] Total unique incidents collected: {len(all_items)}")
    
    driver.quit()
    return all_items


def get_pd_incidents_for_date(target_date_str):
    """Get Police incidents for a date, preferring CSV with HTML fallback."""
    # Try CSV first
    pd_items = fetch_incidents_from_csv(POLICE_CSV_URL, False, target_date_str)
    if len(pd_items) > 0:
        print(f"Using Police CSV data for {target_date_str} ({len(pd_items)} incidents).")
        return pd_items

    print("Police CSV was empty or unavailable, falling back to HTML scraping...")
    return scrape_incidents_for_url(POLICE_URL, False, target_date_str)


def get_fire_incidents_for_date(target_date_str):
    """Get Fire/EMS incidents for a date, preferring CSV with HTML fallback."""
    fire_items = fetch_incidents_from_csv(FIRE_CSV_URL, True, target_date_str)
    if len(fire_items) > 0:
        print(f"[FIRE] Using CSV data for {target_date_str} ({len(fire_items)} incidents).")
        return fire_items

    print("[FIRE] CSV was empty or unavailable, falling back to HTML scraping...")
    return scrape_incidents_for_url(FIRE_URL, True, target_date_str)


def get_pd_incidents_with_date_fallback(today_str, yesterday_str):
    """Get Police incidents, preferring CSV and backing up the start day.

    Order:
    1) Police CSV for today
    2) Police CSV for yesterday
    3) Police HTML scraping for today
    4) Police HTML scraping for yesterday
    """

    # 1) CSV for today
    log_subsection(f"[PD] Trying CSV for {today_str}")
    items = fetch_incidents_from_csv(POLICE_CSV_URL, False, today_str)
    if len(items) > 0:
        print(f"[PD] Using CSV data for {today_str} ({len(items)} incidents).")
        return items, today_str

    # 2) CSV for yesterday
    print(f"[PD] No CSV records for {today_str}. Trying CSV for {yesterday_str}...")
    items = fetch_incidents_from_csv(POLICE_CSV_URL, False, yesterday_str)
    if len(items) > 0:
        print(f"[PD] Using CSV data for {yesterday_str} ({len(items)} incidents).")
        return items, yesterday_str

    # 3) HTML for today
    print("[PD] CSV empty or unavailable for both dates, trying HTML scraping for today...")
    items = scrape_incidents_for_url(POLICE_URL, False, today_str)
    if len(items) > 0:
        print(f"[PD] Using HTML scraping for {today_str} ({len(items)} incidents).")
        return items, today_str

    # 4) HTML for yesterday
    print(f"[PD] No HTML incidents for {today_str}. Trying HTML scraping for {yesterday_str}...")
    items = scrape_incidents_for_url(POLICE_URL, False, yesterday_str)
    if len(items) > 0:
        print(f"[PD] Using HTML scraping for {yesterday_str} ({len(items)} incidents).")
        return items, yesterday_str

    print("[PD] No incidents found via CSV or HTML for today or yesterday.")
    return [], yesterday_str


def main():
    log_section("Como PD Incident Scraper Run")

    # Decide which date to use based on PD data (handles 6-hour delay)
    today_str = datetime.now().strftime("%Y-%m-%d")
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    log_subsection(
        f"[PD] Loading incidents (CSV first, then HTML) for {today_str} with fallback to {yesterday_str}"
    )
    pd_items, target_date_str = get_pd_incidents_with_date_fallback(today_str, yesterday_str)

    log_subsection(
        f"[FIRE] Loading Fire/EMS incidents for {target_date_str} (CSV preferred, then HTML fallback)"
    )
    fire_items = get_fire_incidents_for_date(target_date_str)

    items = pd_items + fire_items

    if len(items) == 0:
        log_subsection("No incidents found; writing empty data.json")
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump([], f, indent=2)
        return

    log_subsection(f"Geocoding {len(items)} incidents")
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
        print(f"[GEO] {i+1}/{len(items)}: {addr} -> {lat},{lon}")
        item["lat"] = lat
        item["lon"] = lon

    # Stamp metadata so the map can show when this dataset was generated
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    for item in items:
        item["generated_at"] = generated_at
        # Also record which dispatch date we scraped
        item["dispatch_date"] = target_date_str

    save_geocache(geocache)
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)
    print(f"\nSaved data.json. Geocoded: {geocoded}, Skipped: {skipped}")

if __name__ == "__main__":
    main()
