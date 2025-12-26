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
GEOCODE_UNMAPPED_DEBUG_FILE = "geocode_unmapped_debug.json"

# Nominatim can return better results if bounded to the local area.
# Bounding box used to constrain results to the Columbia, MO area.
# viewbox format: left,top,right,bottom (lon,lat)
# Note: slightly wider than the immediate city core to reduce false negatives
# on edge-of-city streets while still enforcing that results fall in this bbox.
NOMINATIM_VIEWBOX = (-92.55, 39.15, -92.15, 38.80)

# Some street intersections are not resolvable via Nominatim free-text.
# Overpass can return the actual shared node(s) between two named ways.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def _overpass_escape_regex(text: str) -> str:
    import re

    return re.escape(_clean_whitespace(text))


def _overpass_find_intersection(street_a: str, street_b: str, timeout=25):
    """Return (lat, lon) for the intersection of two streets within Columbia bbox.

    Best-effort: tries exact name match first, then relaxed substring match.
    """
    import re

    a = _clean_whitespace(street_a)
    b = _clean_whitespace(street_b)
    if not a or not b:
        return None, None

    def build_patterns(s: str) -> list[str]:
        """Return a few Overpass regex patterns for name/ref matching."""
        s0 = _clean_whitespace(s)
        if not s0:
            return []

        pats: list[str] = []

        # Exact name/ref
        pats.append("^" + _overpass_escape_regex(s0) + "$")

        # Drop leading direction words
        s1 = re.sub(r"^(North|South|East|West)\s+", "", s0, flags=re.IGNORECASE)
        s1 = _clean_whitespace(s1)
        if s1 and s1 != s0:
            pats.append("^" + _overpass_escape_regex(s1) + "$")

        # Normalized (expands abbreviations like Dr -> Drive)
        try:
            s2 = _clean_whitespace(_normalize_street_text(s0))
        except Exception:
            s2 = ""
        if s2 and s2 not in [s0, s1]:
            pats.append("^" + _overpass_escape_regex(s2) + "$")

        # Contains-style fallbacks (more forgiving)
        for v in [s1, s2, s0]:
            v = _clean_whitespace(v)
            if v:
                pat = _overpass_escape_regex(v)
                if pat and pat not in pats:
                    pats.append(pat)

        # Street-type-stripped token pattern.
        # Helps when OSM has a longer official name (e.g. "Mick Deaver Memorial Drive")
        # but dispatch uses a shortened form (e.g. "Mick Deaver Dr").
        street_type_re = re.compile(
            r"\b(?:ROAD|RD|DRIVE|DR|STREET|ST|AVENUE|AVE|BOULEVARD|BLVD|LANE|LN|COURT|CT|CIRCLE|CIR|PARKWAY|PKWY|TRAIL|TRL|WAY|WY|TERRACE|TER|PLACE|PL)\b\.?\s*$",
            re.IGNORECASE,
        )
        base = street_type_re.sub("", s1 or s0)
        base = _clean_whitespace(base)
        if base and base not in [s0, s1, s2]:
            base_pat = _overpass_escape_regex(base)
            if base_pat and base_pat not in pats:
                pats.append(base_pat)

        # If this looks like a highway/route with a number, add ref-friendly regex.
        m = re.search(r"\b(\d{1,3})\b", s0)
        if m:
            num = m.group(1)
            pats.append(r"\\b(?:US\\s*)?" + num + r"\\b")

        # Keep it small to avoid heavy queries
        return pats[:6]

    a_pats = build_patterns(a)
    b_pats = build_patterns(b)
    if not a_pats or not b_pats:
        return None, None

    south, west, north, east = NOMINATIM_VIEWBOX[3], NOMINATIM_VIEWBOX[0], NOMINATIM_VIEWBOX[1], NOMINATIM_VIEWBOX[2]

    def run_query(a_pat: str, b_pat: str):
        q = f"""
[out:json][timeout:{int(timeout)}];
(
    way[highway][name~\"{a_pat}\",i]({south},{west},{north},{east});
    way[highway][ref~\"{a_pat}\",i]({south},{west},{north},{east});
)->.wa;
(
    way[highway][name~\"{b_pat}\",i]({south},{west},{north},{east});
    way[highway][ref~\"{b_pat}\",i]({south},{west},{north},{east});
)->.wb;
node(w.wa)(w.wb);
out 1;
""".strip()

        r = requests.get(
            OVERPASS_URL,
            params={"data": q},
            headers={"User-Agent": USER_AGENT},
            timeout=timeout + 5,
        )
        r.raise_for_status()
        data = r.json()
        els = data.get("elements") or []
        for el in els:
            if el.get("type") == "node" and "lat" in el and "lon" in el:
                return float(el["lat"]), float(el["lon"])
        return None, None

    # Try a small cartesian of candidate patterns
    for ap in a_pats:
        for bp in b_pats:
            try:
                latlon = run_query(ap, bp)
                if latlon != (None, None):
                    return latlon
            except Exception:
                continue

    return None, None


def _overpass_find_way_center(street_name: str, timeout=25):
    """Return an approximate (lat, lon) for a named street within Columbia bbox.

    Useful for BLOCK-type locations when a specific house number can't be resolved.
    """
    import re

    name = _clean_whitespace(street_name)
    if not name:
        return None, None

    exact = "^" + _overpass_escape_regex(name) + "$"

    # Substring-style fallbacks (more forgiving about Dr/Drive, Blvd/Boulevard, etc.)
    directionless = re.sub(r"^(North|South|East|West)\s+", "", name, flags=re.IGNORECASE)
    contains_1 = _overpass_escape_regex(directionless) if directionless else _overpass_escape_regex(name)
    try:
        normalized = _normalize_street_text(name)
    except Exception:
        normalized = ""
    contains_2 = _overpass_escape_regex(normalized) if normalized else ""

    south, west, north, east = NOMINATIM_VIEWBOX[3], NOMINATIM_VIEWBOX[0], NOMINATIM_VIEWBOX[1], NOMINATIM_VIEWBOX[2]

    def run_query(pat: str):
        q = f"""
[out:json][timeout:{int(timeout)}];
way[highway][name~\"{pat}\",i]({south},{west},{north},{east});
out center 1;
""".strip()
        r = requests.get(
            OVERPASS_URL,
            params={"data": q},
            headers={"User-Agent": USER_AGENT},
            timeout=timeout + 5,
        )
        r.raise_for_status()
        data = r.json()
        els = data.get("elements") or []
        for el in els:
            if el.get("type") == "way":
                c = el.get("center")
                if c and "lat" in c and "lon" in c:
                    return float(c["lat"]), float(c["lon"])
        return None, None

    try:
        latlon = run_query(exact)
        if latlon != (None, None):
            return latlon
    except Exception:
        pass

    for pat in [contains_1, contains_2]:
        if not pat:
            continue
        try:
            latlon = run_query(pat)
            if latlon != (None, None):
                return latlon
        except Exception:
            pass

    return None, None


def _overpass_find_address(house_num: str, street_name: str, timeout=25):
    """Return (lat, lon) for an address (addr:housenumber + addr:street) within Columbia bbox."""
    import re

    num = _clean_whitespace(house_num)
    street = _clean_whitespace(street_name)
    if not num or not street:
        return None, None

    south, west, north, east = NOMINATIM_VIEWBOX[3], NOMINATIM_VIEWBOX[0], NOMINATIM_VIEWBOX[1], NOMINATIM_VIEWBOX[2]

    # Build a few street patterns: exact-ish and forgiving.
    patterns: list[str] = []
    patterns.append("^" + _overpass_escape_regex(street) + "$")

    directionless = re.sub(r"^(North|South|East|West)\s+", "", street, flags=re.IGNORECASE)
    directionless = _clean_whitespace(directionless)
    if directionless and directionless != street:
        patterns.append("^" + _overpass_escape_regex(directionless) + "$")

    try:
        normalized = _clean_whitespace(_normalize_street_text(street))
    except Exception:
        normalized = ""
    if normalized and normalized not in [street, directionless]:
        patterns.append("^" + _overpass_escape_regex(normalized) + "$")

    # Contains fallbacks
    for v in [directionless, normalized, street]:
        v = _clean_whitespace(v)
        if v:
            pat = _overpass_escape_regex(v)
            if pat and pat not in patterns:
                patterns.append(pat)

    patterns = patterns[:6]

    def run_query(street_pat: str):
        q = f"""
[out:json][timeout:{int(timeout)}];
nwr["addr:housenumber"="{num}"]["addr:street"~"{street_pat}",i]({south},{west},{north},{east});
out center 1;
""".strip()

        r = requests.get(
            OVERPASS_URL,
            params={"data": q},
            headers={"User-Agent": USER_AGENT},
            timeout=timeout + 5,
        )
        r.raise_for_status()
        data = r.json()
        els = data.get("elements") or []
        for el in els:
            if el.get("type") == "node" and "lat" in el and "lon" in el:
                return float(el["lat"]), float(el["lon"])
            c = el.get("center")
            if c and "lat" in c and "lon" in c:
                return float(c["lat"]), float(c["lon"])
        return None, None

    for pat in patterns:
        try:
            latlon = run_query(pat)
            if latlon != (None, None):
                return latlon
        except Exception:
            continue

    return None, None


def _clean_whitespace(text: str) -> str:
    return " ".join((text or "").strip().split())


def _normalize_street_text(raw: str) -> str:
    """Normalize common tokens/abbreviations used in Como dispatch locations."""
    if not raw:
        return ""

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
        "CIR": "Circle",
        "TER": "Terrace",
        "TR": "Trail",
        "TRL": "Trail",
        "WAY": "Way",
        "EXPY": "Expressway",
        "EXPRESS": "Expressway",
        "I70": "I-70",
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
    drop_tokens = {
        # travel-direction / ramp tokens that tend to confuse geocoders
        "NB",
        "SB",
        "EB",
        "WB",
        "OFFR",
        "OFF",
        "ONR",
        "RAMP",
        "EXIT",
    }

    # Ensure separators don't get glued to tokens (e.g., "RD/FOO" -> "RD / FOO")
    text = raw
    text = text.replace("/", " / ")
    text = text.replace("&", " & ")
    text = text.replace("@", " @ ")
    text = text.replace("-", " ")
    text = _clean_whitespace(text)
    tokens = text.split()
    normalized_tokens: list[str] = []
    for token in tokens:
        up = token.upper()
        if up in drop_tokens:
            continue
        if up in directions:
            normalized_tokens.append(directions[up])
            continue
        if up in mapping:
            normalized_tokens.append(mapping[up])
            continue
        normalized_tokens.append(token)
    return " ".join(normalized_tokens)


def _normalize_dispatch_location(raw: str) -> str:
    """Normalize common dispatch-only formats before geocoding.

    Examples:
    - "610-BLK CLAUDELL LN" -> "610 BLOCK CLAUDELL LN"
    - "109-336 N KEENE ST" -> "109 N KEENE ST"
    - "0 BLOCK FOURTH AVE" -> "0 BLOCK 4TH AVE"
    """
    import re

    text = _clean_whitespace(raw)
    if not text:
        return ""

    # Convert "123-BLK" or "123-BLK" variants into "123 BLOCK"
    text = re.sub(r"\b(\d{1,6})\s*-\s*BLK\b", r"\1 BLOCK", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(\d{1,6})\s*BLK\b", r"\1 BLOCK", text, flags=re.IGNORECASE)

    # Convert numeric ranges to the first number: "109-336 N KEENE ST" -> "109 N KEENE ST"
    text = re.sub(r"\b(\d{1,6})\s*-\s*(\d{1,6})\b", r"\1", text)

    # Convert common ordinal words to numeric ordinals (helps Nominatim)
    ordinal_map = {
        "FIRST": "1ST",
        "SECOND": "2ND",
        "THIRD": "3RD",
        "FOURTH": "4TH",
        "FIFTH": "5TH",
        "SIXTH": "6TH",
        "SEVENTH": "7TH",
        "EIGHTH": "8TH",
        "NINTH": "9TH",
        "TENTH": "10TH",
    }
    for word, ordnum in ordinal_map.items():
        text = re.sub(rf"\b{word}\b", ordnum, text, flags=re.IGNORECASE)

    return _clean_whitespace(text)


def _strip_unit(raw: str) -> str:
    """Strip apartment/unit/suite fragments (often breaks geocoding)."""
    import re

    if not raw:
        return ""
    text = _clean_whitespace(raw)
    # Remove common trailing unit patterns
    text = re.sub(r"\s+(APT|APARTMENT|UNIT|STE|SUITE)\s+[^,]+$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+#\s*[^,]+$", "", text)
    return _clean_whitespace(text)


def _is_failed_latlon(latlon) -> bool:
    try:
        lat, lon = latlon
        return lat is None or lon is None
    except Exception:
        return True


def _in_viewbox(lat: float, lon: float) -> bool:
    try:
        left, top, right, bottom = NOMINATIM_VIEWBOX
        return (bottom <= float(lat) <= top) and (left <= float(lon) <= right)
    except Exception:
        return False


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

def geocode_address(address, cache, sleep_time=1.0, debug=None, retry_failed_cache=True):
    """Geocode an address using Nominatim with caching.
    Handles intersections by finding where two streets meet.
    """
    if not address:
        return None, None

    raw_address = _clean_whitespace(address)
    normalized_dispatch = _normalize_dispatch_location(raw_address)
    key = raw_address.upper()
    cached = cache.get(key)
    if cached is not None and (not retry_failed_cache or not _is_failed_latlon(cached)):
        return cached

    attempts = []
    errors = []
    used_query = None

    def nominatim_query(q):
        base = "https://nominatim.openstreetmap.org/search?"
        q_clean = _clean_whitespace(q)
        if not q_clean:
            return None, None

        # Only append city/state if caller didn't already specify a place.
        q_full = q_clean
        if "COLUMBIA" not in q_clean.upper() and "MO" not in q_clean.upper() and "MISSOURI" not in q_clean.upper():
            q_full = q_clean + ", Columbia, MO, USA"

        # 1) Preferred: bounded to the local viewbox.
        params = {
            "q": q_full,
            "format": "json",
            "limit": 1,
            "viewbox": f"{NOMINATIM_VIEWBOX[0]},{NOMINATIM_VIEWBOX[1]},{NOMINATIM_VIEWBOX[2]},{NOMINATIM_VIEWBOX[3]}",
            "bounded": 1,
        }
        url = base + urlencode(params)
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            return (lat, lon) if _in_viewbox(lat, lon) else (None, None)

        # 2) Fallback: unbounded search (Nominatim sometimes returns nothing when bounded),
        # but still strictly accept only results inside our viewbox.
        params2 = {
            "q": q_full,
            "format": "json",
            "limit": 1,
            "countrycodes": "us",
        }
        url2 = base + urlencode(params2)
        r2 = requests.get(url2, headers={"User-Agent": USER_AGENT}, timeout=20)
        r2.raise_for_status()
        data2 = r2.json()
        if data2:
            lat = float(data2[0]["lat"])
            lon = float(data2[0]["lon"])
            return (lat, lon) if _in_viewbox(lat, lon) else (None, None)

        return None, None

    def census_query(q: str):
        """US Census Geocoder fallback for address-like inputs.

        This uses TIGER/Line-derived data and can succeed for newer/less-mapped
        streets where OSM/Nominatim doesn't.
        """
        q_clean = _clean_whitespace(q)
        if not q_clean:
            return None, None

        q_full = q_clean
        if "COLUMBIA" not in q_clean.upper() and "MO" not in q_clean.upper() and "MISSOURI" not in q_clean.upper():
            q_full = q_clean + ", Columbia, MO"

        url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
        params = {
            "address": q_full,
            "benchmark": "Public_AR_Current",
            "format": "json",
        }
        r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
        r.raise_for_status()
        data = r.json() or {}
        matches = ((data.get("result") or {}).get("addressMatches")) or []
        if not matches:
            return None, None

        coords = (matches[0].get("coordinates") or {})
        if "x" not in coords or "y" not in coords:
            return None, None
        lon = float(coords["x"])
        lat = float(coords["y"])
        return (lat, lon) if _in_viewbox(lat, lon) else (None, None)

    def try_query(label: str, q: str):
        nonlocal used_query
        q_norm = _clean_whitespace(q)
        if not q_norm:
            return None, None

        attempts.append({"label": label, "q": q_norm})
        try:
            latlon = nominatim_query(q_norm)
            # Respect Nominatim rate limit between requests
            time.sleep(max(0.0, float(sleep_time)))
            if latlon != (None, None):
                used_query = q_norm
            return latlon
        except Exception as e:
            errors.append({"label": label, "q": q_norm, "error": str(e)})
            time.sleep(max(0.0, float(sleep_time)))
            return None, None

    def try_census(label: str, q: str):
        nonlocal used_query
        q_norm = _clean_whitespace(q)
        if not q_norm:
            return None, None
        attempts.append({"label": label, "q": q_norm})
        try:
            latlon = census_query(q_norm)
            time.sleep(max(0.0, float(sleep_time)))
            if latlon != (None, None):
                used_query = q_norm
            return latlon
        except Exception as e:
            errors.append({"label": label, "q": q_norm, "error": str(e)})
            time.sleep(max(0.0, float(sleep_time)))
            return None, None

    latlon = (None, None)

    # Intersections are rarely resolvable via raw free-text; skip the early
    # raw/strip/expand steps and go straight to intersection handling.
    import re
    is_intersection_like = bool(re.search(r"\s*/\s*|\s+AT\s+|\s+AND\s+|\s*&\s*|\s*@\s*", raw_address, flags=re.IGNORECASE))

    # 1) Try raw address
    if not is_intersection_like:
        latlon = try_query("raw", raw_address)

    # 1b) Try normalized dispatch format
    if (not is_intersection_like) and latlon == (None, None) and normalized_dispatch and normalized_dispatch != raw_address:
        latlon = try_query("normalized_dispatch", normalized_dispatch)

    # 2) Try stripping unit fragments
    if (not is_intersection_like) and latlon == (None, None):
        stripped = _strip_unit(raw_address)
        if stripped != raw_address:
            latlon = try_query("strip_unit", stripped)

    # 3) Try expanding abbreviations/directions
    if (not is_intersection_like) and latlon == (None, None):
        expanded = _normalize_street_text(normalized_dispatch or raw_address)
        if expanded and expanded != raw_address:
            latlon = try_query("expanded", expanded)

    # 3b) Try removing "BLOCK" word entirely (Google-style)
    if (not is_intersection_like) and latlon == (None, None):

        no_block = re.sub(r"\bBLOCK\b", " ", normalized_dispatch or raw_address, flags=re.IGNORECASE)
        no_block = _clean_whitespace(no_block)
        if no_block and no_block != raw_address:
            latlon = try_query("no_block", no_block)

    # 3c) If this looks like a street address, try US Census geocoding.
    if (not is_intersection_like) and latlon == (None, None):

        addrish = (normalized_dispatch or raw_address)
        if re.match(r"^\s*\d{1,6}\s+\S+", addrish):
            latlon = try_census("census_address", addrish)

    # If not found, try intersection handling
    if latlon == (None, None):
        import re

        a2 = normalized_dispatch or raw_address

        # 4) Handle "#### BLOCK (OF) ..." by trying a real house-number query
        m = re.match(r"^\s*(\d{1,6})\s*BLOCK\s+(?:OF\s+)?(.+?)\s*$", a2, flags=re.IGNORECASE)
        if m:
            block_num = int(m.group(1))
            street = _clean_whitespace(m.group(2))
            street_expanded = _normalize_street_text(street)

            street_name_variants = []
            for v in [street_expanded, street]:
                v = _clean_whitespace(v)
                if v and v not in street_name_variants:
                    street_name_variants.append(v)

            if block_num == 0:
                # "0 BLOCK" usually means near the street start; try street-only.
                for v in street_name_variants:
                    latlon = try_query("block_0_street_only", v)
                    if latlon != (None, None):
                        break
            else:
                # Try a few representative numbers within the block.
                candidates = [block_num, block_num + 50, block_num + 99]
                for n in candidates:
                    for v in street_name_variants:
                        latlon = try_query("block_to_address", f"{n} {v}")
                        if latlon != (None, None):
                            break
                    if latlon != (None, None):
                        break

                # If OSM search still couldn't resolve a derived address, try Census.
                if latlon == (None, None):
                    for n in candidates:
                        for v in street_name_variants:
                            latlon = try_census("census_block_to_address", f"{n} {v}")
                            if latlon != (None, None):
                                break
                        if latlon != (None, None):
                            break

                # If Nominatim couldn't resolve the derived address, try Overpass
                # addr:housenumber + addr:street (often succeeds when search fails).
                if latlon == (None, None):
                    for n in candidates:
                        for v in street_name_variants:
                            try:
                                attempts.append({"label": "overpass_addr", "q": f"{n} {v}"})
                                latlon = _overpass_find_address(str(n), v)
                                if latlon != (None, None):
                                    used_query = f"overpass_addr: {n} {v}"
                                    break
                            except Exception as e:
                                errors.append({"label": "overpass_addr", "q": f"{n} {v}", "error": str(e)})
                        if latlon != (None, None):
                            break

            # Last ditch for block: street-only
            if latlon == (None, None):
                for v in street_name_variants:
                    latlon = try_query("block_street_only", v)
                    if latlon != (None, None):
                        break

            # If Nominatim still can't place the street, use Overpass to pick
            # an approximate center for the named way.
            if latlon == (None, None):
                try:
                    candidates = []
                    if street:
                        candidates.append(street)
                    if street_expanded and street_expanded != street:
                        candidates.append(street_expanded)

                    for cand in candidates:
                        attempts.append({"label": "overpass_way_center", "q": cand})
                        latlon = _overpass_find_way_center(cand)
                        if latlon != (None, None):
                            used_query = f"overpass_center: {cand}"
                            break
                except Exception as e:
                    errors.append({"label": "overpass_way_center", "q": street_expanded or street, "error": str(e)})

            a2 = street_expanded or street

        # 5) Detect intersection patterns
        if latlon == (None, None):
            # Normalize common separators to a single token for splitting.
            normalized_for_split = a2
            normalized_for_split = normalized_for_split.replace(" AT ", " & ").replace(" @ ", " & ")
            normalized_for_split = normalized_for_split.replace(" AND ", " & ")
            normalized_for_split = normalized_for_split.replace("/", " & ")
            if "&" in normalized_for_split:
                parts = [p.strip() for p in normalized_for_split.split("&") if p.strip()]
                if len(parts) >= 2:
                    p1, p2 = parts[0], parts[1]

                    def street_variants(s: str) -> list[str]:
                        import re

                        base_s = _clean_whitespace(s)
                        variants = []
                        if base_s:
                            variants.append(base_s)
                        expanded_s = _normalize_street_text(base_s)
                        if expanded_s and expanded_s not in variants:
                            variants.append(expanded_s)

                        # Directionless variants (sometimes OSM omits N/S/E/W)
                        for v in list(variants):
                            v2 = re.sub(r"^(North|South|East|West)\s+", "", v, flags=re.IGNORECASE)
                            v2 = _clean_whitespace(v2)
                            if v2 and v2 not in variants:
                                variants.append(v2)

                        # Highway variants: "Highway 63" often resolves better as "US 63"
                        hw_re = re.compile(r"\b(?:US\s+)?(?:HIGHWAY|HWY)\s*(\d+)\b", re.IGNORECASE)
                        for v in list(variants):
                            m = hw_re.search(v)
                            if m:
                                num = m.group(1)
                                for rep in [f"US {num}", f"US Highway {num}", f"Route {num}"]:
                                    v3 = hw_re.sub(rep, v)
                                    v3 = _clean_whitespace(v3)
                                    if v3 and v3 not in variants:
                                        variants.append(v3)

                        # Keep variants small to avoid too many API calls,
                        # but include highway variants like "US 63".
                        return variants[:5]

                    v1s = street_variants(p1)
                    v2s = street_variants(p2)

                    # Generate a small set of intersection query patterns
                    queries = []
                    for a in v1s:
                        for b in v2s:
                            queries.extend(
                                [
                                    f"{a} & {b}",
                                    f"{a} and {b}",
                                    f"{a} at {b}",
                                    f"{a} / {b}",
                                ]
                            )

                    # Deduplicate while preserving order
                    seen = set()
                    deduped = []
                    for q in queries:
                        qn = _clean_whitespace(q)
                        if qn and qn not in seen:
                            seen.add(qn)
                            deduped.append(qn)

                    # Cap attempts to respect Nominatim usage and keep runs fast.
                    # We fall back to Overpass quickly for intersections.
                    for inter in deduped[:4]:
                        latlon = try_query("intersection", inter)
                        if latlon != (None, None):
                            print(f"  Found intersection: {inter}")
                            break

                    # If Nominatim can't resolve this intersection at all,
                    # fall back to Overpass to find the shared node.
                    if latlon == (None, None):
                        try:
                            # Try a few street variants (including highway substitutions)
                            # to better match OSM naming.
                            best = (None, None)
                            best_q = None
                            for a_cand in v1s[:3]:
                                for b_cand in v2s[:3]:
                                    best = _overpass_find_intersection(a_cand, b_cand)
                                    best_q = f"{a_cand} & {b_cand}"
                                    if best != (None, None):
                                        break
                                if best != (None, None):
                                    break

                            latlon = best
                            if latlon != (None, None):
                                attempts.append({"label": "overpass_intersection", "q": best_q})
                                used_query = f"overpass: {best_q}"
                        except Exception as e:
                            errors.append({"label": "overpass_intersection", "q": f"{p1} & {p2}", "error": str(e)})

        # 6) Ramp/off-ramp style strings: try stripping ramp tokens and geocoding remaining
        if latlon == (None, None):
            simplified = _normalize_street_text(a2)
            if simplified and simplified != a2:
                latlon = try_query("simplified", simplified)

        # 7) Final fallback: remove literal words that often appear in dispatch strings
        if latlon == (None, None):
            trimmed = re.sub(r"\b(BLOCK|OF)\b", " ", raw_address, flags=re.IGNORECASE)
            trimmed = _clean_whitespace(trimmed)
            trimmed = _normalize_street_text(trimmed)
            latlon = try_query("final_trim", trimmed)

        # 8) If this still looks like a real address, try Overpass addr tags.
        if latlon == (None, None):
            m_addr = re.match(r"^\s*(\d{1,6})(?:-[A-Z0-9]+)?\s+(.+?)\s*$", normalized_dispatch or raw_address, flags=re.IGNORECASE)
            if m_addr:
                hn = m_addr.group(1)
                st = _clean_whitespace(m_addr.group(2))
                st = _strip_unit(st) or st
                try:
                    attempts.append({"label": "overpass_addr", "q": f"{hn} {st}"})
                    latlon = _overpass_find_address(hn, st)
                    if latlon != (None, None):
                        used_query = f"overpass_addr: {hn} {st}"
                except Exception as e:
                    errors.append({"label": "overpass_addr", "q": f"{hn} {st}", "error": str(e)})

    cache[key] = latlon
    if debug is not None:
        try:
            debug.update(
                {
                    "raw": raw_address,
                    "cache_key": key,
                    "attempts": attempts,
                    "used_query": used_query,
                    "result": {"lat": latlon[0], "lon": latlon[1]},
                    "errors": errors,
                }
            )
        except Exception:
            pass
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
    # Per-run memoization: many incidents repeat the same location string.
    # This avoids re-hitting Nominatim/Overpass/Census for duplicates.
    local_geocode_cache: dict[str, tuple[float | None, float | None]] = {}
    local_debug_cache: dict[str, dict] = {}

    # Keep Nominatim friendly by default, but allow overrides if you want.
    # Nominatim policy generally expects ~1 request/second.
    try:
        sleep_time = float(os.getenv("GEOCODE_SLEEP_SECONDS", "1.0"))
    except Exception:
        sleep_time = 1.0

    # For iterative runs: you can skip re-trying already-failed cached lookups.
    # This speeds things up a lot when you're just regenerating data.json.
    # Set RETRY_FAILED_CACHE=0 to disable.
    retry_failed_cache_env = os.getenv("RETRY_FAILED_CACHE", "1").strip().lower()
    retry_failed_cache = retry_failed_cache_env not in {"0", "false", "no", "off"}
    geocoded = 0
    skipped = 0
    unmapped_debug = []

    for i, item in enumerate(items):
        addr = item.get("location_txt", "")
        dbg = {}
        addr_key = _clean_whitespace(addr).upper()
        if addr_key in local_geocode_cache:
            lat, lon = local_geocode_cache[addr_key]
            # Reuse debug (best-effort) so unmapped report stays helpful.
            try:
                dbg.update(local_debug_cache.get(addr_key) or {})
            except Exception:
                pass
        else:
            lat, lon = geocode_address(
                addr,
                geocache,
                sleep_time=sleep_time,
                debug=dbg,
                retry_failed_cache=retry_failed_cache,
            )
            local_geocode_cache[addr_key] = (lat, lon)
            local_debug_cache[addr_key] = dict(dbg)
        if lat is not None and lon is not None:
            geocoded += 1
        else:
            skipped += 1
            unmapped_debug.append(
                {
                    "incident": item.get("incident"),
                    "datetime": item.get("datetime"),
                    "type": item.get("type"),
                    "agency": item.get("agency"),
                    "location_txt": addr,
                    "service": item.get("service"),
                    "geocode_debug": dbg,
                }
            )
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

    # Write a debug report for anything that couldn't be mapped
    try:
        if unmapped_debug:
            with open(GEOCODE_UNMAPPED_DEBUG_FILE, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "generated_at": generated_at,
                        "dispatch_date": target_date_str,
                        "unmapped_count": len(unmapped_debug),
                        "items": unmapped_debug,
                    },
                    f,
                    indent=2,
                )
            print(f"Wrote {GEOCODE_UNMAPPED_DEBUG_FILE} with {len(unmapped_debug)} unmapped incidents.")
        else:
            # Keep the previous file from going stale/misleading
            if os.path.exists(GEOCODE_UNMAPPED_DEBUG_FILE):
                os.remove(GEOCODE_UNMAPPED_DEBUG_FILE)
    except Exception as e:
        print(f"Could not write unmapped debug report: {e}")

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)
    print(f"\nSaved data.json. Geocoded: {geocoded}, Skipped: {skipped}")

if __name__ == "__main__":
    main()
