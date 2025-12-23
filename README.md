# Como PD / Fire & EMS Incident Mapper

Interactive map of Columbia, MO Police Department and Fire/EMS incidents with real-time severity tracking.

🗺️ **[View Live Map](https://c78c73.github.io/Como-PD-Scraper/)**

## Features

- 🚨 **Automated scraping** every 6 hours via GitHub Actions
- 📍 **Interactive map** with color-coded severity markers:
  - 🔴 Red: Critical/Violent crimes (shots fired, assault, robbery)
  - 🟠 Orange: Serious incidents (burglary, domestic violence, injury crashes, serious fires)
  - 🟡 Yellow: Moderate issues (suspicious activity, theft, non-injury crashes, property crime)
  - 🟢 Green: Low priority (911 checks, assist citizen, parking)
  - 🔵 Blue: Other/uncategorized
- 📊 **Real-time statistics** showing incident count by severity
- 🧯 **Dual layers for Police vs Fire/EMS** with separate marker shapes and a toggle box to view either or both
- 🧭 **Smart geocoding** with intersection support (e.g., "Broadway & Providence")
- ⚡ **Caching system** to minimize API calls
- 🕐 **6-hour delay handling** (automatically retries with yesterday's date)

## Live Demo

Visit **https://c78c73.github.io/Como-PD-Scraper/** to view the live incident map.

The map updates automatically every 6 hours via GitHub Actions.

## Setup

### Prerequisites
- Python 3.11+ 
- Chrome browser (version 142+)
- ChromeDriver (download from https://googlechromelabs.github.io/chrome-for-testing/)

### Installation

1. Clone this repo:
```bash
git clone https://github.com/C78C73/Como-PD-Scraper.git
cd Como-PD-Scraper
```

2. Create a virtual environment and install dependencies:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Download ChromeDriver:
   - Go to https://googlechromelabs.github.io/chrome-for-testing/
   - Download the version matching your Chrome (check `chrome://version`)
   - Extract `chromedriver.exe` to the project folder

## Usage

### Run the scraper (scrape + geocode):
```powershell
python run_scraper.py
```

This will:
- Scrape incidents from the police and Fire/EMS dispatch sites
- Geocode addresses to lat/lon coordinates
- Save results to `data.json`

### View the map:
```powershell
python -m http.server 8000
```

Then open http://localhost:8000/index.html in your browser.

## Files

- `run_scraper.py` - Main script (Selenium scraping + Nominatim geocoding)
- `index.html` - Interactive Leaflet map with severity-based color coding and PD vs Fire/EMS layer controls
- `.github/workflows/scrape.yml` - GitHub Action for automated updates
- `data.json` - Incident data with coordinates (auto-generated)
- `geocache.json` - Cached geocoding results (auto-generated)
- `requirements.txt` - Python dependencies (selenium, beautifulsoup4, requests)

## How It Works

1. **Scraping**: Selenium navigates to the Como PD dispatch site, sets date filters, and extracts incident data from all paginated results
2. **Duplicate Detection**: Tracks incident IDs to prevent scraping the same data when pagination loops
3. **Geocoding**: Uses Nominatim (OpenStreetMap) API to convert addresses to coordinates
4. **Intersection Handling**: Detects intersection patterns (e.g., "Broadway/Stadium") and finds the meeting point
5. **Caching**: Stores geocoded addresses to avoid redundant API calls
6. **Automation**: GitHub Actions runs the scraper every 6 hours and commits updated data

## GitHub Actions Deployment

This repo includes automated deployment via GitHub Actions:

- **Schedule**: Runs every 6 hours (`0 */6 * * *`)
- **Manual Trigger**: Can be run manually from the Actions tab
- **Auto-commit**: Pushes updated `data.json` and `geocache.json` to the repo
- **GitHub Pages**: Serves the static HTML map at https://c78c73.github.io/Como-PD-Scraper/

No manual intervention needed - the map stays updated automatically!

## Data Sources

Data is scraped from the official Columbia, MO dispatch logs:

- Police: https://www.como.gov/CMS/911dispatch/police.php
- Fire & Rescue / EMS: https://www.como.gov/CMS/911dispatch/fire.php

**Note:** The dispatch site has a 6-hour delay for public data. The scraper automatically handles this by retrying with yesterday's date if today returns no results.

## License

MIT
