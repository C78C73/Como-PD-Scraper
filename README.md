# Como PD Incident Mapper

Interactive map of Columbia, MO Police Department incidents (6-hour delayed public data).

## Features
- Scrapes incident data from https://www.como.gov/CMS/911dispatch/police.php
- Geocodes addresses to map coordinates (with intelligent intersection handling)
- Displays incidents on an interactive Leaflet map
- Auto-handles 6-hour delay (retries with yesterday's date if no data for today)
- Caches geocoded addresses to minimize API calls

## Setup

### Prerequisites
- Python 3.11+ 
- Chrome browser (version 142+)
- ChromeDriver (download from https://googlechromelabs.github.io/chrome-for-testing/)

### Installation

1. Clone this repo:
```bash
git clone https://github.com/YOUR_USERNAME/Como-PD-Scraper.git
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
- Scrape incidents from the police dispatch site
- Geocode addresses to lat/lon coordinates
- Save results to `data.json`

### View the map:
```powershell
python -m http.server 8000
```

Then open http://localhost:8000/index.html in your browser.

## Files

- `run_scraper.py` - Main script (scraping + geocoding)
- `index.html` - Interactive map viewer
- `data.json` - Incident data with coordinates (generated)
- `geocache.json` - Cached geocoding results (generated)
- `requirements.txt` - Python dependencies

## GitHub Pages Deployment

To deploy the map to GitHub Pages:

1. Run the scraper locally to generate `data.json`
2. Commit `data.json` and `index.html` to your repo
3. Enable GitHub Pages in repo settings (source: main branch, root folder)
4. Your map will be available at `https://YOUR_USERNAME.github.io/Como-PD-Scraper/`

**Note:** You'll need to run the scraper locally and commit updated `data.json` manually, or set up a GitHub Action to run it automatically.

## License

MIT
