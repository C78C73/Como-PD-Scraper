import run_scraper


def main():
    cache = run_scraper.load_geocache()
    tests = [
        "1000 BLOCK E BROADWAY",
        "7000 BLOCK N BUCKINGHAM SQ",
        "W SEXTON RD/MCBAINE AVE",
        "VANDIVER DR/N HWY 63 NB",
        "1600-8D HANOVER BLVD",
    ]
    for t in tests:
        dbg = {}
        r = run_scraper.geocode_address(t, cache, sleep_time=0.0, debug=dbg, retry_failed_cache=True)
        print(t, "=>", r, type(r))


if __name__ == "__main__":
    main()
