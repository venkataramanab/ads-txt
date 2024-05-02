import csv
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime as dt

from src.scraper import AdsDotTxtScraper

TARGETS_FILE = 'targets.txt'
SEARCH_FILE = 'searches.txt'
RESULTS_FILE = f'results/results_{dt.now().strftime("%d_%m_%y")}.csv'
FAILED_FILE = f'results/failed_{dt.now().strftime("%d_%m_%y")}.txt'
GSHEET_FILE = 'ads_spec.xlsx'
GSHEET_ID = os.environ.get('GSHEET_ID')

FAILED_RETRY_SECS = 5
FAILED_RETRY_ATTEMPTS = 1


def download_gsheet():
    if not GSHEET_ID:
        raise ValueError('GSHEET_ID env variable is missing')
    url = f'https://docs.google.com/spreadsheets/d/{GSHEET_ID}/export'
    urllib.request.urlretrieve(url, GSHEET_FILE)


def read_file_contents(file_path):
    with open(file_path) as f:
        return f.read().splitlines()


def read_sheet_contents(sheet_name):
    return pd.read_excel(io=GSHEET_FILE, sheet_name=sheet_name, dtype=str).iloc[:, 0].tolist()


def dump_results(results, cols):
    with open(RESULTS_FILE, "w", newline="") as csvfile:
        fieldnames = ["TARGET", "APP_NAME", "URL", "ADS.TXT", "IS HTTPS?"] + cols + ["REMARKS"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)


def dump_failures(failed):
    with open(FAILED_FILE, "w", newline="") as f:
        for i in failed:
            f.write(i + "\n")


def preprocess(_scraper, line, results, failed, fill_ups, trial=FAILED_RETRY_ATTEMPTS):
    if not (line and len(line.strip())):
        return

    status = True
    _scraped = []
    _check = None
    try:
        # TODO: App store bundle ids whose url has a country code other than "us" won't be available for scraping. Need to pass the full url into the scraper.
        _scraped = _scraper.scrape(line)
        _check = _scraped[2]
    except:
        status = False

    if not (status and _check):
        if trial:
            print(f"Retrying...  {line}  < 5 seconds >")
            time.sleep(FAILED_RETRY_SECS)
            return preprocess(_scraper, line, results, failed, fill_ups, trial - 1)

        results.append({"TARGET": line, "APP_NAME": "-", "URL": "-", "ADS.TXT": "Failed", "IS HTTPS?": "-", **fill_ups,
                        "REMARKS": "Unable to scrape data."})
        failed.append(line)
        return

    return _scraped


def process(_scraper, line, results, failed, fill_ups, cols, default_cols=False):
    preprocessed = preprocess(_scraper, line, results, failed, fill_ups)
    if not preprocessed:
        # continue
        return

    t_url, app_name, scraped_data = preprocessed
    is_https, content_type, a_url, content = scraped_data
    if content_type and "text/plain" not in content_type:
        failed.append(line)
        results.append(
            {"TARGET": line, "APP_NAME": "-", "URL": "-", "ADS.TXT": "-", "IS HTTPS?": is_https, **fill_ups,
             "REMARKS": "Text content not found."})
        # continue
        return

    splitted_content = [i.strip("\r") for i in content.split("\n") if i.strip()]
    no_of_lines = len(splitted_content)

    if no_of_lines < 5:
        results.append(
            {"TARGET": line, "APP_NAME": app_name, "URL": t_url, "ADS.TXT": a_url, "IS HTTPS?": is_https,
             **fill_ups,
             "REMARKS": f"{no_of_lines} lines only."})
        # continue
        return

    r_dict = {"TARGET": line, "APP_NAME": app_name, "URL": t_url, "ADS.TXT": a_url, "IS HTTPS?": is_https}
    for i in cols:
        options = (", ".join(i.lower().split(",")), ",".join(i.lower().split(",")))
        if default_cols:
            values = "True" if (
                    options[0] in str(content).lower() or options[1] in str(content).lower()) else "False"
        else:
            values = "; ".join(
                filter(lambda x: (options[0] in x.lower() or options[1] in x.lower()), splitted_content)) or "-"
        r_dict[i] = values

    results.append(r_dict)


def run():
    cols = read_sheet_contents("search")
    data = read_sheet_contents("targets")
    default_cols = False

    results = []
    failed = []
    fill_ups = {i: "-" for i in cols}

    scraper = AdsDotTxtScraper({})

    futures = []
    with ThreadPoolExecutor() as pool:
        for line in data:
            futures.append(pool.submit(process, *(scraper, line, results, failed, fill_ups, cols, default_cols)))
    for future in futures:
        future.result()
    dump_results(results, cols)
    dump_failures(failed)


if __name__ == '__main__':
    download_gsheet()
    run()
