import csv
import os
import re
from enum import Enum

import psutil
import sqlite3
import time
import urllib.request
from urllib.parse import parse_qs, urlparse
import argparse

from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime as dt
import extruct
from bs4 import BeautifulSoup
from src.extractor import ContentExtractor
from src.scraper import AdsDotTxtScraper
from google_play_scraper import app
from google_play_scraper.exceptions import NotFoundError
from itunes_app_scraper.scraper import AppStoreScraper, AppStoreException

TARGETS_FILE = 'targets.txt'
SEARCH_FILE = 'searches.txt'
RESULTS_FILE = f'results/results_{dt.now().strftime("%d_%m_%y")}.csv'
FAILED_FILE = f'results/failed_{dt.now().strftime("%d_%m_%y")}.txt'
GSHEET_FILE = 'ads_spec.xlsx'
GSHEET_ID = os.environ.get('GSHEET_ID')

FAILED_RETRY_SECS = 1
FAILED_RETRY_ATTEMPTS = 0


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


def preprocess(runner, _scraper, line, results, failed, fill_ups, trial=FAILED_RETRY_ATTEMPTS):
    if not (line and len(line.strip())):
        return

    status = True
    _scraped = []
    _check = None
    try:
        # TODO: App store bundle ids whose url has a country code other than "us" won't be available for scraping. Need to pass the full url into the scraper.
        app_request: AppRequest = runner.build_app_request(line)
        if not app_request:
            return
        app_details = runner.get_app_from_db(app_request)
        if not app_details:
            return
        if app_details[7]:
            _scraped = [line, '-', '-', app_details[7]]
            raise Exception(app_details[7])
        _scraped = _scraper.scrape(app_details[0], app_details[1], app_details[2] + '/app-ads.txt')
        if not _scraped:
            print(f'Unable to identify target {line}')
            raise Exception(f'Unable to identify target {line}')
        _check = _scraped[2]
    except Exception as e:
        print(e)
        status = False

    if not (status and _check):
        if trial:
            print(f"Retrying...  {line}  < {FAILED_RETRY_SECS} seconds >")
            time.sleep(FAILED_RETRY_SECS)
            return preprocess(_scraper, line, results, failed, fill_ups, trial - 1)
        if _scraped:
            results.append(
                {"TARGET": line, "APP_NAME": _scraped[1], "URL": _scraped[0], "ADS.TXT": "-", "IS HTTPS?": "-",
                 **fill_ups,
                 "REMARKS": _scraped[3]})
        else:
            results.append(
                {"TARGET": line, "APP_NAME": "-", "URL": "-", "ADS.TXT": "Failed", "IS HTTPS?": "-", **fill_ups,
                 "REMARKS": "Unable to scrape data."})
        failed.append(line)
        return

    return _scraped


def process(_id, runner, _scraper, line, results, failed, fill_ups, cols, default_cols=False):
    print(f"Running line no {_id}")
    print(
        f"CPU: {psutil.cpu_percent()}, MEM_AVAILABLE: {psutil.virtual_memory().available * 100 / psutil.virtual_memory().total}")

    preprocessed = preprocess(runner, _scraper, line, results, failed, fill_ups)
    if not preprocessed:
        # continue
        return

    t_url, app_name, scraped_data, remarks = preprocessed
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


def run_local():
    cols = read_file_contents(SEARCH_FILE)
    data = read_file_contents(TARGETS_FILE)
    run(cols, data)


def run_gsheet():
    download_gsheet()
    cols = read_sheet_contents("search")
    data = read_sheet_contents("targets")
    run(cols, data)


def run(cols, data):
    default_cols = False

    results = []
    failed = []
    fill_ups = {i: "-" for i in cols}

    scraper = AdsDotTxtScraper({})

    futures = []
    runner = Runner()
    with ThreadPoolExecutor() as pool:
        for line_no, line in enumerate(data):
            futures.append(
                pool.submit(process, *(line_no, runner, scraper, line, results, failed, fill_ups, cols, default_cols)))
    for future in futures:
        future.result()
    dump_results(results, cols)
    dump_failures(failed)


class Store(int, Enum):
    APPSTORE = 0
    PLAYSTORE = 1


class AppRequest(dict):
    def __init__(self, store, app_id, country, language):
        super().__init__()
        self['store'] = store
        self['app_id'] = app_id
        self['country'] = country
        self['language'] = language

    @property
    def store(self):
        return self['store']

    @property
    def app_id(self):
        return self['app_id']

    @property
    def country(self):
        return self['country']

    @property
    def language(self):
        return self['language']

    @property
    def full_url(self):
        if self.store == Store.APPSTORE:
            return f'https://apps.apple.com/{self.country}/app/id{self.app_id}'
        elif self.store == Store.PLAYSTORE:
            return f'https://play.google.com/store/apps/details?id={self.app_id}&gl={self.country}&hl={self.language}'


class AppResponse(dict):
    def __init__(self, app_id, app_name, app_domain, store, country, last_checked_at, language=None, notes=None):
        super().__init__()
        self['notes'] = notes
        self['last_checked_at'] = last_checked_at
        self['language'] = language
        self['country'] = country
        self['store'] = store
        self['app_domain'] = app_domain
        self['app_name'] = app_name
        self['app_id'] = app_id

    @classmethod
    def from_app_request(cls, app_request: AppRequest, app_name=None, app_domain=None, notes=None):
        return cls(app_request.app_id, app_name, app_domain, app_request.store, app_request.country, dt.now(),
                   language=app_request.language, notes=notes)

    def __getattr__(self, item):
        return self[item]

    @property
    def as_tuple(self):
        return (
            self.app_id, self.app_name, self.app_domain, self.store, self.country, self.language,
            self.last_checked_at, self.notes)


class Runner(object):
    """
    Gathering ads.txt form App Store and playstore
    app_url,app_name,app-ads.txt,last_checked_at
    """
    appstore_pat = re.compile(r"^https://apps.apple.com/(\S+)/app(?:/\S+)?/id(\d+)(?:/\S+)?")
    playstore_pat = re.compile(r"^https://play.google.com/store/apps/details\S+")
    playstore_bundle_pat = re.compile(r"^(?:[a-zA-Z]+(?:\d*[a-zA-Z_]*)*)(?:\.[a-zA-Z]+(?:\d*[a-zA-Z_]*)*)+$")
    appstore_bundle_pat = re.compile(r"^\d+$")
    appstore = 0
    playstore = 1

    def __init__(self, **kwargs):
        self.only_new_apps = kwargs.get('only_new_apps', os.getenv('ONLY_NEW_APPS', False))
        self.force = kwargs.get('force', os.getenv('FORCE', False))
        self.db_path = 'data/app-ads-txt.db'
        self.appstore_scraper = AppStoreScraper()
        self.content_extractor = ContentExtractor()

    def _init_db(self):
        if not os.path.exists(self.db_path):
            self._create_tables()

    def _create_tables(self):
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute(
            '''CREATE TABLE apps (app_id text PRIMARY KEY NOT NULL, app_name text, app_domain text, store integer NOT NULL, country CHAR(3), language CHAR(3), last_checked_at text NOT NULL, notes text)''')
        con.commit()
        con.close()

    def get_app_from_db(self, app_request):
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("SELECT * from apps WHERE app_id=?", (app_request.app_id,))
        row = cur.fetchone()
        con.close()
        return row

    def _sync_app_on_db(self, app_response: AppResponse):
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute(
            "insert or replace into apps (app_id, app_name, app_domain, store, country, language, last_checked_at, notes) values (?,?,?,?,?,?,?,?);",
            app_response.as_tuple)
        con.commit()
        con.close()

    def _fetch_fallback_appstore_app_details(self, app_request: AppRequest):
        title, dev_website = None, None
        response = self.content_extractor.request_page(app_request.full_url, text_only=True)
        if response:
            json_ld = extruct.extract(response, syntaxes=["json-ld"])["json-ld"][0]
            title = json_ld.get("name")
            if app_request.store == Store.APPSTORE:
                _soup = BeautifulSoup(response, 'html.parser')
                _ul = _soup.find('ul', {"class": "inline-list--app-extensions"})
                if _ul:
                    el = _ul.findAll('a')
                    if el:
                        dev_website = f"https://{urlparse(el[0].get('href')).netloc}"
            elif app_request.store == Store.PLAYSTORE:
                author = json_ld.get("author")
                if author:
                    _url = author.get("url")
                    if _url:
                        dev_website = f"https://{urlparse(_url).netloc}"
        if title and dev_website:
            return AppResponse.from_app_request(app_request, title, dev_website)

    def _fetch_latest_app_details(self, app_request: AppRequest):
        title, dev_website = None, None
        if app_request.store == Store.PLAYSTORE:
            try:
                result = app(app_request.app_id, lang=app_request.language, country=app_request.country)
            except NotFoundError as e:
                return AppResponse.from_app_request(app_request, notes=str(e))
            if result:
                title, dev_website = result.get('title'), result.get('developerWebsite') or result.get('privacyPolicy')
        elif app_request.store == Store.APPSTORE:
            try:
                app_details = self.appstore_scraper.get_app_details(app_request.app_id, country=app_request.country)
            except AppStoreException as e:
                return AppResponse.from_app_request(app_request, notes=str(e))
            if app_details:
                title, dev_website = app_details.get('trackName'), app_details.get('sellerUrl')
        if title and dev_website:
            p_result = urlparse(dev_website)
            dev_website = f'{p_result.scheme}://{p_result.netloc}'
            return AppResponse.from_app_request(app_request, title, dev_website)
        else:
            return self._fetch_fallback_appstore_app_details(app_request)

    def _sync_app_details_if_required(self, app_request):
        app_result = self.get_app_from_db(app_request)
        if app_result:
            if self.only_new_apps:
                return
            if 'Could not parse app store response for ID' in app_result[7]:
                print(f'Rechecking existing app {app_request.app_id}')
            else:
                # check for expiry if expired fetch again
                # print(f"app_exists skipping {app_request.app_id}")
                if not self.force:
                    return
        print(f"fetching app details {app_request}")
        app_response = self._fetch_latest_app_details(app_request)
        if app_response:
            self._sync_app_on_db(app_response)
        else:
            print(f"failed to receive app_response for {app_request.app_id}")

    def build_app_request(self, cell):
        if self.appstore_pat.match(cell):
            country, app_id = self.appstore_pat.search(cell).groups()
            return AppRequest(Store.APPSTORE, app_id, country.lower(), "")
        if self.playstore_pat.match(cell):
            p_url = parse_qs(urlparse(cell).query)
            app_id = p_url.get('id', None)
            if not app_id:
                return
            return AppRequest(Store.PLAYSTORE, app_id[0], p_url.get('gl', 'US').lower(),
                              p_url.get('hl', 'en').lower())
        if self.playstore_bundle_pat.match(cell):
            return AppRequest(Store.PLAYSTORE, cell, 'us', 'en')
        if self.appstore_bundle_pat.match(cell):
            return AppRequest(Store.APPSTORE, cell, 'us', '')
        return

    def run(self):
        download_gsheet()
        self._init_db()
        col = read_sheet_contents("targets")
        futures = []
        with ThreadPoolExecutor(max_workers=4) as pool:
            for cell in col:
                cell = cell.strip()
                if cell:
                    app_request = self.build_app_request(cell)
                    if not app_request:
                        print(f"Issue with {cell}")
                        continue
                    futures.append(pool.submit(self._sync_app_details_if_required, app_request))
                    # self._sync_app_details_if_required(app_request)
            for future in futures:
                future.result()


def sync_apps(_args):
    Runner(**_args).run()


def ads_txt(_args):
    run_gsheet()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ads-txt runner')
    subparsers = parser.add_subparsers()

    sync_apps_parser = subparsers.add_parser('sync_apps', help='Sync apps')
    sync_apps_parser.set_defaults(func=sync_apps)
    sync_apps_parser.add_argument('--force', action='store_true', help='force latest info')
    sync_apps_parser.add_argument('--only-new-apps', action='store_true', help='sync only new apps')

    index_all_parser = subparsers.add_parser('ads_txt', help='Checks app-ads.txt')
    index_all_parser.set_defaults(func=ads_txt)

    args = parser.parse_args()
    args.func(vars(args))
