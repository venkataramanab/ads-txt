import os
import threading
import time
import hashlib
from src.settings import default_request_timeouts
from src.utils import get_url_category, validate_bundle_id
from src.extractor import AppContentExtractor, ContentExtractor
from itunes_app_scraper.scraper import AppStoreException


class AdsDotTxtScraper:
    store = {}
    state = {}
    rlock = threading.RLock()

    def __init__(self, config):
        self.__parse_config(config)
        self.content_extractor = ContentExtractor(self.request_timeouts if config else default_request_timeouts)
        self.app_content_extractor = AppContentExtractor()

    def __parse_config(self, config):
        for k, v in config.items():
            setattr(self, k, v)

    def __build_target(self, target):
        _target = get_url_category(target)

        if _target:
            if _target in ["playstore", "appstore"]:
                return self.app_content_extractor.process(target)
            return _target
        else:
            _target = validate_bundle_id(target)
            return self.app_content_extractor.process(_target) if _target else None

    def get_cache_file_name(self, a_url):
        return f"temp/{hashlib.md5(a_url.encode()).hexdigest()}.txt"

    def get_cached_ads_txt_contents(self, a_url):
        file_name = self.get_cache_file_name(a_url)
        with open(file_name) as fp:
            return fp.read()

    def cache_ads_txt_file(self, a_url, contents):
        file_name = self.get_cache_file_name(a_url)
        os.makedirs('temp', exist_ok=True)
        with open(file_name, 'w') as fp:
            fp.write(contents)
        return file_name

    def scrape(self, t_url, app_name, a_url):
        # try:
        #     _target = self.__build_target(target)
        # except (AppStoreException, RuntimeError) as e:
        #     return '-', '-', None, e
        #
        # if not _target:
        #     return

        # t_url, a_url, app_name = _target
        if a_url in self.state:
            while self.state[a_url]['code'] == 1:  # Queued
                time.sleep(0.1)
                # print(f"waiting for {a_url}")
            if self.state[a_url]['code'] == 2:  # Failed
                return t_url, app_name, None, self.state[a_url]['e']
        else:
            with self.rlock:
                self.state[a_url] = {'code': 1}
                # print(f"url queued {a_url}")

        if a_url not in self.store:
            try:
                response = self.content_extractor.request_page(a_url)
                # if not response:
                #     print(f"failed to get {a_url}")
                is_https, content_type, _url, text = response
                with self.rlock:
                    self.cache_ads_txt_file(a_url, text)
                    self.store[a_url] = (is_https, content_type)
                    self.state[a_url].update({'code': 0})  # Success
                    return t_url, app_name, (is_https, content_type, a_url, text), None
            except RuntimeError as e:
                # print(f'error while getting {a_url}', e)
                with self.rlock:
                    self.store[a_url] = None
                    self.state[a_url].update({'code': 2, 'e': e})
                return t_url, app_name, None, e

        # if not self.store.get(a_url):
        #     print(f"Retrying...  {a_url}  < 2 seconds >")
        #     time.sleep(2)
        #     self.store[a_url] = self.content_extractor.request_page(a_url)
        if self.store[a_url]:
            is_https, content_type = self.store[a_url]
            contents = self.get_cached_ads_txt_contents(a_url)
            return t_url, app_name, (is_https, content_type, a_url, contents), None

        return t_url, app_name, None, self.state[a_url]['e']
