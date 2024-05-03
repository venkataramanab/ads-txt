import threading
import time

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

    def scrape(self, target):
        try:
            _target = self.__build_target(target)
        except (AppStoreException, RuntimeError) as e:
            return '-', '-', None, e

        if not _target:
            return

        t_url, a_url, app_name = _target
        if a_url in self.state:
            while self.state[a_url]['code'] == 1:  # Queued
                time.sleep(0.1)
            if self.state[a_url]['code'] == 2:  # Failed
                return t_url, app_name, None, self.state[a_url]['e']
        else:
            with self.rlock:
                self.state[a_url] = {'code': 1}

        if a_url not in self.store:
            try:
                response = self.content_extractor.request_page(a_url)
                with self.rlock:
                    self.store[a_url] = response
                    self.state[a_url].update({'code': 0})  # Success
            except RuntimeError as e:
                print(e)
                with self.rlock:
                    self.store[a_url] = None
                    self.state[a_url].update({'code': 2, 'e': e})
                return t_url, app_name, None, e

        # if not self.store.get(a_url):
        #     print(f"Retrying...  {a_url}  < 2 seconds >")
        #     time.sleep(2)
        #     self.store[a_url] = self.content_extractor.request_page(a_url)

        return t_url, app_name, self.store[a_url], None
