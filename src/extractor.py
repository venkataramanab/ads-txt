import re
import extruct
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from src.settings import ua, default_request_timeouts
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from itunes_app_scraper.scraper import AppStoreScraper

class ContentExtractor:

    def __init__(self, request_timeouts=default_request_timeouts):
        self.request_timeouts = request_timeouts["connect"], request_timeouts["read"]

    def request_page(self, url, text_only=False):
        print(f"Fetching...  {url}")
        try:
            response = requests.get(
                url, allow_redirects=True,
                headers={"User-Agent": ua.random},
                timeout=self.request_timeouts
            )
        except (ReadTimeout, ConnectTimeout, ConnectionError) as e:
            raise RuntimeError(f"ConnectionError {e}")

        if response.status_code != 200:
            if response.status_code == 429:
                message = 'Rate Limit Exceeded'
            elif response.status_code == 404:
                if url.find('ads.txt') > -1:
                    message = 'Ads.txt Not Found'
                else:
                    message = 'App Not Found'
            else:
                message = f'NA - {response.status_code}'
            raise RuntimeError(f"{message}")

        text = response.text
        if text_only:
            return text

        _url = response.url
        is_https = _url.startswith("https://")
        content_type = response.headers.get("Content-Type")

        return is_https, content_type, _url, text


class AppContentExtractor(ContentExtractor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.appstore_scraper = AppStoreScraper()
        self.appstore_regex = re.compile(r"^https://apps.apple.com/(\S+)/app(?:/\S+)?/id(\d+)(?:/\S+)?")

    def get_app_details_from_itunes(self, url):
        search_result = self.appstore_regex.search(url)
        if search_result:
            country, app_id = search_result.groups()
            app_details = self.appstore_scraper.get_app_details(app_id, country=country.lower())
            return app_details.get('sellerUrl'), app_details.get('trackName')

    def process(self, url):
        is_appstore_url = self.appstore_regex.match(url)
        if is_appstore_url:
            # for i in range(40):
            seller_url, title = self.get_app_details_from_itunes(url)
            if seller_url:
                return (url, f"https://{urlparse(seller_url).netloc}/app-ads.txt", title)
        text = self.request_page(url, text_only=True)
        json_ld = extruct.extract(text, syntaxes=["json-ld"])["json-ld"][0]

        title = json_ld.get("name", "-")
        author = json_ld.get("author")

        if url.startswith("https://apps.apple.com"):
            _soup = BeautifulSoup(text, 'html.parser')
            _ul = _soup.find('ul', {"class": "inline-list--app-extensions"})

            if _ul:
                el = _ul.findAll('a')
                if el:
                    return (url, f"https://{urlparse(el[0].get('href')).netloc}/app-ads.txt", title)

        if author:
            _url = author.get("url")
            return _url and (url, f"https://{urlparse(_url).netloc}/app-ads.txt", title)
