import re
import extruct
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from src.settings import ua, default_request_timeouts
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError


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
            return None

        if response.status_code != 200:
            print(f"{url} failed with status code: {response.status_code}")
            return None

        text = response.text
        if text_only:
            return text

        _url = response.url
        is_https = _url.startswith("https://")
        content_type = response.headers.get("Content-Type")

        return is_https, content_type, _url, text


class AppContentExtractor(ContentExtractor):

    def process(self, url):
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
