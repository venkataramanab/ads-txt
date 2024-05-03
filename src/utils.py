import re
import urllib


def regex_checker(text, regex):
    text = text.strip()
    pattern = re.compile(regex)

    return bool(pattern.match(text))


def get_url_category(url):
    playstore_regex = r"^https://play.google.com/store/apps/details\S+"
    appstore_regex = r"^https://apps.apple.com/\S+/app/\S+/id\S+"
    web_regex = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\." + \
        r"[a-z]{2,4}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"

    if regex_checker(url, playstore_regex):
        return "playstore"
    elif regex_checker(url, appstore_regex):
        return "appstore"

    # if not url.startswith("http"):
    #     url = "http://" + url

    return regex_checker(url, web_regex) and (url, url + "/ads.txt", "-")


def validate_bundle_id(text):
    regex = r"^(?:[a-zA-Z]+(?:\d*[a-zA-Z_]*)*)(?:\.[a-zA-Z]+(?:\d*[a-zA-Z_]*)*)+$"
    if regex_checker(text, regex):
        return f"https://play.google.com/store/apps/details?id={text}"

    try:
        bundle_id = int(text)
        return f"https://apps.apple.com/us/app/id{bundle_id}"
    except Exception as e:
        print(e)
        print(f"Invalid bundle id: {text}")
