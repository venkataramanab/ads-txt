import csv
import time
from src.settings import get_default_cols
from src.scraper import AdsDotTxtScraper

config = {
    ## page request timeouts can be manipulated here
    "request_timeouts": {
        "connect": 2,
        "read": 5
    },
    "mode": "local"
}


def take_input(text, mandatory=True):
    filename = input(f"Enter {text} filename (expected .txt): ")

    if not mandatory and not filename:
        return

    filename = filename.strip()
    try:
        *_, ext = filename.split(".")
    except:
        print("Error in filename")
        take_input(text)

    ext = ext.lower()
    if ext != "txt":
        print("Only txt files allowed.")
        take_input(text)

    return filename


## From now, search keys must be passed enclosed in files too
## Previously, we were using comma-separated values
filenames = {"targets": True, "search-keys": False}
for k, v in filenames.items():
    filenames[k] = take_input(k, v)

default_cols = False
col_file = filenames["search-keys"]
if col_file:
    with open(col_file) as f:
        cols = f.read().splitlines()
else:
    default_cols = True
    cols = get_default_cols()

with open(filenames["targets"]) as f:
    data = f.read().splitlines()

out_file = input(f"Enter output filename < default: results.csv >: ")
if not out_file:
    out_file = "results.csv"

results = []
failed = []
fillups = {i: "-" for i in cols}
_scraper = AdsDotTxtScraper(config)


def preprocess(line, trial=1):
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
            time.sleep(5)
            return preprocess(line, trial - 1)

        results.append({"TARGET": line, "APP_NAME": "-", "URL": "-", "ADS.TXT": "Failed", "IS HTTPS?": "-", **fillups,
                        "REMARKS": "Unable to scrape data."})
        failed.append(line)
        return

    return _scraped


for line in data:
    preprocessed = preprocess(line)
    if not preprocessed:
        continue

    t_url, app_name, scraped_data = preprocessed
    is_https, content_type, a_url, content = scraped_data
    if content_type and "text/plain" not in content_type:
        failed.append(line)
        results.append({"TARGET": line, "APP_NAME": "-", "URL": "-", "ADS.TXT": "-", "IS HTTPS?": is_https, **fillups,
                        "REMARKS": "Text content not found."})
        # results.append({"TARGET": line, "APP_NAME": app_name, "URL": t_url, "ADS.TXT": a_url, "IS HTTPS?": is_https, **fillups, "REMARKS": "Text content not found."})
        continue

    splitted_content = [i.strip("\r") for i in content.split("\n") if i.strip()]
    no_of_lines = len(splitted_content)

    if no_of_lines < 5:
        results.append(
            {"TARGET": line, "APP_NAME": app_name, "URL": t_url, "ADS.TXT": a_url, "IS HTTPS?": is_https, **fillups,
             "REMARKS": f"{no_of_lines} lines only."})
        continue

    r_dict = {"TARGET": line, "APP_NAME": app_name, "URL": t_url, "ADS.TXT": a_url, "IS HTTPS?": is_https}
    for i in cols:
        options = (", ".join(i.lower().split(",")), ",".join(i.lower().split(",")))
        if default_cols:
            values = "True" if (options[0] in str(content).lower() or options[1] in str(content).lower()) else "False"
        else:
            values = "; ".join(
                filter(lambda x: (options[0] in x.lower() or options[1] in x.lower()), splitted_content)) or "-"
        r_dict[i] = values

    results.append(r_dict)

if not out_file.endswith(".csv"):
    out_file += ".csv"

with open(out_file, "w", newline="") as csvfile:
    fieldnames = ["TARGET", "APP_NAME", "URL", "ADS.TXT", "IS HTTPS?"] + cols + ["REMARKS"]

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(results)

with open("failed.txt", "w", newline="") as f:
    for i in failed:
        f.write(i + "\n")
