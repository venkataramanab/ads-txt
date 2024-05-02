# Ads.txt Scraper

## Steps

- Open command prompt on this directory OR 'cd' into this directory.

- Initialize dependencies (required only on first install)

        pip3 install -r requirements.txt

        OR

        pip install -r requirements.txt

- Run command

        python3 start.py

        OR

        python start.py

- Enter required filenames:
    1. Targets filename - includes the urls/bundle_ids to be targeted - must be a txt file
    2. Searches filename - includes texts to be searched - must be a txt file - if not provided, takes in default columns from program itself
    3. Output filename - a csv file that will contain the output - if not provided, "results.csv" is taken as default


### Info

- In case of any error while scraping a page, the scraper will retry after 2 seconds of wait.
- In case of any error at target level, the scraper will retry after 5 seconds of wait.
- Whatever maybe the case, the failed target will be dumped into a file named "failed.txt"
    - This automatically generated file can be used again as the targets file.