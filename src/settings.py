import os
import json
from fake_useragent import UserAgent


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

USER_AGENTS_PATH = os.path.join(DATA_DIR, "user-agents.json")
CONSTANTS_PATH = os.path.join(DATA_DIR, "constants.json")

ua = UserAgent(cache_path=USER_AGENTS_PATH)


def get_default_cols():
    with open(CONSTANTS_PATH) as f:
        data = json.load(f)
        return data["default_cols"]


default_request_timeouts = {"connect": 1, "read": 2}
