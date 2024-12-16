import datetime
import re
import time


def detect_foreign_characters(text):
    """
    Detect if a given text contains any Chinese, Thai, Arabic, or English characters.

    Args:
        text (str): The text to be checked.

    Returns:
        dict: A dictionary with the following keys:
             - 'chinese': True if the text contains Chinese characters, False otherwise.
             - 'thai': True if the text contains Thai characters, False otherwise.
             - 'arabic': True if the text contains Arabic characters, False otherwise.
             - 'english': True if the text contains English characters, False otherwise.
    """
    # Define regular expressions to match Chinese, Thai, Arabic, and English characters
    chinese_pattern = r"[\u4e00-\u9fff]"
    thai_pattern = r"[\u0e00-\u0e7f]"
    arabic_pattern = r"[\u0600-\u06ff\u0750-\u077f\u08a0-\u08ff]"
    english_pattern = r"[a-zA-Z]"

    languages = []
    # Check if the text contains any of the foreign characters
    contains_chinese = bool(re.search(chinese_pattern, text))
    if contains_chinese:
        languages.append("chinese")

    contains_thai = bool(re.search(thai_pattern, text))
    if contains_thai:
        languages.append("thai")

    contains_arabic = bool(re.search(arabic_pattern, text))
    if contains_arabic:
        languages.append("arabic")

    contains_english = bool(re.search(english_pattern, text))
    if contains_english:
        languages.append("english")

    return languages


def get_unix_timestamp():
    """
    Get the current timestamp in seconds since the Unix Epoch (1970-01-01 00:00:00 UTC).

    Returns:
        int: The current timestamp in seconds since the Unix Epoch.
    """
    now_dt = datetime.datetime.utcnow()
    unix_timestamp = int(time.mktime(now_dt.timetuple()))
    return unix_timestamp


def get_vod_user_level(chargingcp_id, free_time):

    if chargingcp_id == 1:
        return 3

    current_time = get_unix_timestamp()
    if free_time > current_time:
        return 2

    return 0


def is_chinese(query):
    # Regular expression to match Chinese characters
    chinese_pattern = re.compile(r"[\u4e00-\u9fff]")

    # Check if the query contains any Chinese characters
    if chinese_pattern.search(query):
        return True
    else:
        return False
