"""
Usage:
    app.py --logical_date=<logical_date>

Options:
    --logical_date=<logical_date>       Draw date (YYYY-MM-DD format).
"""

import requests
from docopt import docopt
from datetime import datetime, timedelta
import sys
import re
import json
import pathlib


def get_previous_tuesday(given_date):
    """
    Returns the given date if it's a Tuesday; otherwise, returns the previous Tuesday.

    Args:
        given_date (datetime.date or datetime): The input date.

    Returns:
        datetime.date: The Tuesday date.
    """

    # Check if the given date is a Tuesday (weekday == 1)
    if given_date.weekday() == 1:
        return given_date

    # Calculate the previous Tuesday
    days_to_subtract = (given_date.weekday() - 1) % 7
    previous_tuesday = given_date - timedelta(days=days_to_subtract)
    return previous_tuesday


def get_draw_object_from_date(
    logical_date: datetime,
):  # format as Airflow DS - 2024-01-18
    run_date = get_previous_tuesday(logical_date)
    # earliest available info on the internet
    first_play = {"date": datetime(2006, 2, 7), "draw": 625}
    diff_days = (run_date - first_play["date"]).days
    return {
        "draw_number": first_play["draw"] + (diff_days // 7),
        "draw_date": run_date.strftime("%Y-%m-%d"),
    }


def get_results_htlm(draw_object: dict):
    lotto_url = (
        "https://www.ozlotteries.com/oz-lotto/results/draw/{draw_number}".format(
            draw_number=draw_object["draw_number"]
        )
    )
    headers = {"User-Agent": "curl/8.5.0"}
    response = requests.get(lotto_url, headers=headers)
    return response.text


def extract_result_numbers_from_html(xhtml: str):
    matched = re.search(r"numberSets.*?divisions", xhtml)
    cleaned = (
        str(matched.group()).replace(',"divisions', "").replace('numberSets":', "")
    )
    print(cleaned)
    jc = json.loads(cleaned)
    print(jc[0].get("numbers"))
    win_numbers = [num.get("number") for num in jc[0].get("numbers")]
    return win_numbers


def validate_arguments(args):
    # Validate logical_date (YYYY-MM-DD format)
    try:
        draw_date = datetime.strptime(args["--logical_date"], "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid draw date format: {e}")
    return draw_date


if __name__ == "__main__":
    # Parse command-line arguments
    args = docopt(__doc__)

    try:
        # Validate arguments
        draw_date = validate_arguments(args)
        draw_object = get_draw_object_from_date(draw_date)
        xhtml_response = get_results_htlm(draw_object=draw_object)
        draw_object["draw_result"] = extract_result_numbers_from_html(
            xhtml=xhtml_response
        )
        # print draw object to be catched by ariflow as xcom
        print(draw_object)
        pathlib.Path("/airflow/xcom").mkdir(parents=True, exist_ok=True)
        with open("/airflow/xcom/return.json", "w") as xcom_file:
            json.dump(draw_object, xcom_file)
        print("Record inserted successfully.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(10)
