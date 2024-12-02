"""
Usage:
    app.py --draw_number=<draw_number> --draw_date=<draw_date> --draw_result=<draw_result>
    app.py --draw_object=<draw_object>

Options:
    --draw_number=<draw_number>   Draw number (integer).
    --draw_date=<draw_date>       Draw date (YYYY-MM-DD format).
    --draw_result=<draw_result>   Draw result (comma-separated list of integers).
    --draw_object=<draw_object>   Json object having above attributes.
"""

import os
import psycopg2
from psycopg2 import sql
from docopt import docopt
from datetime import datetime
import sys
import json


def validate_arguments(args):
    if args["--draw_number"]:
        # Validate draw_number (integer)
        try:
            draw_number = int(args["--draw_number"])
            if draw_number <= 0:
                raise ValueError("Draw number must be a positive integer.")
        except ValueError as e:
            raise ValueError(f"Invalid draw number: {e}")

        # Validate draw_date (YYYY-MM-DD format)
        try:
            draw_date = datetime.strptime(args["--draw_date"], "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Invalid draw date format: {e}")

        # Validate draw_result (comma-separated list of integers)
        try:
            draw_result = [int(num) for num in args["--draw_result"].split(",")]
            if not all(1 <= num <= 47 for num in draw_result):  # Example validation
                raise ValueError("Draw results must be integers between 1 and 47.")
            if len(draw_result) != 7:
                raise ValueError("Draw results must be 7 digits")
        except ValueError as e:
            raise ValueError(f"Invalid draw result: {e}")
    elif args["--draw_object"]:
        try:
            draw_object = json.loads(args["--draw_object"].replace("'", '"'))
            draw_number = draw_object["draw_number"]
            draw_date = draw_object["draw_date"]
            draw_result = draw_object["draw_result"]
            # validate draw object
            if draw_number <= 0:
                raise ValueError("Draw number must be a positive integer.")
            # throws exception if not valid
            draw_date = datetime.strptime(draw_date, "%Y-%m-%d").date()
            if len(draw_result) != 7:
                raise ValueError("Draw result must have 7 numbers.")
        except ValueError as e:
            raise ValueError(f"Invalid draw number: {e}")

    return draw_number, draw_date, draw_result


def insert_record(draw_number, draw_date, draw_result):
    # Retrieve credentials from environment variables
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    database = os.getenv("POSTGRES_DB")

    if not all([user, password, host, database]):
        raise EnvironmentError(
            "PostgreSQL credentials are not set in the environment variables."
        )

    # Connect to PostgreSQL
    conn = psycopg2.connect(dbname=database, user=user, password=password, host=host)
    cursor = conn.cursor()

    # SQL query to insert a record
    insert_query = sql.SQL(
        """
        INSERT INTO lotto (draw_number, draw_date, draw_result)
        VALUES (%s, %s, %s)
    """
    )

    # Insert a record
    cursor.execute(insert_query, (draw_number, draw_date, draw_result))

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # Parse command-line arguments
    args = docopt(__doc__)

    try:
        # Validate arguments
        draw_number, draw_date, draw_result = validate_arguments(args)

        # Insert the record
        insert_record(draw_number, draw_date, draw_result)
        print("Record inserted successfully.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(10)
