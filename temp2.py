import datetime
import firebase_admin
from firebase_admin import firestore


def convert_to_utc_datetime(date: datetime.date, time_str: str):
    """
    Converts a given time string into a Firestore-compatible UTC datetime.

    Assumes the input time is already in UTC.

    :param date: The date to use (typically today’s date).
    :param time_str: The time string (e.g., "11 am" or "8:30 pm").
    :return: Python datetime object in UTC.
    """

    # Determine correct time format
    time_format = "%I:%M %p" if ":" in time_str else "%I %p"

    # Combine date and time string
    utc_datetime = datetime.datetime.strptime(f"{date} {time_str}", f"%Y-%m-%d {time_format}")
    utc_datetime = utc_datetime.replace(tzinfo=datetime.timezone.utc)

    # Ensure it's a naive datetime (no timezone info), Firestore expects naive UTC datetime
    return utc_datetime

def store_in_firestore():
    """
    Fetches documents, updates the time fields, and stores them back in Firestore.
    """

    # Convert times to UTC datetime (assuming stored as string in the document)
    today = datetime.date.today()
    start_time_utc = convert_to_utc_datetime(today, "11 am")
    end_time_utc = convert_to_utc_datetime(today, "8:30 pm")
    print(start_time_utc.tzname())

    print(f"Updated time " + str(start_time_utc) + " / " + str(end_time_utc))
            

# Run the Firestore update process
store_in_firestore()
