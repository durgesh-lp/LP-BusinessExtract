import pandas as pd
import json
import firebase_admin
from firebase_admin import messaging, credentials, firestore
import re
import datetime

def extract_business_data(file_path, output_json):
    """Extracts business details from CSV, formats, uploads to Firebase, and saves JSON."""
    # Load CSV file
    df = pd.read_csv(file_path)
    
    # Select required columns
    selected_columns = ["Name", "Description", "Fulladdress", "Phone", "Phones", "Featured Image", "Latitude", "Longitude", "Street", "Website", "Place Id", "Opening Hours"]
    
    # Fill missing columns with empty string
    for col in selected_columns:
        if col not in df.columns:
            df[col] = ""
    
    df_selected = df[selected_columns].fillna("")
    
    # Process 'Phones' column to extract the first phone number and trim whitespace
    df_selected["Phones"] = df_selected["Phones"].astype(str).apply(lambda x: x.split(",")[0].replace(" ", "") if pd.notna(x) else "")
    
    # Process 'Phone' column to trim whitespace
    df_selected["Phone"] = df_selected["Phone"].astype(str).apply(lambda x: x.replace(" ", ""))
    
    # Initialize Firebase
    cred = credentials.Certificate("credentials_prod.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    
    business_list = []
    
    postcode_pattern = r"E\d{2} \w{3}"

    for _, row in df_selected.iterrows():
        postcode_match = re.search(postcode_pattern, row["Fulladdress"])
        # Extracted postcode
        postcode = postcode_match.group() if postcode_match else None
        doc_ref = db.collection("vendors").document()
        doc_id = doc_ref.id
        openingHoursRow = convert_hours_string_to_dict(row["Opening Hours"])
        openingHours = parse_opening_hours(openingHoursRow)
        print(openingHoursRow)
        print(openingHours)
        hdImageUrl = resize_google_image_url(row["Featured Image"])
        business_data = {
            "active": True,
            "address": row["Fulladdress"],
            "category": "Other",
            "city": "London",
            "contact": row["Phones"],
            "country": "United Kingdom",
            "description": row["Description"],
            "dynamicLink": "",
            "email": "",
            "endTime": firestore.SERVER_TIMESTAMP,
            "images": hdImageUrl,
            "isVerified": True,
            "latitude": row["Latitude"],
            "line1": row["Street"],
            "location": firestore.GeoPoint(float(row["Latitude"]), float(row["Longitude"])),
            "longitude": row["Longitude"],
            "name": row["Name"],
            "openingHours": openingHours,
            "ownerId": doc_id,
            "phone": row["Phone"],
            "pincode": postcode,
            "qrCode": "",
            "rating": {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "count": 0, "rating": 0},
            "ratings": [],
            "socialLinks": {"facebookId": "", "instaId": ""},
            "startTime": firestore.SERVER_TIMESTAMP,
            "state": "NA",
            "uid": doc_id,
            "website": row["Website"],
            "extracted": True,
            "claimed": False,
            "google_place_id": row["Place Id"],
            "workingDays": {day: True for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}
        }
        
        # Upload to Firebase
        #doc_ref = db.collection("TempBusinesses").add(business_data)
        #business_data["ownerId"] = doc_ref[1].id
        #business_data["uid"] = doc_ref[1].id
        
        isVendorExists = is_name_in_list(row["Name"])
        if isVendorExists:
            print("Already exists: " + row["Name"])
        else:
            doc_ref.set(business_data)
            #send_shop_onboard_notification(row["Fulladdress"], row["Name"], doc_id)
        #business_list.append(business_data)
    
    # Save to JSON
    #with open(output_json, "w", encoding="utf-8") as f:
    #    json.dump(business_list, f, indent=4)
    
    print(f"Data successfully saved to {output_json}")


def send_shop_onboard_notification(address: str, name: str, vendor_id: str):
    """
    Sends an FCM notification when a new shop is onboarded.

    :param address: The address of the shop.
    :param name: The name of the shop.
    :param vendor_id: The vendor ID.
    """
    title = "New Shop is Onboarded!!!"
    body = f"{name} is opened at {address}"
    topic = "vendorAdd"

    # Constructing the message
    message = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body
        ),
        data={
            "type": topic,
            "vendorId": vendor_id
        },
        topic=topic
    )

    # Sending the notification
    response = messaging.send(message)
    db = firestore.client()
    doc_ref = db.collection("userNotifications").document()
    userNotification = {
        "body": body,
        "title": title,
        "redirectLink": vendor_id,
        "timestamp": firestore.SERVER_TIMESTAMP,
    }
    doc_ref.set(userNotification)
    print("Successfully sent message:", response)
    
def is_name_in_list(name: str, filename: str = "names.json") -> bool:
    """
    Checks if a given name exists in the JSON file containing a list of names.

    :param name: The name to check.
    :param filename: The JSON file where the list is stored.
    :return: True if the name exists, otherwise False.
    """
    try:
        # Load the JSON file
        with open(filename, "r") as json_file:
            names = json.load(json_file)

        # Check if the name is in the list
        return name in names

    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading file: {e}")
        return False
    

def parse_opening_hours(row: dict):
    """
    Converts CSV row opening hours into Firestore openingHours format.

    :param row: Dictionary containing days as keys and their opening hours as values.
    :return: Dictionary formatted for Firestore.
    """

    # Default hours for days that might be missing
    default_hours = {
        "Monday": "10 am-8:30 pm",
        "Tuesday": "10 am-8:30 pm",
        "Wednesday": "10 am-8:30 pm",
        "Thursday": "10 am-8:30 pm",
        "Friday": "10 am-8:30 pm",
        "Saturday": "10 am-8:30 pm",
        "Sunday": "10 am-8 pm"
    }

    # Get the current date to construct timestamps
    today = datetime.date.today()

    opening_hours = {}

    for day in default_hours.keys():
        # Check if the day is present in the row; if not, mark it as closed
        if day in row:
            hours = row[day]
            is_open = True
        else:
            hours = default_hours[day]  # Use default hours
            is_open = False  # Mark as closed

        # Extract start and end time
        start_time, end_time = hours.split("-")

        # Define time format dynamically based on presence of minutes
        today = datetime.date.today()
        start_datetime = convert_to_utc_datetime(today, start_time)
        end_datetime = convert_to_utc_datetime(today, end_time)

        # Add Firestore timestamps
        opening_hours[day] = {
            "startTime": start_datetime,  # Firestore Timestamp with given datetime
            "endTime": end_datetime,  # Firestore Timestamp with given datetime
            "isOpen": is_open
        }

    return opening_hours

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

def convert_hours_string_to_dict(hours_string: str):
    """
    Converts an hours string format into a dictionary.

    :param hours_string: String containing opening hours in the given format.
    :return: Dictionary with day as key and hours as value.
    """

    # Fix: Replace Unicode narrow no-break spaces (`\u202f`) with standard spaces
    hours_string = hours_string.replace("\u202f", " ")

    # Fixed regex pattern: Now correctly captures both "11 am-8:30 pm" and "11:30 am-8 pm"
    pattern = r"(\w+): \[([\d:]+ ?[ap]m-[\d:]+ ?[ap]m)\]"

    # Extract matches
    matches = re.findall(pattern, hours_string)

    # Convert matches into dictionary (Fix: Unpack only day & time_range)
    hours_dict = {day: time_range for day, time_range in matches}

    return hours_dict


def resize_google_image_url(image_url: str, scale_factor: int = 7):
    """
    Modifies a Google image URL to change the width (w) and height (h) by a given scale factor.

    :param image_url: The original Google image URL.
    :param scale_factor: The factor by which to multiply the dimensions (default: 3x).
    :return: The modified URL with updated dimensions.
    """

    # Regular expression to find width (w) and height (h) parameters
    pattern = r"w(\d+)-h(\d+)"
    match = re.search(pattern, image_url)

    if match:
        # Extract current width and height
        width, height = int(match.group(1)), int(match.group(2))

        # Scale dimensions
        new_width, new_height = width * scale_factor, height * scale_factor

        # Replace in URL
        updated_url = re.sub(pattern, f"w{new_width}-h{new_height}", image_url)

        return updated_url

    return image_url  # Return original if no match found

# Example usage
#extract_business_data("G-Maps-Extractor-10-restaurants-2025-02-10.csv", "businesses.json")
extract_business_data("others_leyton.csv", "businesses.json")

""" "openingHours": {
                day: {
                    "endTime": firestore.SERVER_TIMESTAMP,
                    "isOpen": True,
                    "startTime": firestore.SERVER_TIMESTAMP
                } for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            }, """