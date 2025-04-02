import pandas as pd
import json
import firebase_admin
from firebase_admin import messaging, credentials, firestore
import re
import datetime
import time 
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import asyncio

# Function to set up Selenium WebDriver
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

# Function to extract website from Google Place ID
def get_website_from_place_id(place_id):
    driver = setup_driver()
    
    try:
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        driver.get(maps_url)
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        try:
            website = soup.find("a", {"data-item-id": "authority"}).get("href")
        except AttributeError:
            website = None

        driver.quit()
        return website

    except Exception as e:
        driver.quit()
        print(f"Error fetching website for Place ID {place_id}: {e}")
        return None

# Function to extract email from website
def extract_emails_from_website(website_url):
    if not website_url:
        return None, None  # No website, so no email

    if not website_url.startswith("http"):
        website_url = "http://" + website_url

    try:
        response = requests.get(website_url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        emails = list(set(re.findall(email_pattern, soup.text)))  

        if emails:
            return emails[0], ", ".join(emails[1:]) if len(emails) > 1 else ""  
        else:
            return None, None

    except Exception as e:
        print(f"Error fetching email from website {website_url}: {e}")
        return None, None

def fetch_registered_vendors():
    names_list = []
    db = firestore.client()
    docs = db.collection("vendors").get()
    print("== Fetching data from Firebase ==")
    for doc in docs:
        data = doc.to_dict()
        if "name" in data:
            names_list.append(data["name"])
    return names_list

# Function to process and upload business data
async def extract_business_data(file_path):
    df = pd.read_csv(file_path)
    
    selected_columns = ["Name", "Description", "Categories", "Fulladdress", "Phone", "Phones", "Featured Image", "Latitude", "Longitude", "Street", "Website", "Place Id", "Opening Hours"]
    
    for col in selected_columns:
        if col not in df.columns:
            df[col] = ""
    
    df_selected = df[selected_columns].fillna("")
    
    df_selected["Phones"] = df_selected["Phones"].astype(str).apply(lambda x: x.split(",")[0].replace(" ", "") if pd.notna(x) else "")
    df_selected["Phone"] = df_selected["Phone"].astype(str).apply(lambda x: x.replace(" ", ""))
    
    # Add Email Column
    df_selected["Email"] = ""
    print("== Fetched data from CSV ==")

    # Initialize Firebase
    cred = credentials.Certificate("credentials_dev.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    registered_vendors = fetch_registered_vendors()
    print("== Fetched registered vendors == " + str(len(registered_vendors)))
    
    #postcode_pattern = r"E\d{2} \w{3}"
    postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})"

    for _, row in df_selected.iterrows():
        postcode_match = re.search(postcode_pattern, row["Fulladdress"])
        postcode = postcode_match.group() if postcode_match else None
        doc_ref = db.collection("vendors").document()
        doc_id = doc_ref.id
        openingHoursRow = convert_hours_string_to_dict(row["Opening Hours"])
        openingHours = parse_opening_hours(openingHoursRow)
        hdImageUrl = resize_google_image_url(row["Featured Image"])
        
        # Fetch Website and Extract Email
        website = row["Website"]
        if not website:
            website = get_website_from_place_id(row["Place Id"])
        
        email, additional_emails = extract_emails_from_website(website) if website else (None, None)
        print("== Fetched email:", email)
        
        # Assign email to DataFrame
        df_selected.at[_, "Email"] = email if email else ""

        business_data = {
            "active": True,
            "address": row["Fulladdress"],
            "category": row["Categories"],
            "city": "London",
            "contact": row["Phones"],
            "country": "United Kingdom",
            "description": row["Description"],
            "dynamicLink": "",
            "email": email if email else "",
            "additional_emails": additional_emails if additional_emails else "",
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

        isVendorExists = row["Name"] in registered_vendors
        if isVendorExists:
            print(f"Already exists: {row['Name']}")
        else:
            doc_ref.set(business_data)
            #send_shop_onboard_notification(row["Fulladdress"], row["Name"], doc_id)
            print(f"Vendor added: {row['Name']}")
            
    """ df_selected.to_csv(output_json, index=False)
    print(f"\n✅ Data successfully saved to {output_json}") """



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

# Example Usage
#extract_business_data("test.csv")
asyncio.run(extract_business_data("test.csv"))