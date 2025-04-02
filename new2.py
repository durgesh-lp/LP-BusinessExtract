import pandas as pd
import re
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

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
        # Open Google Maps business page
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        driver.get(maps_url)
        time.sleep(5)  # Allow page to load

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Extract Website URL
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

# Function to extract emails from website
def extract_emails_from_website(website_url):
    if not website_url:
        return None, None  # No website, so no email

    if not website_url.startswith("http"):
        website_url = "http://" + website_url  # Ensure valid URL format

    try:
        response = requests.get(website_url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all emails in the page
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        emails = list(set(re.findall(email_pattern, soup.text)))  # Remove duplicates

        if emails:
            return emails[0], ", ".join(emails[1:]) if len(emails) > 1 else ""  # First email & additional emails
        else:
            return None, None

    except Exception as e:
        print(f"Error fetching email from website {website_url}: {e}")
        return None, None

# Function to process CSV file
def process_csv(input_csv, output_csv):
    # Load the CSV
    df = pd.read_csv(input_csv)

    # Ensure 'Place Id' column exists
    if 'Place Id' not in df.columns:
        print("Error: CSV file must contain a 'Place Id' column")
        return

    # Create new columns 'Email' & 'Additional Emails'
    df['Email'] = ""
    df['Additional Emails'] = ""

    # Iterate through each row
    for index, row in df.iterrows():
        place_id = row['Place Id']
        
        print(f"Processing Place ID: {place_id}")

        # Get website from Place ID
        website = get_website_from_place_id(place_id)

        if website:
            print(f"  ↳ Website Found: {website}")
            email, additional_emails = extract_emails_from_website(website)

            if email:
                df.at[index, 'Email'] = email
                df.at[index, 'Additional Emails'] = additional_emails
                print(f"  ✅ Email Found: {email}")
                if additional_emails:
                    print(f"  ✅ Additional Emails: {additional_emails}")
            else:
                print("  ❌ No Email Found")
        else:
            print("  ❌ No Website Found")

    # Save updated CSV with all original columns + new email columns
    df.to_csv(output_csv, index=False)
    print(f"\n✅ Process Completed! Emails saved to: {output_csv}")

# Example usage
input_csv = "G-Maps-Extractor-10-restaurants-2025-02-10.csv"   # Your input CSV file
output_csv = "businesses_with_emails.csv"  # Output file with extracted emails
process_csv(input_csv, output_csv)
