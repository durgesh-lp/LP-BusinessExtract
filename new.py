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

# Function to extract business details using Google Place ID
def get_business_details(place_id):
    driver = setup_driver()
    
    try:
        # Navigate to Google Maps business page
        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        driver.get(maps_url)
        time.sleep(5)  # Allow page to load

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Extract Business Name
        try:
            name = soup.find("h1", class_="DUwDvf").text.strip()
        except AttributeError:
            name = "N/A"

        # Extract Address
        try:
            address = soup.find("button", {"data-item-id": "address"}).text.strip()
        except AttributeError:
            address = "N/A"

        # Extract Phone Number
        try:
            phone = soup.find("button", {"data-tooltip": "Copy phone number"}).text.strip()
        except AttributeError:
            phone = "N/A"

        # Extract Website URL
        try:
            website = soup.find("a", {"data-item-id": "authority"}).get("href")
        except AttributeError:
            website = "N/A"

        driver.quit()

        # Print Extracted Data
        business_data = {
            "Name": name,
            "Address": address,
            "Phone": phone,
            "Website": website,
            "Place ID": place_id,
            "Email": extract_email_from_website(website) if website != "N/A" else "N/A"
        }
        return business_data

    except Exception as e:
        driver.quit()
        print(f"Error fetching business details: {e}")
        return None

# Function to extract email from business website
def extract_email_from_website(website_url):
    if not website_url.startswith("http"):
        website_url = "http://" + website_url  # Ensure valid URL format

    try:
        response = requests.get(website_url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all emails in the page
        email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        emails = re.findall(email_pattern, soup.text)

        return emails[0] if emails else "Email Not Found"

    except Exception as e:
        print(f"Error fetching email from website: {e}")
        return "Email Not Found"

# Example: Replace with your own Google Place ID
place_id = "ChIJ6agvSFUbdkgR3PO7Wiq5wyA"  # Example Place ID
business_info = get_business_details(place_id)

# Print extracted details
print(business_info)
