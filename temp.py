import firebase_admin
from firebase_admin import credentials, firestore
import re

# Initialize Firebase Admin SDK (Ensure it's done once)
cred = credentials.Certificate("credentials_prod.json")  # Replace with your Firestore service account key
firebase_admin.initialize_app(cred)

# Firestore database reference
db = firestore.client()

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

def update_images_in_firestore():
    """
    Fetches all Firestore documents where `isextracted` is True, modifies image URLs,
    and updates Firestore with the new image URLs.
    """
    # Query Firestore for documents where `isextracted` is True
    docs = db.collection("vendors").where("extracted", "==", True).stream()
    

    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()

        if "images" in data and isinstance(data["images"], list):
            # Modify each image URL
            updated_images = [resize_google_image_url(url) for url in data["images"]]

            # Update Firestore document with new URLs
            db.collection("vendors").document(doc_id).update({"images": updated_images})
            print(f"Old document : {data["images"][0]}")
            print(f"Updated document : {updated_images[0]}")

# Run the update process
update_images_in_firestore()
