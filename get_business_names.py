import firebase_admin
from firebase_admin import credentials, firestore
import json

# Initialize Firebase Admin SDK (Ensure this is only done once)
cred = credentials.Certificate("credentials_prod.json")  # Replace with actual path
firebase_admin.initialize_app(cred)

def save_names_from_firestore(collection_name: str, output_filename: str = "names.json"):
    """
    Fetches all documents from a Firestore collection, extracts the 'name' field, 
    and saves it as a JSON file.

    :param collection_name: The Firestore collection to fetch documents from.
    :param output_filename: The filename to save the extracted names as JSON.
    """
    # Initialize Firestore
    db = firestore.client()

    # Fetch all documents from the collection
    docs = db.collection(collection_name).stream()

    # Extract 'name' from each document
    names = [doc.to_dict().get("name") for doc in docs if "name" in doc.to_dict()]

    # Save the list to a JSON file
    with open(output_filename, "w") as json_file:
        json.dump(names, json_file, indent=4)

    print(f"Names successfully saved to {output_filename}")

# Example usage
save_names_from_firestore("vendors")
