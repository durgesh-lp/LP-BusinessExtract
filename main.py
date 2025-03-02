import pandas as pd
import json

def extract_business_data(file_path, output_json):
    """Extracts business details from CSV and saves to JSON."""
    # Load CSV file
    df = pd.read_csv(file_path)
    
    # Select required columns
    selected_columns = ["Name", "Description", "Fulladdress", "Phone", "Phones", "Featured Image", "Latitude", "Longitude"]
    
    # Ensure required columns exist
    missing_columns = set(selected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    df_selected = df[selected_columns]
    
    # Process 'Phones' column to extract the first phone number
    df_selected["Phones"] = df_selected["Phones"].astype(str).apply(lambda x: x.split(",")[0] if pd.notna(x) else "")
    
    # Convert to dictionary
    business_list = df_selected.to_dict(orient="records")
    
    # Save to JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(business_list, f, indent=4)
    
    print(f"Data successfully saved to {output_json}")

# Example usage
extract_business_data("G-Maps-Extractor-10-restaurants-2025-02-10.csv", "businesses.json")