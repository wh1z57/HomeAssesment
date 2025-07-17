import json
import mysql.connector
from mysql.connector import Error
import os

# --- Configuration ---
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "db_user",
    "password": "6equj5_db_user",
    "database": "home_db"
}

JSON_PATH = "full_data.json"  # change to full file path
BATCH_SIZE = 100  # you can tune this


# --- Load JSON data ---
def load_data(path):
    with open(path, 'r') as f:
        return json.load(f)


# --- Connect to MySQL ---
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


# --- Insert batches using executemany ---
def insert_batch(cursor, query, batch, label="batch"):
    if not batch:
        return
    try:
        cursor.executemany(query, batch)
        print(f"✅ Inserted {len(batch)} rows into {label}")
    except Error as e:
        print(f"❌ Error inserting into {label}: {e}")


def main():
    data = load_data(JSON_PATH)
    conn = get_connection()
    cursor = conn.cursor()

    # SQL templates
    property_insert = """INSERT INTO properties (
        property_title, address, reviewed_status, most_recent_status, source, market,
        occupancy, flood, street_address, city, state, zip, property_type, highway,
        train, tax_rate, sqft_basement, htw, pool, commercial, water, sewage, year_built,
        sqft_mu, sqft_total, parking, bed, bath, basement_yes_no, layout, net_yield,
        irr, rent_restricted, neighborhood_rating, latitude, longitude, subdivision,
        taxes, selling_reason, seller_retained_broker, final_reviewer, school_average
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    valuation_insert = """INSERT INTO valuations (
        property_id, list_price, previous_rent, arv, expected_rent, zestimate,
        rent_zestimate, low_fmr, high_fmr, redfin_value
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    hoa_insert = """INSERT INTO hoa_fees (
        property_id, hoa, hoa_flag
    ) VALUES (%s, %s, %s)"""

    rehab_insert = """INSERT INTO rehab_estimates (
        property_id, underwriting_rehab, rehab_calculation, paint, flooring_flag,
        foundation_flag, roof_flag, hvac_flag, kitchen_flag, bathroom_flag,
        appliances_flag, windows_flag, landscaping_flag, trashout_flag
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    property_batch = []
    property_id_map = []  # tracks (temp_index, db_id) for assigning child rows

    for idx, prop in enumerate(data):
        values = (
            prop.get("Property_Title"),
            prop.get("Address"),
            prop.get("Reviewed_Status"),
            prop.get("Most_Recent_Status"),
            prop.get("Source"),
            prop.get("Market"),
            prop.get("Occupancy"),
            prop.get("Flood"),
            prop.get("Street_Address"),
            prop.get("City"),
            prop.get("State"),
            prop.get("Zip"),
            prop.get("Property_Type"),
            prop.get("Highway"),
            prop.get("Train"),
            prop.get("Tax_Rate"),
            prop.get("SQFT_Basement"),
            prop.get("HTW"),
            prop.get("Pool"),
            prop.get("Commercial"),
            prop.get("Water"),
            prop.get("Sewage"),
            prop.get("Year_Built"),
            prop.get("SQFT_MU"),
            prop.get("SQFT_Total"),
            prop.get("Parking"),
            prop.get("Bed"),
            prop.get("Bath"),
            prop.get("BasementYesNo"),
            prop.get("Layout"),
            prop.get("Net_Yield"),
            prop.get("IRR"),
            prop.get("Rent_Restricted"),
            prop.get("Neighborhood_Rating"),
            prop.get("Latitude"),
            prop.get("Longitude"),
            prop.get("Subdivision"),
            prop.get("Taxes"),
            prop.get("Selling_Reason"),
            prop.get("Seller_Retained_Broker"),
            prop.get("Final_Reviewer"),
            prop.get("School_Average")
        )
        property_batch.append(values)

        # Insert in batches
        if len(property_batch) == BATCH_SIZE or idx == len(data) - 1:
            cursor.executemany(property_insert, property_batch)
            conn.commit()
            start_id = cursor.lastrowid - len(property_batch) + 1
            for i, _ in enumerate(property_batch):
                property_id_map.append(start_id + i)
            print(f"🏠 Inserted {len(property_batch)} properties")
            property_batch = []

    # Second pass: Insert children
    valuation_batch, hoa_batch, rehab_batch = [], [], []

    for idx, prop in enumerate(data):
        prop_id = property_id_map[idx]

        for val in prop.get("Valuation", []):
            valuation_batch.append((
                prop_id,
                val.get("List_Price"),
                val.get("Previous_Rent"),
                val.get("ARV"),
                val.get("Expected_Rent"),
                val.get("Zestimate"),
                val.get("Rent_Zestimate"),
                val.get("Low_FMR"),
                val.get("High_FMR"),
                val.get("Redfin_Value")
            ))

        for hoa in prop.get("HOA", []):
            hoa_batch.append((
                prop_id,
                hoa.get("HOA"),
                hoa.get("HOA_Flag")
            ))

        for rehab in prop.get("Rehab", []):
            rehab_batch.append((
                prop_id,
                rehab.get("Underwriting_Rehab"),
                rehab.get("Rehab_Calculation"),
                rehab.get("Paint"),
                rehab.get("Flooring_Flag"),
                rehab.get("Foundation_Flag"),
                rehab.get("Roof_Flag"),
                rehab.get("HVAC_Flag"),
                rehab.get("Kitchen_Flag"),
                rehab.get("Bathroom_Flag"),
                rehab.get("Appliances_Flag"),
                rehab.get("Windows_Flag"),
                rehab.get("Landscaping_Flag"),
                rehab.get("Trashout_Flag")
            ))

    insert_batch(cursor, valuation_insert, valuation_batch, "valuations")
    insert_batch(cursor, hoa_insert, hoa_batch, "hoa_fees")
    insert_batch(cursor, rehab_insert, rehab_batch, "rehab_estimates")

    conn.commit()
    cursor.close()
    conn.close()
    print("🎉 ETL completed successfully for full dataset.")


if __name__ == "__main__":
    main()
