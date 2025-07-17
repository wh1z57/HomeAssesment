import json
import mysql.connector
from mysql.connector import Error

# --- Configuration ---
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "db_user",
    "password": "6equj5_db_user",
    "database": "home_db"
}

JSON_PATH = "fake_property_data.json"
LOG_FILE = "etl_validation_errors.log"
BATCH_SIZE = 500  # Batch for children inserts

# --- Validation Function ---
def validate_property(prop, idx):
    errors = []

    if not prop.get("Address"):
        errors.append("Missing Address")
    if not prop.get("City"):
        errors.append("Missing City")
    if not prop.get("State"):
        errors.append("Missing State")
    if not prop.get("Zip"):
        errors.append("Missing Zip")

    try:
        float(prop.get("Latitude", 0))
        float(prop.get("Longitude", 0))
    except (ValueError, TypeError):
        errors.append("Invalid coordinates")

    try:
        if prop.get("Tax_Rate") and not (0 <= float(prop["Tax_Rate"]) <= 100):
            errors.append("Invalid Tax_Rate")
    except (ValueError, TypeError):
        errors.append("Invalid Tax_Rate type")

    try:
        bed = int(prop.get("Bed", 0))
        if bed < 0:
            errors.append("Negative bed count")
    except (ValueError, TypeError):
        errors.append("Invalid bed count")

    if prop.get("Pool") not in ("Yes", "No", None):
        errors.append(f"Unexpected Pool value: {prop.get('Pool')}")

    return errors

# --- Load ---
def load_data(path):
    with open(path, 'r') as f:
        return json.load(f)

# --- Get DB connection ---
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

# --- Insert batch ---
def insert_batch(cursor, query, batch, label="batch"):
    if not batch:
        return
    try:
        cursor.executemany(query, batch)
        print(f"✅ Inserted {len(batch)} into {label}")
    except Error as e:
        print(f"❌ Error inserting into {label}: {e}")

# --- Main ETL ---
def main():
    data = load_data(JSON_PATH)
    conn = get_connection()
    cursor = conn.cursor()
    open(LOG_FILE, "w").close()

    # SQL Templates
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

    # Track inserted or existing properties
    property_id_map = {}
    skipped_rows = 0
    duplicate_skips = 0

    for idx, prop in enumerate(data):
        validation_errors = validate_property(prop, idx)
        if validation_errors:
            with open(LOG_FILE, "a") as log:
                log.write(f"Row {idx}: {validation_errors}\n")
            skipped_rows += 1
            continue

        addr, city, state = prop.get("Address"), prop.get("City"), prop.get("State")

        # Check for duplicate
        cursor.execute("""
            SELECT id FROM properties
            WHERE address = %s AND city = %s AND state = %s
        """, (addr, city, state))
        row = cursor.fetchone()

        if row:
            property_id_map[idx] = row[0]
            duplicate_skips += 1
            continue

        values = (
            prop.get("Property_Title"), addr, prop.get("Reviewed_Status"),
            prop.get("Most_Recent_Status"), prop.get("Source"), prop.get("Market"),
            prop.get("Occupancy"), prop.get("Flood"), prop.get("Street_Address"),
            city, state, prop.get("Zip"), prop.get("Property_Type"), prop.get("Highway"),
            prop.get("Train"), prop.get("Tax_Rate"), prop.get("SQFT_Basement"),
            prop.get("HTW"), prop.get("Pool"), prop.get("Commercial"), prop.get("Water"),
            prop.get("Sewage"), prop.get("Year_Built"), prop.get("SQFT_MU"),
            prop.get("SQFT_Total"), prop.get("Parking"), prop.get("Bed"), prop.get("Bath"),
            prop.get("BasementYesNo"), prop.get("Layout"), prop.get("Net_Yield"),
            prop.get("IRR"), prop.get("Rent_Restricted"), prop.get("Neighborhood_Rating"),
            prop.get("Latitude"), prop.get("Longitude"), prop.get("Subdivision"),
            prop.get("Taxes"), prop.get("Selling_Reason"), prop.get("Seller_Retained_Broker"),
            prop.get("Final_Reviewer"), prop.get("School_Average")
        )

        try:
            cursor.execute(property_insert, values)
            conn.commit()
            property_id_map[idx] = cursor.lastrowid
        except Error as e:
            print(f"❌ Property insert failed at row {idx}: {e}")
            skipped_rows += 1

    print(f"✅ Properties added: {len(property_id_map)} | Duplicates skipped: {duplicate_skips}")

    # --- Insert child records ---
    valuation_batch, hoa_batch, rehab_batch = [], [], []

    for idx, prop in enumerate(data):
        prop_id = property_id_map.get(idx)
        if not prop_id:
            continue

        for val in prop.get("Valuation", []):
            valuation_batch.append((
                prop_id, val.get("List_Price"), val.get("Previous_Rent"), val.get("ARV"),
                val.get("Expected_Rent"), val.get("Zestimate"), val.get("Rent_Zestimate"),
                val.get("Low_FMR"), val.get("High_FMR"), val.get("Redfin_Value")
            ))

        for hoa in prop.get("HOA", []):
            hoa_batch.append((prop_id, hoa.get("HOA"), hoa.get("HOA_Flag")))

        for rehab in prop.get("Rehab", []):
            rehab_batch.append((
                prop_id, rehab.get("Underwriting_Rehab"), rehab.get("Rehab_Calculation"),
                rehab.get("Paint"), rehab.get("Flooring_Flag"), rehab.get("Foundation_Flag"),
                rehab.get("Roof_Flag"), rehab.get("HVAC_Flag"), rehab.get("Kitchen_Flag"),
                rehab.get("Bathroom_Flag"), rehab.get("Appliances_Flag"), rehab.get("Windows_Flag"),
                rehab.get("Landscaping_Flag"), rehab.get("Trashout_Flag")
            ))

    insert_batch(cursor, valuation_insert, valuation_batch, "valuations")
    insert_batch(cursor, hoa_insert, hoa_batch, "hoa_fees")
    insert_batch(cursor, rehab_insert, rehab_batch, "rehab_estimates")

    conn.commit()
    cursor.close()
    conn.close()

    print("🎉 ETL completed successfully.")
    if skipped_rows > 0:
        print(f"⚠️ Skipped {skipped_rows} invalid rows.")
    if duplicate_skips > 0:
        print(f"🔁 Skipped {duplicate_skips} duplicates.")

if __name__ == "__main__":
    main()
