import json
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "db_user",
    "password": "6equj5_db_user",
    "database": "home_db"
}

JSON_PATH = "fake_property_data.json"
CHUNK_SIZE = 1000

def load_data(path):
    with open(path, 'r') as f:
        return json.load(f)

def get_connection():
    return mysql.connector.connect(**DB_CONFIG, autocommit=False, connection_timeout=30)

def preload_existing_properties(cursor):
    cursor.execute("SELECT address, city, state, id FROM properties")
    return {(row[0], row[1], row[2]): row[3] for row in cursor.fetchall()}

def preload_existing_child_rows(cursor, table, fields):
    cursor.execute(f"SELECT property_id, {', '.join(fields)} FROM {table}")
    return set(tuple(row) for row in cursor.fetchall())

def insert_in_chunks(cursor, conn, query, data, label="batch", chunk_size=CHUNK_SIZE):
    total = len(data)
    for i in range(0, total, chunk_size):
        chunk = data[i:i + chunk_size]
        try:
            cursor.executemany(query, chunk)
            conn.commit()
            print(f"Inserted {len(chunk)} into {label} ({i + 1}/{total})")
        except Error as e:
            print(f"Failed chunk insert into {label}: {e}")

def main():
    data = load_data(JSON_PATH)
    conn = get_connection()
    cursor = conn.cursor()

    existing_props = preload_existing_properties(cursor)
    prop_rows = []
    index_id_map = {}
    duplicate_count = 0

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

    for idx, prop in enumerate(data):
        key = (prop.get("Address"), prop.get("City"), prop.get("State"))
        if key in existing_props:
            index_id_map[idx] = existing_props[key]
            duplicate_count += 1
            continue

        row = (
            prop.get("Property_Title"), prop.get("Address"), prop.get("Reviewed_Status"),
            prop.get("Most_Recent_Status"), prop.get("Source"), prop.get("Market"),
            prop.get("Occupancy"), prop.get("Flood"), prop.get("Street_Address"),
            prop.get("City"), prop.get("State"), prop.get("Zip"), prop.get("Property_Type"),
            prop.get("Highway"), prop.get("Train"), prop.get("Tax_Rate"), prop.get("SQFT_Basement"),
            prop.get("HTW"), prop.get("Pool"), prop.get("Commercial"), prop.get("Water"),
            prop.get("Sewage"), prop.get("Year_Built"), prop.get("SQFT_MU"),
            prop.get("SQFT_Total"), prop.get("Parking"), prop.get("Bed"), prop.get("Bath"),
            prop.get("BasementYesNo"), prop.get("Layout"), prop.get("Net_Yield"),
            prop.get("IRR"), prop.get("Rent_Restricted"), prop.get("Neighborhood_Rating"),
            prop.get("Latitude"), prop.get("Longitude"), prop.get("Subdivision"),
            prop.get("Taxes"), prop.get("Selling_Reason"), prop.get("Seller_Retained_Broker"),
            prop.get("Final_Reviewer"), prop.get("School_Average")
        )
        prop_rows.append((idx, row))

    new_ids = []
    for i in range(0, len(prop_rows), CHUNK_SIZE):
        chunk = prop_rows[i:i + CHUNK_SIZE]
        chunk_data = [row for _, row in chunk]
        cursor.executemany(property_insert, chunk_data)
        conn.commit()
        first_id = cursor.lastrowid
        for j, (orig_idx, _) in enumerate(chunk):
            real_id = first_id + j
            index_id_map[orig_idx] = real_id

    print(f"Inserted new properties: {len(prop_rows)} | Skipped duplicates: {duplicate_count}")

    # Child insert statements
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

    existing_valuations = preload_existing_child_rows(cursor, "valuations", [
        "list_price", "previous_rent", "arv", "expected_rent", "zestimate",
        "rent_zestimate", "low_fmr", "high_fmr", "redfin_value"
    ])

    existing_hoas = preload_existing_child_rows(cursor, "hoa_fees", [
        "hoa", "hoa_flag"
    ])

    existing_rehabs = preload_existing_child_rows(cursor, "rehab_estimates", [
        "underwriting_rehab", "rehab_calculation", "paint", "flooring_flag",
        "foundation_flag", "roof_flag", "hvac_flag", "kitchen_flag", "bathroom_flag",
        "appliances_flag", "windows_flag", "landscaping_flag", "trashout_flag"
    ])

    valuation_rows, hoa_rows, rehab_rows = [], [], []

    for idx, prop in enumerate(data):
        prop_id = index_id_map.get(idx)
        if not prop_id:
            continue

        for val in prop.get("Valuation", []):
            row = (
                prop_id, val.get("List_Price"), val.get("Previous_Rent"), val.get("ARV"),
                val.get("Expected_Rent"), val.get("Zestimate"), val.get("Rent_Zestimate"),
                val.get("Low_FMR"), val.get("High_FMR"), val.get("Redfin_Value")
            )
            if row not in existing_valuations:
                valuation_rows.append(row)

        for hoa in prop.get("HOA", []):
            row = (prop_id, hoa.get("HOA"), hoa.get("HOA_Flag"))
            if row not in existing_hoas:
                hoa_rows.append(row)

        for rehab in prop.get("Rehab", []):
            row = (
                prop_id, rehab.get("Underwriting_Rehab"), rehab.get("Rehab_Calculation"),
                rehab.get("Paint"), rehab.get("Flooring_Flag"), rehab.get("Foundation_Flag"),
                rehab.get("Roof_Flag"), rehab.get("HVAC_Flag"), rehab.get("Kitchen_Flag"),
                rehab.get("Bathroom_Flag"), rehab.get("Appliances_Flag"), rehab.get("Windows_Flag"),
                rehab.get("Landscaping_Flag"), rehab.get("Trashout_Flag")
            )
            if row not in existing_rehabs:
                rehab_rows.append(row)

    insert_in_chunks(cursor, conn, valuation_insert, valuation_rows, "valuations")
    insert_in_chunks(cursor, conn, hoa_insert, hoa_rows, "hoa_fees")
    insert_in_chunks(cursor, conn, rehab_insert, rehab_rows, "rehab_estimates")

    cursor.close()
    conn.close()
    print("ETL completed successfully.")

if __name__ == "__main__":
    main()
