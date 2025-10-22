import os
import zipfile
import io
import xml.etree.ElementTree as ET
import psycopg2
import datetime
import time
from psycopg2.extras import execute_values
import config # Import config

# --- CONFIGURATION ---
DB_CONNECTION_STRING = config.DB_CONNECTION_STRING
# Use paths from config file
BASE_PROJECT_PATH = os.path.dirname(os.path.abspath(__file__)) # Assumes script is in donationtracker folder
DOWNLOADED_DATA_BASE_PATH = config.BILL_DATA_PATH
MAIN_ZIP_FILENAME = config.MAIN_ZIP_FILENAME

# Define the range of Congresses you downloaded
START_CONGRESS = 108
END_CONGRESS = 119 # Current Congress

# Base names of the inner zip files to process
INNER_ZIP_BASENAMES = ['hr', 's', 'hjres', 'sjres']
BATCH_SIZE = 1000

def clear_bills_table(conn):
    """Deletes all rows from the Bills table and ensures Congress column exists."""
    print("Clearing all old data from the 'Bills' table...")
    try:
        cur = conn.cursor()
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='bills' AND column_name='congress') THEN
                    ALTER TABLE Bills ADD COLUMN Congress INT;
                    CREATE INDEX IF NOT EXISTS idx_bills_congress ON Bills (Congress);
                END IF;
            END $$;
        """)
        cur.execute("DELETE FROM Bills;")
        cur.execute("ALTER SEQUENCE Bills_BillID_seq RESTART WITH 1;")
        conn.commit()
        print("Table cleared successfully.")
    except Exception as e:
        print(f"Error clearing table: {e}"); conn.rollback(); raise e

def parse_and_insert_from_main_zip(project_path, main_zip_name):
    """Opens a main zip, finds inner zips, parses XMLs, and batch inserts laws."""
    conn = None
    total_inserted_count = 0
    total_xml_files_processed = 0
    
    main_zip_filepath = os.path.join(project_path, main_zip_name)
    
    if not os.path.isfile(main_zip_filepath):
        print(f"Error: Main ZIP file not found at '{main_zip_filepath}'.")
        print("Please ensure 'bills.zip' is in your 'donationtracker/bills' folder.")
        return

    try:
        print("Connecting to Supabase..."); conn = psycopg2.connect(DB_CONNECTION_STRING)
        clear_bills_table(conn); cur = conn.cursor()
        overall_start_time = time.time()
        print(f"Starting to process main ZIP file: {main_zip_filepath}")

        with zipfile.ZipFile(main_zip_filepath, 'r') as main_zip:
            for congress_num in range(START_CONGRESS, END_CONGRESS + 1):
                congress_start_time = time.time()
                print(f"\n--- Processing Congress {congress_num} ---")
                laws_for_this_congress = []
                for basename in INNER_ZIP_BASENAMES:
                    inner_zip_filename = f"BILLSTATUS-{congress_num}-{basename}.zip"
                    # Path inside archive: bills/108/BILLSTATUS-108-hr.zip
                    inner_zip_path_in_archive = f"bills/{congress_num}/{inner_zip_filename}" 
                    
                    print(f"  Looking for inner ZIP: {inner_zip_path_in_archive}...")

                    try:
                        inner_zip_content = main_zip.read(inner_zip_path_in_archive)
                        with zipfile.ZipFile(io.BytesIO(inner_zip_content), 'r') as inner_zip_ref:
                            print(f"    Processing XMLs within {inner_zip_filename}...")
                            for member_filename in inner_zip_ref.namelist():
                                if member_filename.endswith(".xml"):
                                    total_xml_files_processed += 1
                                    with inner_zip_ref.open(member_filename) as xml_file:
                                        try:
                                            xml_content_bytes = io.BytesIO(xml_file.read())
                                            tree = ET.parse(xml_content_bytes); root = tree.getroot()
                                            is_enacted = False
                                            if root.find('.//laws/item') is not None: is_enacted = True
                                            else:
                                                latest_action = root.find('.//latestAction/text')
                                                if latest_action is not None and latest_action.text:
                                                    action = latest_action.text.strip().lower()
                                                    if "became public law" in action or "became private law" in action: is_enacted = True
                                            if is_enacted:
                                                bill_node = root.find('.//bill');
                                                if bill_node is None: continue
                                                b_type = bill_node.findtext('type','').strip(); b_num = bill_node.findtext('number','').strip()
                                                b_title = bill_node.findtext('title','').strip()
                                                b_intro_date = bill_node.findtext('.//introducedDate','').strip()
                                                bill_num_ins = f"{b_type}{b_num}"; date_intro = None
                                                if b_intro_date:
                                                    try: date_intro = datetime.date.fromisoformat(b_intro_date)
                                                    except: pass
                                                if bill_num_ins and bill_num_ins != 'NoneNone':
                                                    laws_for_this_congress.append((bill_num_ins, b_title, date_intro, congress_num))
                                        except: pass # Suppress individual file errors
                    except KeyError:
                        print(f"  Warning: Inner ZIP path '{inner_zip_path_in_archive}' not found. Skipping.")
                    except (zipfile.BadZipFile, Exception) as inner_zip_err:
                        print(f"  Error reading inner ZIP '{inner_zip_path_in_archive}': {inner_zip_err}. Skipping.")

                if laws_for_this_congress:
                    print(f"Found {len(laws_for_this_congress)} enacted laws for Congress {congress_num}. Batch inserting...")
                    sql = """
                        INSERT INTO Bills (BillNumber, Title, DateIntroduced, Congress)
                        VALUES %s
                        ON CONFLICT (BillNumber) DO NOTHING;
                    """
                    try:
                        execute_values(cur, sql, laws_for_this_congress, template=None, page_size=BATCH_SIZE)
                        conn.commit(); print(f"Batch insert successful.")
                    except psycopg2.Error as db_err:
                        print(f"  DB batch error: {db_err}. Rolling back."); conn.rollback(); cur = conn.cursor()
                else:
                    print(f"Found 0 enacted laws for Congress {congress_num}.")
                
                print(f"--- Finished Congress {congress_num} in {time.time()-congress_start_time:.2f}s ---")

        overall_end_time = time.time()
        print(f"\n--- OVERALL SUCCESS ---")
        print(f"Processed {total_xml_files_processed} XML files from main ZIP archive.")
        cur.execute("SELECT COUNT(*) FROM Bills;"); final_count = cur.fetchone()[0]
        print(f"Successfully inserted {final_count} unique laws.")
        print(f"Total execution time: {overall_end_time - overall_start_time:.2f}s.")
    except zipfile.BadZipFile: print(f"Error: '{main_zip_filepath}' is not valid ZIP.")
    except psycopg2.OperationalError as db_conn_err: print(f"--- DB CONNECTION ERROR --- Error: {db_conn_err}")
    except Exception as e: print(f"An unexpected error occurred: {e}");
    finally:
        if conn:
            try: cur.close()
            except: pass
            conn.close(); print("DB connection closed.")

if __name__ == "__main__":
    parse_and_insert_from_main_zip(DOWNLOADED_DATA_BASE_PATH, MAIN_ZIP_FILENAME)