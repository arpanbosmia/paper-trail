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
# Get path from the config file
DOWNLOADED_DATA_BASE_PATH = config.BILL_DATA_PATH 

# Define the range of Congresses you downloaded
START_CONGRESS = 108
END_CONGRESS = 119 # Current Congress

# Base names of the inner zip files to process
INNER_ZIP_BASENAMES = ['hr', 's', 'hjres', 'sjres']
BATCH_SIZE = 1000

def clear_bills_table(conn):
    """Deletes all rows from the Bills table and ensures Congress/subjects columns exist."""
    print("Clearing all old data from the 'Bills' table...")
    try:
        cur = conn.cursor()
        # Ensure Congress column exists
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='bills' AND column_name='congress') THEN
                    ALTER TABLE Bills ADD COLUMN Congress INT;
                    CREATE INDEX IF NOT EXISTS idx_bills_congress ON Bills (Congress);
                END IF;
            END $$;
        """)
        # Ensure subjects column exists (as a text array)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='bills' AND column_name='subjects') THEN
                    ALTER TABLE Bills ADD COLUMN subjects TEXT[];
                END IF;
            END $$;
        """)
        
        cur.execute("DELETE FROM Bills;")
        cur.execute("ALTER SEQUENCE Bills_BillID_seq RESTART WITH 1;")
        conn.commit()
        print("Table cleared successfully (and columns ensured).")
    except Exception as e:
        print(f"Error clearing table: {e}"); conn.rollback(); raise e

def parse_and_insert_enacted_laws_fast(base_path):
    """Finds zip files in subfolders, unzips them, parses XMLs, and batch inserts laws WITH SUBJECTS."""
    conn = None
    total_inserted_count = 0
    total_xml_files_processed = 0
    
    if not os.path.isdir(base_path):
        print(f"Error: Base bills folder not found at '{base_path}'.")
        print("Please ensure 'BILL_DATA_PATH' in config.py points to your 'bills' folder.")
        return

    try:
        print("Connecting to Supabase..."); conn = psycopg2.connect(DB_CONNECTION_STRING)
        clear_bills_table(conn); cur = conn.cursor()
        overall_start_time = time.time()
        print(f"Starting to process ZIP files from: {base_path}")

        # Loop through each expected Congress number
        for congress_num in range(START_CONGRESS, END_CONGRESS + 1):
            congress_start_time = time.time()
            congress_path = os.path.join(base_path, str(congress_num)) # Path to folder '108', '109', etc.
            print(f"\n--- Processing Congress {congress_num} ---")
            
            if not os.path.isdir(congress_path):
                print(f"Warning: Directory not found for Congress {congress_num} at '{congress_path}'. Skipping.")
                continue
                
            laws_for_this_congress = [] 

            # Loop through the expected inner zip basenames (hr, s, etc.)
            for basename in INNER_ZIP_BASENAMES:
                zip_filename = f"BILLSTATUS-{congress_num}-{basename}.zip"
                zip_filepath = os.path.join(congress_path, zip_filename) # Path to the actual zip file
                
                if not os.path.isfile(zip_filepath):
                    print(f"  Warning: ZIP file '{zip_filename}' not found. Skipping.")
                    continue
                
                print(f"  Processing ZIP file: {zip_filename}...")
                try:
                    # Open the zip file directly from its path
                    with zipfile.ZipFile(zip_filepath, 'r') as inner_zip_ref: 
                        for member_filename in inner_zip_ref.namelist():
                            if member_filename.endswith(".xml"):
                                total_xml_files_processed += 1
                                with inner_zip_ref.open(member_filename) as xml_file:
                                    try:
                                        xml_content_bytes = io.BytesIO(xml_file.read())
                                        tree = ET.parse(xml_content_bytes); root = tree.getroot()

                                        is_enacted = False
                                        # Check if <laws> tag exists
                                        if root.find('.//laws/item') is not None: is_enacted = True
                                        else:
                                            # Fallback: check latest action text
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
                                            
                                            # Extract subjects
                                            subjects_list = []
                                            policy_area_node = root.find('.//policyArea/name')
                                            if policy_area_node is not None and policy_area_node.text:
                                                subjects_list.append(policy_area_node.text.strip())

                                            bill_num_ins = f"{b_type}{b_num}"; date_intro = None
                                            if b_intro_date:
                                                try: date_intro = datetime.date.fromisoformat(b_intro_date)
                                                except: pass
                                                
                                            if bill_num_ins and bill_num_ins != 'NoneNone':
                                                # Add tuple with 5 values
                                                laws_for_this_congress.append((bill_num_ins, b_title, date_intro, congress_num, subjects_list))

                                    except (ET.ParseError, Exception) as file_err: 
                                        # print(f"Warning: Error parsing {member_filename}: {file_err}") # Uncomment for deep debug
                                        pass # Suppress individual file parse errors
                except (zipfile.BadZipFile, Exception) as zip_err:
                     print(f"  Error reading ZIP '{zip_filename}': {zip_err}. Skipping.")
            
            # Batch insert after processing all zips for this Congress
            if laws_for_this_congress:
                print(f"Found {len(laws_for_this_congress)} enacted laws for Congress {congress_num}. Batch inserting...")
                # SQL now includes the 'subjects' column
                sql = """
                    INSERT INTO Bills (BillNumber, Title, DateIntroduced, Congress, subjects)
                    VALUES %s
                    ON CONFLICT (BillNumber) DO NOTHING;
                """
                try:
                    execute_values(cur, sql, laws_for_this_congress, template=None, page_size=BATCH_SIZE)
                    conn.commit(); print(f"Batch insert successful.")
                    total_inserted_count += len(laws_for_this_congress)
                except psycopg2.Error as db_err:
                    print(f"  DB batch error: {db_err}. Rolling back."); conn.rollback(); cur = conn.cursor()
            else:
                print(f"Found 0 enacted laws for Congress {congress_num}.")
            
            print(f"--- Finished Congress {congress_num} in {time.time()-congress_start_time:.2f}s ---")

        overall_end_time = time.time()
        print(f"\n--- OVERALL SUCCESS ---")
        print(f"Processed {total_xml_files_processed} XML files from all ZIP archives.")
        cur.execute("SELECT COUNT(*) FROM Bills;"); final_count = cur.fetchone()[0]
        print(f"Successfully inserted {final_count} unique laws.")
        print(f"Total execution time: {overall_end_time - overall_start_time:.2f}s.")

    except psycopg2.OperationalError as db_conn_err: print(f"--- DB CONNECTION ERROR --- Error: {db_conn_err}")
    except Exception as e: print(f"An unexpected error occurred: {e}");
    finally:
        if conn:
            try: cur.close()
            except: pass
            conn.close(); print("DB connection closed.")

# Run the main function
if __name__ == "__main__":
    parse_and_insert_enacted_laws_fast(DOWNLOADED_DATA_BASE_PATH)

