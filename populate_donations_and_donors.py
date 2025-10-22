import os
import zipfile
import io
import csv
import psycopg2
import time
import requests
from psycopg2.extras import execute_values
import config # Import config

# --- CONFIGURATION ---
DB_CONNECTION_STRING = config.DB_CONNECTION_STRING
FEC_DATA_FOLDER_PATH = config.FEC_DATA_FOLDER_PATH
BATCH_SIZE = 5000 

# --- Global Lookups ---
fec_id_to_politician_id_lookup = {} 
fec_committee_name_lookup = {}      
fec_cmte_to_cand_id_lookup = {}     
donor_db_lookup = {}                

# --- FEC Data File Headers ---
CM_HEADERS = ['CMTE_ID', 'CMTE_NM', 'CMTE_PTY_AFFILIATION', 'CMTE_TP']
CCL_HEADERS = ['CAND_ID', 'CAND_ELECTION_YR', 'FEC_ELECTION_YR', 'CMTE_ID', 'CMTE_TP', 'CMTE_DSGN', 'LINKAGE_ID']
PAS2_HEADERS = ['CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID', 'CAND_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID']
ITCONT_HEADERS = ['CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID']

# URLs for the individual contribution files
INDIV_FILE_URLS = [
    "https://www.fec.gov/files/bulk-downloads/2004/indiv04.zip",
    "https://www.fec.gov/files/bulk-downloads/2006/indiv06.zip",
    "https://www.fec.gov/files/bulk-downloads/2008/indiv08.zip",
    "https://www.fec.gov/files/bulk-downloads/2010/indiv10.zip",
    "https://www.fec.gov/files/bulk-downloads/2012/indiv12.zip",
    "https://www.fec.gov/files/bulk-downloads/2014/indiv14.zip",
    "https://www.fec.gov/files/bulk-downloads/2016/indiv16.zip",
    "https://www.fec.gov/files/bulk-downloads/2018/indiv18.zip",
    "https://www.fec.gov/files/bulk-downloads/2020/indiv20.zip",
    "https://www.fec.gov/files/bulk-downloads/2022/indiv22.zip",
    "https://www.fec.gov/files/bulk-downloads/2024/indiv24.zip",
    "https://www.fec.gov/files/bulk-downloads/2026/indiv26.zip"
]

# --- Helper Functions ---
def parse_fec_date(date_str):
    if not date_str or len(date_str) != 8: return None
    try: return f"{date_str[4:8]}-{date_str[0:2]}-{date_str[2:4]}"
    except: return None

# --- Database Functions ---
def clear_donation_tables(conn):
    print("Clearing 'Donations' and 'Donors' tables..."); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM Donations;"); cur.execute("DELETE FROM Donors;");
        cur.execute("ALTER SEQUENCE Donations_DonationID_seq RESTART WITH 1;");
        cur.execute("ALTER SEQUENCE Donors_DonorID_seq RESTART WITH 1;");
        conn.commit(); print("Tables cleared successfully.")
    except Exception as e: print(f"Error clearing tables: {e}"); conn.rollback(); raise e
    finally: cur.close()

def load_fec_lookups(conn, fec_folder_path):
    """Loads all FEC lookup maps: Politician Map (DB), Committees (file), and Committee-to-Candidate (file)."""
    global fec_id_to_politician_id_lookup, fec_committee_name_lookup, fec_cmte_to_cand_id_lookup
    cur = conn.cursor()
    
    # 1. Load the map we built
    print("Loading FEC Candidate Map from DB...");
    cur.execute("SELECT fec_candidate_id, politician_id FROM fec_politician_map")
    for row in cur.fetchall():
        fec_id_to_politician_id_lookup[row[0]] = row[1]
    print(f"Loaded {len(fec_id_to_politician_id_lookup)} FEC ID-to-PoliticianID mappings.")
    cur.close()

    # 2. Build Committee Name lookup from local cm.zip files
    print("Building FEC Committee lookup from local files...")
    cm_files = sorted([f for f in os.listdir(fec_folder_path) if f.startswith('cm') and f.endswith('.zip')])
    if not cm_files: print("Error: 'cm.zip' files not found."); raise FileNotFoundError
    for filename in cm_files:
        filepath = os.path.join(fec_folder_path, filename)
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                data_filename = [f for f in zf.namelist() if f.endswith('.txt')][0]
                with zf.open(data_filename, 'r') as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding='latin-1'), delimiter='|')
                    for row in reader:
                        try:
                            record = dict(zip(CM_HEADERS, row))
                            if record.get('CMTE_ID') and record.get('CMTE_NM'):
                                fec_committee_name_lookup[record['CMTE_ID']] = record['CMTE_NM'].strip()
                        except: continue
        except Exception as e: print(f"    Warning: Could not process {filename}: {e}")
    print(f"Loaded {len(fec_committee_name_lookup)} committee names.")

    # 3. Build Committee-to-Candidate lookup from local ccl.zip files
    print("Building FEC Committee-to-Candidate lookup from local files...")
    ccl_files = sorted([f for f in os.listdir(fec_folder_path) if f.startswith('ccl') and f.endswith('.zip')])
    if not ccl_files: print("Error: 'ccl.zip' files not found."); raise FileNotFoundError
    for filename in ccl_files:
        filepath = os.path.join(fec_folder_path, filename)
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                data_filename = [f for f in zf.namelist() if f.endswith('.txt')][0]
                with zf.open(data_filename, 'r') as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding='latin-1'), delimiter='|')
                    for row in reader:
                        try:
                            record = dict(zip(CCL_HEADERS, row))
                            cmte_id = record.get('CMTE_ID'); cand_id = record.get('CAND_ID')
                            if cmte_id and cand_id: fec_cmte_to_cand_id_lookup[cmte_id] = cand_id
                        except: continue
        except Exception as e: print(f"    Warning: Could not process {filename}: {e}")
    print(f"Loaded {len(fec_cmte_to_cand_id_lookup)} committee-to-candidate links.")


def update_donor_lookup(conn, cur, new_donor_keys):
    """Batch inserts new donors and updates the global donor_db_lookup cache."""
    global donor_db_lookup
    if not new_donor_keys: return
    donors_to_insert = [(key[0], key[1], key[2] if key[2] else None, key[3] if key[3] else None) for key in new_donor_keys]
    print(f"  Found {len(donors_to_insert)} new unique donors. Batch inserting...")
    sql_insert_donors = "INSERT INTO Donors (Name, DonorType, Employer, State) VALUES %s ON CONFLICT (Name, DonorType, Employer, State) DO NOTHING;"
    temp_table_name = "new_donor_keys_temp"
    try:
        execute_values(cur, sql_insert_donors, donors_to_insert, template=None, page_size=BATCH_SIZE)
        conn.commit() 
        print("  Refreshing donor cache (single join query)...")
        cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
        cur.execute(f"CREATE TEMPORARY TABLE {temp_table_name} (name TEXT, donortype TEXT, employer TEXT, state TEXT) ON COMMIT DROP;")
        if donors_to_insert:
            execute_values(cur, f"INSERT INTO {temp_table_name} (name, donortype, employer, state) VALUES %s", donors_to_insert, template=None, page_size=BATCH_SIZE)
        cur.execute(f"""
            SELECT d.DonorID, d.Name, d.DonorType, d.Employer, d.State FROM Donors d JOIN {temp_table_name} temp
            ON d.Name = temp.name AND d.DonorType = temp.donortype
            AND COALESCE(d.Employer, '') = COALESCE(temp.employer, '')
            AND COALESCE(d.State, '') = COALESCE(temp.state, '');
        """)
        rows = cur.fetchall()
        for row in rows:
            donor_id, name, donortype, employer, state = row
            key = (str(name or '').strip().lower(), str(donortype or '').strip().lower(), str(employer or '').strip().lower(), str(state or '').strip().lower())
            donor_db_lookup[key] = donor_id
        print(f"  Donor cache updated with {len(rows)} new/found IDs.")
        conn.commit()
    except psycopg2.Error as e:
        print(f"  DB error in update_donor_lookup: {e}. Rolling back."); conn.rollback(); cur = conn.cursor(); return

def process_pas2_files(conn, cur, fec_folder_path):
    """Processes all local pas2.zip files."""
    print(f"\n--- Stage 1: Processing local PAC-to-Candidate files (pas2) ---")
    pas2_files = sorted([f for f in os.listdir(fec_folder_path) if f.startswith('pas2') and f.endswith('.zip')])
    if not pas2_files: print("No local 'pas2XX.zip' files found."); return 0
    
    total_pas2_inserted = 0
    for filename in pas2_files:
        filepath = os.path.join(fec_folder_path, filename); print(f"Processing {filename}...")
        file_start_time = time.time(); file_donations_added = 0
        donations_to_process = []; new_donor_keys = set()
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                data_filename = [f for f in zf.namelist() if f.endswith('.txt')][0]
                with zf.open(data_filename, 'r') as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding='latin-1'), delimiter='|')
                    for i, row in enumerate(reader):
                        if (i+1) % 10000 == 0: print(f"  Processed {i+1} rows...", end='\r')
                        try:
                            record = dict(zip(PAS2_HEADERS, row))
                            amount = float(record.get('TRANSACTION_AMT', 0))
                            if amount <= 2000.0: continue
                            date = parse_fec_date(record.get('TRANSACTION_DT')); fec_cmte_id = record.get('CMTE_ID'); fec_cand_id = record.get('CAND_ID')
                            politician_id = fec_id_to_politician_id_lookup.get(fec_cand_id)
                            if not politician_id: continue
                            donor_name = fec_committee_name_lookup.get(fec_cmte_id, record.get('NAME', 'Unknown Committee')); donor_type = 'PAC/Party'
                            donor_key = (donor_name.lower(), donor_type.lower(), '', '') 
                            if politician_id and date:
                                donations_to_process.append((politician_id, amount, date, donor_type, donor_key))
                                if donor_key not in donor_db_lookup: new_donor_keys.add((donor_name, donor_type, None, None))
                        except: continue
        except Exception as e: print(f"  Error processing {filename}: {e}"); continue
        print(f"\n  Finished reading {filename}. Found {len(donations_to_process)} donations > $2000.")
        update_donor_lookup(conn, cur, new_donor_keys)
        donations_to_batch_insert = []
        for pol_id, amount, date, donor_type, donor_key in donations_to_process:
            donor_id = donor_db_lookup.get(donor_key) 
            if donor_id:
                donations_to_batch_insert.append((donor_id, pol_id, amount, date, donor_type)); file_donations_added += 1
        if donations_to_batch_insert:
            print(f"  Inserting {len(donations_to_batch_insert)} donation records...")
            execute_values(cur, "INSERT INTO Donations (DonorID, PoliticianID, Amount, Date, ContributionType) VALUES %s ON CONFLICT DO NOTHING;", donations_to_batch_insert)
            total_pas2_inserted += len(donations_to_batch_insert)
        conn.commit(); print(f"--- Finished {filename} in {time.time() - file_start_time:.2f}s. Added {file_donations_added} donations. ---")
    print(f"Stage 1 Complete. Inserted {total_pas2_inserted} PAC/Party donations.")
    return total_pas2_inserted

def process_indiv_files(conn, cur, fec_folder_path):
    """Downloads, processes, and deletes individual (itcont) zip files one by one."""
    print(f"\n--- Stage 2: Processing Individual Contribution files (itcont) ---")
    total_indiv_inserted = 0
    for url in INDIV_FILE_URLS:
        filename = url.split('/')[-1]; filepath = os.path.join(fec_folder_path, filename); file_start_time = time.time()
        print(f"\nDownloading {filename}...");
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192*10): f.write(chunk)
            print("Download complete.")
        except Exception as e: print(f"  Error downloading {filename}: {e}. Skipping."); continue
        print(f"Processing {filename}...")
        file_donations_added = 0; donations_to_process = []; new_donor_keys = set()
        try:
            with zipfile.ZipFile(filepath, 'r') as zf:
                data_filename = [f for f in zf.namelist() if f.endswith('.txt')][0]
                with zf.open(data_filename, 'r') as f:
                    reader = csv.reader(io.TextIOWrapper(f, encoding='latin-1'), delimiter='|')
                    for i, row in enumerate(reader):
                        if (i+1) % 50000 == 0: print(f"  Processed {i+1} rows...", end='\r')
                        try:
                            record = dict(zip(ITCONT_HEADERS, row))
                            amount = float(record.get('TRANSACTION_AMT', 0))
                            transaction_type = record.get('TRANSACTION_TP', '').upper()
                            # --- CORRECTED FILTER ---
                            if amount <= 2000.0 or not transaction_type.startswith('15'): continue
                            if record.get('OTHER_ID') and transaction_type not in ['15E', '15Z']: continue # Skip if not earmarked
                            # --- END CORRECTION ---
                            date = parse_fec_date(record.get('TRANSACTION_DT')); fec_cmte_id = record.get('CMTE_ID') 
                            donor_name, donor_employer, donor_state = record.get('NAME'), record.get('EMPLOYER'), record.get('STATE')
                            donor_type = 'Individual'
                            fec_cand_id = fec_cmte_to_cand_id_lookup.get(fec_cmte_id)
                            if not fec_cand_id: continue
                            politician_id = fec_id_to_politician_id_lookup.get(fec_cand_id)
                            if not politician_id: continue 
                            donor_key = (str(donor_name or '').strip().lower(), donor_type.lower(), str(donor_employer or '').strip().lower(), str(donor_state or '').strip().lower())
                            if politician_id and date and donor_name and donor_state:
                                donations_to_process.append((politician_id, amount, date, donor_type, donor_key))
                                if donor_key not in donor_db_lookup: new_donor_keys.add((donor_name, donor_type, donor_employer, donor_state))
                        except: continue
        except Exception as e: print(f"  Error processing {filename}: {e}")
        print(f"\n  Finished reading {filename}. Found {len(donations_to_process)} donations > $2000.")
        update_donor_lookup(conn, cur, new_donor_keys)
        donations_to_batch_insert = []
        for pol_id, amount, date, donor_type, donor_key in donations_to_process:
            donor_id = donor_db_lookup.get(donor_key) 
            if donor_id:
                donations_to_batch_insert.append((donor_id, pol_id, amount, date, donor_type)); file_donations_added += 1
        if donations_to_batch_insert:
            print(f"  Inserting {len(donations_to_batch_insert)} donation records...")
            execute_values(cur, "INSERT INTO Donations (DonorID, PoliticianID, Amount, Date, ContributionType) VALUES %s ON CONFLICT DO NOTHING;", donations_to_batch_insert)
            total_indiv_inserted += len(donations_to_batch_insert)
        conn.commit(); print(f"--- Finished {filename} in {time.time() - file_start_time:.2f}s. Added {file_donations_added} donations. ---")
        try: os.remove(filepath); print(f"Successfully deleted {filename}.")
        except Exception as e: print(f"  Warning: Could not delete {filename}: {e}")
    print(f"\nStage 2 Complete. Inserted {total_indiv_inserted} individual donations.")
    return total_indiv_inserted

# --- Main Execution ---
def main():
    conn = None
    try:
        print("Connecting to Supabase..."); conn = psycopg2.connect(DB_CONNECTION_STRING)
        load_fec_lookups(conn, FEC_DATA_FOLDER_PATH) # Build all maps first
        clear_donation_tables(conn); cur = conn.cursor()
        overall_start_time = time.time()
        pac_donations = process_pas2_files(conn, cur, FEC_DATA_FOLDER_PATH)
        indiv_donations = process_indiv_files(conn, cur, FEC_DATA_FOLDER_PATH)
        print(f"\n--- OVERALL SUCCESS ---")
        cur.execute("SELECT COUNT(*) FROM Donors;"); final_donor_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM Donations;"); final_donation_count = cur.fetchone()[0]
        print(f"Created {final_donor_count} unique donors.")
        print(f"Inserted {final_donation_count} total unique donations > $2000.")
        print(f"Total execution time: {time.time() - overall_start_time:.2f} seconds.")
    except Exception as e:
        print(f"\nAn unexpected error occurred in main: {e}"); import traceback; traceback.print_exc()
        if conn: conn.rollback()
    finally:
        if conn:
            try: cur.close()
            except: pass
            conn.close(); print("Database connection closed.")

if __name__ == "__main__":
    main()