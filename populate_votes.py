import os
import json
import psycopg2
import time
from psycopg2.extras import execute_values
import config # Import config

# --- CONFIGURATION ---
DB_CONNECTION_STRING = config.DB_CONNECTION_STRING
VOTE_DATA_FOLDER_PATH = config.VOTE_DATA_FOLDER_PATH
MEMBER_FILE_PATH = config.MEMBER_FILE_PATH
BATCH_SIZE = 5000 

# --- Global Lookups ---
politician_db_lookup = {} 
bill_db_lookup = {}       
icpsr_lookup = {}         
rollcall_lookup = {}      

# Voteview cast_code mapping
VOTEVIEW_CODE_MAP = {
    1: 'Yea', 2: 'Yea', 3: 'Yea',
    4: 'Nay', 5: 'Nay', 6: 'Nay',
    7: 'Not Voting', 8: 'Not Voting', 9: 'Not Voting', 0: 'Not Voting'
}

def clear_votes_table(conn):
    print("Clearing 'Votes' table..."); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM Votes;");
        cur.execute("ALTER SEQUENCE Votes_VoteID_seq RESTART WITH 1;");
        conn.commit(); print("Table cleared.")
    except Exception as e: print(f"Error clearing: {e}"); conn.rollback(); raise e

def load_db_lookups(conn):
    """Loads Politicians and Bills from Supabase."""
    global politician_db_lookup, bill_db_lookup
    cur = conn.cursor()
    print("Loading Politicians lookup from DB...");
    cur.execute("SELECT PoliticianID, FirstName, LastName, State FROM Politicians")
    for row in cur.fetchall():
        pid, fname, lname, state = row
        key = (str(fname or '').strip().lower(), str(lname or '').strip().lower(), str(state or '').strip().lower())
        politician_db_lookup[key] = pid
    print(f"Loaded {len(politician_db_lookup)} politicians.")
    
    print("Loading Bills lookup from DB...");
    cur.execute("SELECT BillID, BillNumber FROM Bills WHERE Congress >= 108");
    for row in cur.fetchall():
        bid, bnumber = row
        key = str(bnumber or '').strip().lower().replace(" ", "").replace(".", "")
        bill_db_lookup[key] = bid
    print(f"Loaded {len(bill_db_lookup)} enacted bills."); cur.close()

def load_icpsr_lookup(member_filepath):
    """Loads the Voteview member file (HSall_members.json)."""
    global icpsr_lookup
    print(f"Loading ICPSR mapping from {member_filepath}...")
    try:
        with open(member_filepath, 'r', encoding='utf-8') as f: member_data = json.load(f)
        for member in member_data:
            icpsr, state, bioname = member.get('icpsr'), member.get('state_abbrev', '').strip().upper(), member.get('bioname', '')
            fname, lname = "", ""
            if ',' in bioname: parts = bioname.split(',', 1); lname = parts[0].strip().lower(); fname = parts[1].strip().lower()
            elif member.get('name'): lname = member.get('name').strip().lower()
            if icpsr and state and (fname or lname):
                icpsr_lookup[icpsr] = (fname, lname, state.lower())
        print(f"Loaded {len(icpsr_lookup)} ICPSR-to-Name mappings.")
    except FileNotFoundError: print(f"Error: Member file not found at '{member_filepath}'"); raise
    except Exception as e: print(f"Error reading member file: {e}"); raise

def load_rollcall_lookup(vote_folder_path):
    """Loads all _rollcalls.json files to map (congress, rollnumber) to BillID."""
    global rollcall_lookup
    print("Loading roll call to bill lookup...")
    rollcall_files = sorted([f for f in os.listdir(vote_folder_path) if f.startswith('HS') and f.endswith('_rollcalls.json')])
    if not rollcall_files: print(f"Error: No '*_rollcalls.json' files found in '{vote_folder_path}'"); raise FileNotFoundError
    
    for filename in rollcall_files:
        filepath = os.path.join(vote_folder_path, filename)
        print(f"  Reading {filename}...")
        try:
            with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
            for roll_call in data:
                bill_number = roll_call.get('bill_number')
                bill_key = str(bill_number or '').strip().lower().replace(" ", "").replace(".", "")
                bill_id = bill_db_lookup.get(bill_key)
                if bill_id:
                    key = (roll_call.get('congress'), roll_call.get('rollnumber'), roll_call.get('chamber'))
                    rollcall_lookup[key] = bill_id
        except Exception as e: print(f"    Warning: Error reading {filename}: {e}. Skipping file.")
    print(f"Loaded {len(rollcall_lookup)} roll calls linked to enacted bills.")

def find_politician_id(icpsr):
    """Matches Voteview icpsr to our politician_db_lookup."""
    name_state_key = icpsr_lookup.get(icpsr)
    if not name_state_key: return None
    return politician_db_lookup.get(name_state_key)

def process_and_insert_votes():
    """Reads _votes.json files, uses lookups, and batch inserts votes."""
    conn = None; total_inserted_votes = 0; total_votes_processed = 0
    try:
        print("Connecting to Supabase..."); conn = psycopg2.connect(DB_CONNECTION_STRING)
        load_db_lookups(conn)
        load_icpsr_lookup(MEMBER_FILE_PATH)
        load_rollcall_lookup(VOTE_DATA_FOLDER_PATH)
        clear_votes_table(conn); cur = conn.cursor()
        overall_start_time = time.time()

        vote_files = sorted([f for f in os.listdir(VOTE_DATA_FOLDER_PATH) if f.startswith('HS') and f.endswith('_votes.json')])
        if not vote_files: 
            print(f"Error: No '*_votes.json' files found in '{VOTE_DATA_FOLDER_PATH}'"); return
            
        print(f"Found {len(vote_files)} Voteview *votes* JSON files to process.")
        votes_to_batch_insert = []

        for filename in vote_files:
            filepath = os.path.join(VOTE_DATA_FOLDER_PATH, filename)
            print(f"\n--- Processing File: {filename} ---")
            file_start_time = time.time(); file_votes_matched = 0
            try:
                with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
            except Exception as e: print(f"Error reading file {filename}: {e}. Skipping."); continue
            if not isinstance(data, list): print(f"Warning: Expected list in {filename}. Skipping."); continue

            print(f"Processing {len(data)} individual vote records...")
            for i, vote_record in enumerate(data):
                total_votes_processed += 1
                if (i + 1) % 50000 == 0: print(f"  Processed {i+1}/{len(data)} records...", end='\r')
                try:
                    congress = vote_record.get('congress'); rollnumber = vote_record.get('rollnumber')
                    chamber = vote_record.get('chamber'); icpsr = vote_record.get('icpsr')
                    cast_code = vote_record.get('cast_code')

                    rollcall_key = (congress, rollnumber, chamber)
                    bill_id = rollcall_lookup.get(rollcall_key)
                    if not bill_id: continue
                    
                    politician_id = find_politician_id(icpsr)
                    vote_string = VOTEVIEW_CODE_MAP.get(cast_code)
                    
                    if politician_id and bill_id and vote_string:
                        votes_to_batch_insert.append((politician_id, bill_id, vote_string))
                        file_votes_matched += 1
                except: continue # Skip bad/malformed rows

                if len(votes_to_batch_insert) >= BATCH_SIZE:
                    print(" " * 80, end='\r'); print(f"  Inserting batch of {len(votes_to_batch_insert)} votes...")
                    sql_insert = "INSERT INTO Votes (PoliticianID, BillID, Vote) VALUES %s ON CONFLICT DO NOTHING;"
                    try:
                        execute_values(cur, sql_insert, votes_to_batch_insert, page_size=BATCH_SIZE)
                        conn.commit(); total_inserted_votes += len(votes_to_batch_insert)
                        votes_to_batch_insert = []
                    except psycopg2.Error as db_err:
                        print(f"\n  DB batch error: {db_err}. Rolling back."); conn.rollback(); cur = conn.cursor()
                        votes_to_batch_insert = []
            
            if votes_to_batch_insert:
                print(" " * 80, end='\r'); print(f"  Inserting final batch of {len(votes_to_batch_insert)} votes...")
                sql_insert = "INSERT INTO Votes (PoliticianID, BillID, Vote) VALUES %s ON CONFLICT DO NOTHING;"
                try:
                    execute_values(cur, sql_insert, votes_to_batch_insert, page_size=BATCH_SIZE)
                    conn.commit(); total_inserted_votes += len(votes_to_batch_insert)
                except psycopg2.Error as db_err:
                    print(f"\n  DB final batch error: {db_err}. Rolling back."); conn.rollback(); cur = conn.cursor()

            print(" " * 80, end='\r')
            print(f"  Matched {file_votes_matched} votes in this file.")
            print(f"--- Finished file {filename} in {time.time() - file_start_time:.2f}s ---")

        print(f"\n--- OVERALL SUCCESS ---")
        print(f"Processed {total_votes_processed} individual vote records.")
        cur.execute("SELECT COUNT(*) FROM Votes;"); final_count = cur.fetchone()[0]
        print(f"Successfully inserted {final_count} vote records linked to enacted laws.")
        print(f"Total execution time: {time.time() - overall_start_time:.2f} seconds.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}"); import traceback; traceback.print_exc()
        if conn: conn.rollback()
    finally:
        if conn:
            try: cur.close()
            except: pass
            conn.close(); print("Database connection closed.")

if __name__ == "__main__":
    process_and_insert_votes()