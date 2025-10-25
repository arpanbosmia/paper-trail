import requests
import psycopg2
import time
from psycopg2.extras import execute_values
import re 
import sys
import os
import config

# --- CONFIGURATION ---
DB_CONNECTION_STRING = config.DB_CONNECTION_STRING
CONGRESS_GOV_API_KEY = config.CONGRESS_GOV_API_KEY

START_CONGRESS = 108 # Per Corey's update
END_CONGRESS = 119   # Current Congress
CURRENT_CONGRESS = 119 # Used to set IsActive flag
BATCH_SIZE = 1000
API_PAGE_DELAY = 0.3

# --- Global Lookups ---
politician_db_lookup = {}
global_unique_politicians = set()
    
def clear_politicians_table(conn):
    """Deletes all rows from the Politicians table and ensures Role column exists."""
    print("Clearing all data from the 'Politicians' table...")
    try:
        cur = conn.cursor()
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='politicians' AND column_name='role') THEN
                    ALTER TABLE Politicians ADD COLUMN Role TEXT;
                END IF;
            END $$;
        """)
        cur.execute("DELETE FROM Politicians;")
        cur.execute("ALTER SEQUENCE Politicians_PoliticianID_seq RESTART WITH 1;")
        conn.commit()
        print("Table cleared successfully (and Role column ensured).")
    except Exception as e:
        print(f"Error clearing table: {e}"); conn.rollback(); raise e

def fetch_members_generic(session, params):
    """Fetches members based on provided parameters, handling pagination correctly."""
    api_url_base = "https://api.congress.gov/v3/member"
    headers = {"X-Api-Key": CONGRESS_GOV_API_KEY}
    all_members = []
    limit = params.get("limit", 250)
    page_count = 0
    current_params = params.copy()
    fetch_url = api_url_base

    while True:
        page_count += 1
        print(f"  Fetching page {page_count}...", end='\r')
        request_params = None
        if "next_url" in current_params:
            fetch_url = current_params["next_url"]
        else:
            fetch_url = api_url_base
            request_params = current_params.copy()
            if "next_url" in request_params: del request_params["next_url"]

        try:
            response = session.get(fetch_url, headers=headers, params=request_params)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as req_err:
             print(f"\n  Network error page {page_count}: {req_err}. Retrying..."); time.sleep(10)
             try:
                 response = session.get(fetch_url, headers=headers, params=request_params)
                 response.raise_for_status(); data = response.json()
             except requests.exceptions.RequestException as req_err2:
                 print(f"\n  Retry failed page {page_count}: {req_err2}. Skipping fetch."); return []

        members_page = data.get('members', [])
        if not members_page:
             print(" " * 80, end='\r'); print(f"  Received empty list on page {page_count}. Assuming end.")
             break
        all_members.extend(members_page)

        pagination = data.get('pagination', {}); next_url = pagination.get('next')
        if not next_url:
             print(" " * 80, end='\r'); print(f"  No 'next' link. Finished fetching."); break
        current_params = {"next_url": next_url}
        time.sleep(API_PAGE_DELAY)

    print(" " * 80, end='\r')
    print(f"Finished fetching. Total members found: {len(all_members)}")
    return all_members


def update_active_status(conn, cur, current_members_keys):
    """Updates IsActive=True using a temporary table for matching."""
    print(f"\nUpdating IsActive status for {len(current_members_keys)} identified current politicians...")
    if not current_members_keys: print("No current members identified."); return 0
    temp_table_name = "active_politician_keys"
    keys_list = list(current_members_keys)
    try:
        print(f"Creating temp table '{temp_table_name}'...");
        cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
        cur.execute(f"""
            CREATE TEMPORARY TABLE {temp_table_name} (fname TEXT, lname TEXT, state TEXT, PRIMARY KEY (fname, lname, state))
            ON COMMIT DROP;
        """)
        print(f"Inserting {len(keys_list)} keys into temp table...");
        insert_sql = f"INSERT INTO {temp_table_name} (fname, lname, state) VALUES %s;"
        execute_values(cur, insert_sql, keys_list, template=None, page_size=BATCH_SIZE)
        print("Performing UPDATE...");
        update_sql = f"""
            UPDATE Politicians p SET IsActive = TRUE FROM {temp_table_name} temp
            WHERE p.FirstName = temp.fname AND p.LastName = temp.lname AND p.State = temp.state;
        """
        cur.execute(update_sql); updated_count = cur.rowcount
        conn.commit(); print(f"Successfully updated IsActive for {updated_count} politicians.")
        return updated_count
    except psycopg2.Error as db_err:
        print(f"  DB error during IsActive update: {db_err}"); conn.rollback(); return 0

def insert_politicians_final_active():
    """Final version: Inserts all as inactive, then updates active based on currentMember filter."""
    conn = None; total_processed_api_records = 0; session = requests.Session()
    try:
        print("Connecting..."); conn = psycopg2.connect(DB_CONNECTION_STRING)
        clear_politicians_table(conn); cur = conn.cursor(); start_time = time.time()
        
        # --- Stage 1: Insert ALL unique politicians as IsActive = False ---
        print("\n--- Stage 1: Inserting all historical politicians as INACTIVE ---")
        for congress_num in range(START_CONGRESS, END_CONGRESS + 1):
            congress_start_time = time.time(); print(f"\n--- Processing Congress {congress_num} ---")
            params = {"congress": congress_num, "limit": 250, "format": "json"}
            members_list = fetch_members_generic(session, params)
            if not members_list: print(f"Skipping Congress {congress_num}."); continue

            politicians_this_congress = {}; processed_in_congress = 0
            print(f"Parsing and de-duplicating {len(members_list)} members...")
            for member in members_list:
                processed_in_congress += 1; total_processed_api_records += 1
                db_chamber_name = None; district = None; role = None
                try: 
                    latest_term = member.get('terms', {}).get('item', [{}])[-1]
                    latest_term_chamber = latest_term.get('chamber')
                    if latest_term_chamber == 'House of Representatives': db_chamber_name = 'House'; role = 'Representative'
                    elif latest_term_chamber == 'Senate': db_chamber_name = 'Senate'; role = 'Senator'
                    else: continue
                except (IndexError, TypeError, AttributeError): continue
                if db_chamber_name == 'House':
                    district_str = member.get('District')
                    if district_str is None: district_str = latest_term.get('district', '0')
                    try: district = int(district_str) if str(district_str).isdigit() else None
                    except (ValueError, TypeError): district = None
                full_name = member.get('name', '')
                first_name = ""; last_name = ""
                if ',' in full_name: parts = full_name.split(',', 1); last_name = parts[0].strip(); first_name = parts[1].strip()
                else: last_name = full_name.strip()
                party = member.get('partyName'); state = member.get('state')
                if not first_name and not last_name: continue
                if not state: continue
                unique_key = (first_name, last_name, state)
                politicians_this_congress[unique_key] = (first_name, last_name, party, db_chamber_name, state, district, False, role) # IsActive = False
                global_unique_politicians.add(unique_key)

            politicians_to_batch = list(politicians_this_congress.values())
            print(f"Found {len(politicians_to_batch)} unique for Congress {congress_num}. Batch inserting (as inactive)...")
            if politicians_to_batch:
                sql_insert = """
                    INSERT INTO Politicians (FirstName, LastName, Party, Chamber, State, District, IsActive, Role)
                    VALUES %s
                    ON CONFLICT (FirstName, LastName, State) DO NOTHING;
                """
                try:
                    execute_values(cur, sql_insert, politicians_to_batch, template=None, page_size=BATCH_SIZE)
                    conn.commit(); print(f"Batch insert successful.")
                except psycopg2.Error as db_err:
                     print(f"  DB batch error (Stage 1): {db_err}. Rolling back."); conn.rollback(); cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM Politicians;"); current_total_rows = cur.fetchone()[0]
            print(f"  Finished Congress {congress_num} in {time.time() - congress_start_time:.2f}s. Total unique in table: {current_total_rows}")

        # --- Stage 1b: Manually add Presidents (since 108th Congress) ---
        print("\n--- Stage 1b: Inserting Presidents ---")
        presidents = [
            ('George W.', 'Bush', 'Republican', 'TX', False, 'President'),
            ('Barack', 'Obama', 'Democrat', 'IL', False, 'President'),
            ('Donald', 'Trump', 'Republican', 'FL', False, 'President'), # 45th term
            ('Joe', 'Biden', 'Democrat', 'DE', False, 'President'), # 46th term
            # 47th term for Trump will be handled by the update stage
        ]
        presidents_to_insert = []
        for pres in presidents:
            fname, lname, party, state, is_active, role = pres
            unique_key_tuple = (fname.lower(), lname.lower(), state.lower())
            # Check against global set to avoid duplicates
            if unique_key_tuple not in global_unique_politicians:
                 presidents_to_insert.append((fname, lname, party, 'Executive', state, None, is_active, role))
                 global_unique_politicians.add(unique_key_tuple)

        if presidents_to_insert:
             print(f"Batch inserting {len(presidents_to_insert)} new presidents...")
             sql_pres_insert = """
                 INSERT INTO Politicians (FirstName, LastName, Party, Chamber, State, District, IsActive, Role)
                 VALUES %s
                 ON CONFLICT (FirstName, LastName, State) DO NOTHING;
             """
             try:
                 execute_values(cur, sql_pres_insert, presidents_to_insert, template=None, page_size=BATCH_SIZE)
                 conn.commit(); print("President insert successful.")
             except psycopg2.Error as db_err:
                  print(f"  DB batch error (Presidents): {db_err}. Rolling back."); conn.rollback(); cur = conn.cursor()

        # --- Stage 1c: Governors ---
        print("\n--- Stage 1c: Governors (Manual SQL) ---")
        print("NOTE: Run manual SQL in Supabase Editor to add historical governors.")
        
        # --- Stage 2: Fetch *CURRENT* members/officials and Update IsActive ---
        print("\n--- Stage 2: Identifying and updating ACTIVE politicians ---")
        print("Fetching currently serving federal members...")
        current_member_params = {"currentMember": "true", "limit": 250, "format": "json"}
        current_members_list = fetch_members_generic(session, current_member_params)
        
        current_officials_keys = set()
        if current_members_list:
            print(f"Parsing {len(current_members_list)} currently serving federal members...")
            for member in current_members_list:
                full_name = member.get('name', ''); first_name = ""; last_name = ""
                if ',' in full_name: parts = full_name.split(',', 1); last_name = parts[0].strip(); first_name = parts[1].strip()
                else: last_name = full_name.strip()
                state = member.get('state')
                if state and (first_name or last_name):
                    current_officials_keys.add((first_name.lower(), last_name.lower(), state.lower()))
        else: print("Warning: Failed to fetch currently serving federal members.")
        
        # --- Manually add Current President & Governors ---
        print("Adding manually specified active Presidents and Governors...")
        current_officials_keys.add(('donald', 'trump', 'fl')) # 47th President
        
        # This list must be manually updated as governors change
        current_governors = [ 
            ('kay', 'ivey', 'alabama'), ('mike', 'dunleavy', 'alaska'), ('lemanu peleti', 'mauga', 'american samoa'), 
            ('katie', 'hobbs', 'arizona'), ('sarah huckabee', 'sanders', 'arkansas'), ('gavin', 'newsom', 'california'),
            ('jared', 'polis', 'colorado'), ('ned', 'lamont', 'connecticut'), ('john', 'carney', 'delaware'), 
            ('ron', 'desantis', 'florida'), ('brian', 'kemp', 'georgia'), ('lou', 'leon guerrero', 'guam'),
            ('josh', 'green', 'hawaii'), ('brad', 'little', 'idaho'), ('j. b.', 'pritzker', 'illinois'),
            ('mike', 'braun', 'indiana'), ('kim', 'reynolds', 'iowa'), ('laura', 'kelly', 'kansas'),
            ('andy', 'beshear', 'kentucky'), ('jeff', 'landry', 'louisiana'), ('janet', 'mills', 'maine'),
            ('wes', 'moore', 'maryland'), ('maura', 'healey', 'massachusetts'), ('gretchen', 'whitmer', 'michigan'),
            ('tim', 'walz', 'minnesota'), ('tate', 'reeves', 'mississippi'), ('mike', 'kehoe', 'missouri'),
            ('greg', 'gianforte', 'montana'), ('jim', 'pillen', 'nebraska'), ('joe', 'lombardo', 'nevada'),
            ('kelly', 'ayotte', 'new hampshire'), ('phil', 'murphy', 'new jersey'), ('michelle', 'lujan grisham', 'new mexico'),
            ('kathy', 'hochul', 'new york'), ('josh', 'stein', 'north carolina'), ('kelly', 'armstrong', 'north dakota'),
            ('david', 'apatang', 'northern mariana islands'), ('mike', 'dewine', 'ohio'), ('kevin', 'stitt', 'oklahoma'),
            ('tina', 'kotek', 'oregon'), ('josh', 'shapiro', 'pennsylvania'), ('jenniffer', 'gonzález-colón', 'puerto rico'),
            ('daniel', 'mckee', 'rhode island'), ('henry', 'mcmaster', 'south carolina'), ('larry', 'rhoden', 'south dakota'),
            ('bill', 'lee', 'tennessee'), ('greg', 'abbott', 'texas'), ('spencer', 'cox', 'utah'),
            ('phil', 'scott', 'vermont'), ('albert', 'bryan', 'virgin islands'), ('glenn', 'youngkin', 'virginia'),
            ('bob', 'ferguson', 'washington'), ('patrick', 'morrisey', 'west virginia'), ('tony', 'evers', 'wisconsin'),
            ('mark', 'gordon', 'wyoming')
        ]
        # Normalize keys from manual list
        for gov_fname, gov_lname, gov_state in current_governors:
             current_officials_keys.add((gov_fname.lower(), gov_lname.lower(), gov_state.lower()))
        
        print(f"Identified {len(current_officials_keys)} unique currently serving officials (Congress + manual adds).")
        
        update_active_status(conn, cur, current_officials_keys)

        # --- Final Report ---
        end_time = time.time(); print(f"\n--- OVERALL SUCCESS ---")
        cur.execute("SELECT COUNT(*) FROM Politicians;"); final_db_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM Politicians WHERE IsActive = TRUE;"); final_active_count = cur.fetchone()[0]
        print(f"Finished processing Congresses {START_CONGRESS}-{END_CONGRESS} (plus Presidents).")
        print(f"Final total unique politicians in database: {final_db_count}")
        print(f"Final count of politicians marked as Active: {final_active_count}")
        print(f"Total execution time: {time.time() - start_time:.2f} seconds.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}"); import traceback; traceback.print_exc()
        if conn: conn.rollback()
    finally:
        session.close()
        if conn:
            try: cur.close()
            except: pass
            conn.close(); print("Database connection closed.")

if __name__ == "__main__":
    insert_politicians_final_active()

