# config.py
# --- SECRET KEYS AND CONFIGURATION ---
# This file is in .gitignore and WILL NOT be uploaded to GitHub.

import os

# Get the base directory of the project (where this config.py file is located)
# This makes all paths relative to the project folder, so it works on any computer.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- API KEYS & DATABASE ---
# Supabase database connection string
DB_CONNECTION_STRING = "postgresql://postgres:4K9RheFckSfEr5W5@db.mnaoamjxbjlsypdopnem.supabase.co:5432/postgres"

# API key for Congress.gov (for politicians)
CONGRESS_GOV_API_KEY = "AcW3yczMNabRoGS9OayTc0uy57E3x7X61sn3DbQK"

# API key for Senate Lobbying Disclosure Act (LDA) - (Not currently used by scripts)
# LDA_API_KEY = "YOUR_SENATE_LDA_KEY_HERE" 

# --- FOLDER & FILE PATHS ---
# Path to the folder containing cn.zip, cm.zip, ccl.zip, pas2.zip
FEC_DATA_FOLDER_PATH = os.path.join(BASE_DIR, "contributions")

# Path to the folder containing HSXXX_rollcalls.json and HSXXX_votes.json
VOTE_DATA_FOLDER_PATH = os.path.join(BASE_DIR, "votes")

# Path to the single Voteview member file
MEMBER_FILE_PATH = os.path.join(BASE_DIR, "HSall_members.json")

# Path to the main 'bills' zip file
# Assumes bills.zip is in the main 'paper-trail' folder
BILL_ZIP_FILE_PATH = os.path.join(BASE_DIR, "bills.zip")

