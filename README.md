# Project: Paper Trail

## Overview

Project: Paper Trail is a public database designed to track and correlate political campaign contributions with the voting records of elected officials. The goal is to provide a clear, easy-to-navigate tool that visualizes the flow of money in politics and its potential influence on legislation.

This repository contains the Python scripts used to populate the database.

## Database Schema

The project uses a PostgreSQL database (hosted on Supabase) with the following core tables:

* `Politicians`: Stores all Senators, Representatives, Governors, and Presidents since the 108th Congress.
* `Bills`: Stores all bills enacted into law since the 108th Congress.
* `Votes`: Links `Politicians` and `Bills`, recording how each member voted.
* `Donors`: Stores unique contributors (individuals, PACs, parties).
* `Donations`: Links `Donors` and `Politicians`, recording contribution details (amount > $2000, date).
* `fec_politician_map`: A helper table linking FEC candidate IDs to `Politician` table IDs.

## Data Sources

* **Politicians:** Congress.gov API (for legislators) & manual entry (for governors/presidents).
* **Bills:** GovInfo.gov (BILLSTATUS bulk data XMLs).
* **Votes:** Voteview.com (bulk roll call and member JSONs).
* **Donations:** FEC.gov (bulk data: candidate, committee, individual, and committee-to-candidate files).

## How to Run

1.  **Setup:**
    * Create a PostgreSQL database (e.g., on Supabase).
    * Run the `CREATE TABLE` SQL commands to set up the schema.
    * Create a `config.py` file with your `DB_CONNECTION_STRING` and API keys (this file is in the `.gitignore`).
2.  **Download Data:**
    * Download BILLSTATUS ZIP files (Congresses 108-119) into a `bills/` folder.
    * Download Voteview `HSall_members.json` and `HSXXX_rollcalls.json` / `HSXXX_votes.json` files (108-119) into a `votes/` folder.
    * Download FEC helper files (`cn.zip`, `cm.zip`, `ccl.zip`) into a `contributions/` folder.
3.  **Run Scripts (in order):**
    * `python populate_politicians.py` (Adds politicians)
    * `python populate_bills.py` (Adds enacted laws)
    * `python populate_votes.py` (Adds vote records)
    * `python build_fec_map.py` (Builds the FEC-to-Politician ID map)
    * `python populate_donors_and_donations.py` (Adds donors and donations)
