import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request
from flask_cors import CORS
import re # Import regex for precise filtering

# --- Add parent directory to path to find 'config' ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir) 

import config # Import your config file

# --- App Initialization ---
app = Flask(__name__)
CORS(app) # Enable Cross-Origin Resource Sharing

# --- Database Connection Helper ---
def get_db_connection():
    """Establishes and returns a new connection to the Supabase database."""
    conn = psycopg2.connect(config.DB_CONNECTION_STRING)
    return conn

# --- API Endpoints ---

@app.route('/')
def home():
    """A simple root route to confirm the API is running."""
    return "Paper Trail API is running."

@app.route('/api/politicians/search')
def search_politicians():
    """
    Searches for politicians by name or role.
    Example: /api/politicians/search?name=kemp
    """
    query_name = request.args.get('name', '') 
    if not query_name or len(query_name) < 2:
        return jsonify({"error": "A 'name' parameter with at least 2 characters is required."}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor) 
        search_query = f"%{query_name}%"
        
        cur.execute(
            """
            SELECT PoliticianID, FirstName, LastName, Party, State, Role, IsActive 
            FROM Politicians 
            WHERE LastName ILIKE %s 
               OR FirstName ILIKE %s
               OR Role ILIKE %s
            LIMIT 50; -- Add a limit to avoid huge responses
            """,
            (search_query, search_query, search_query)
        )
        
        politicians = cur.fetchall() 
        cur.close()
        return jsonify(politicians)
        
    except Exception as e:
        print(f"Database error in /api/politicians/search: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/politician/<int:politician_id>')
def get_politician_by_id(politician_id):
    """
    Gets details for a single politician by their ID.
    Example: /api/politician/2825
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT PoliticianID, FirstName, LastName, Party, State, Role, IsActive 
            FROM Politicians 
            WHERE PoliticianID = %s;
            """, (politician_id,)
        )
        politician = cur.fetchone() # Get the single result
        cur.close()
        if politician: return jsonify(politician)
        else: return jsonify({"error": "Politician not found"}), 404
    except Exception as e:
        print(f"Database error in /api/politician/<id>: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/politician/<int:politician_id>/votes')
def get_votes_by_politician(politician_id):
    """
    Gets the voting record for a single politician, with optional filtering and sorting.
    Example: /api/politician/530/votes?type=hr&sort=asc
    """
    # Get filter and sort parameters from the URL
    bill_type_filter = request.args.get('type', None)
    sort_order = request.args.get('sort', 'desc') # Default to descending (newest first)

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Base query
        query = """
            SELECT v.Vote, b.BillNumber, b.Title, b.Congress, b.DateIntroduced
            FROM Votes v
            JOIN Bills b ON v.BillID = b.BillID
            WHERE v.PoliticianID = %s
        """
        params = [politician_id]

        # --- UPDATED FILTER LOGIC ---
        # Dynamically add the bill type filter if provided
        if bill_type_filter and bill_type_filter in ['hr', 's', 'hjres', 'sjres']:
            # Use regex to match only the exact type (e.g., 's' not 'sjres')
            # ^ = start of string, [0-9]+ = one or more numbers, $ = end of string
            # i = case-insensitive (handled by ~*)
            filter_pattern = f"^{re.escape(bill_type_filter)}[0-9]+$"
            query += " AND b.BillNumber ~* %s" # ~* is case-insensitive regex
            params.append(filter_pattern)
        # --- END UPDATED LOGIC ---

        # Dynamically add the sorting order
        if sort_order.lower() == 'asc':
            query += " ORDER BY b.DateIntroduced ASC, b.BillNumber ASC;"
        else:
            query += " ORDER BY b.DateIntroduced DESC, b.BillNumber DESC;"
        
        cur.execute(query, tuple(params))
        
        votes = cur.fetchall()
        cur.close()
        
        return jsonify(votes)
        
    except Exception as e:
        print(f"Database error in /api/politician/<id>/votes: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/politician/<int:politician_id>/donations/summary')
def get_donations_summary_by_politician(politician_id):
    """Gets a summarized list of donations for a politician for a pie chart."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            WITH PoliticianDonations AS (
                SELECT d.DonorID, SUM(d.Amount) AS TotalAmount
                FROM Donations d WHERE d.PoliticianID = %s GROUP BY d.DonorID
            ), TotalReceived AS (
                SELECT SUM(TotalAmount) as GrandTotal FROM PoliticianDonations WHERE TotalAmount > 0
            )
            SELECT
                dn.Name AS DonorName, dn.DonorType, dn.Employer, dn.State AS DonorState,
                pd.TotalAmount, (pd.TotalAmount / NULLIF(tr.GrandTotal, 0)) * 100 AS Percentage
            FROM PoliticianDonations pd
            JOIN Donors dn ON pd.DonorID = dn.DonorID
            JOIN TotalReceived tr ON 1=1
            WHERE pd.TotalAmount > 0 ORDER BY pd.TotalAmount DESC;
            """, (politician_id,)
        )
        donations_summary = cur.fetchall()
        cur.close()
        return jsonify(donations_summary)
    except Exception as e:
        print(f"Database error in /api/politician/<id>/donations/summary: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/donors/search')
def search_donors():
    """Searches for donors (individuals or PACs) by name."""
    query_name = request.args.get('name', '')
    if not query_name or len(query_name) < 3:
        return jsonify({"error": "A 'name' parameter with at least 3 characters is required."}), 400
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        search_query = f"%{query_name}%"
        cur.execute(
            """
            SELECT DonorID, Name, DonorType, Employer, State
            FROM Donors
            WHERE Name ILIKE %s OR Employer ILIKE %s
            LIMIT 50;
            """, (search_query, search_query)
        )
        donors = cur.fetchall()
        cur.close()
        return jsonify(donors)
    except Exception as e:
        print(f"Database error in /api/donors/search: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

@app.route('/api/donor/<int:donor_id>/donations')
def get_donations_by_donor(donor_id):
    """Gets the full contribution history for a single donor."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT d.Amount, d.Date, p.PoliticianID, p.FirstName, p.LastName, p.Party, p.State, p.Role
            FROM Donations d
            JOIN Politicians p ON d.PoliticianID = p.PoliticianID
            WHERE d.DonorID = %s
            ORDER BY d.Date DESC;
            """, (donor_id,)
        )
        donations = cur.fetchall()
        cur.close()
        return jsonify(donations)
    except Exception as e:
        print(f"Database error in /api/donor/<id>/donations: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

# This makes the script runnable with 'py app.py'
if __name__ == '__main__':
    # debug=True automatically reloads the server when you save changes
    app.run(debug=True, host='0.0.0.0', port=5000)

