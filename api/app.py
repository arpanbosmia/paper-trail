import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request
from flask_cors import CORS
import math # Import math for calculating total pages

# --- Add parent directory to path to find 'config' ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import config # Import your config file with secret keys

# --- App Initialization ---
app = Flask(__name__)
CORS(app)

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
    """Searches for politicians by name or role."""
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
            WHERE LastName ILIKE %s OR FirstName ILIKE %s OR Role ILIKE %s
            LIMIT 50;
            """, (search_query, search_query, search_query)
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
    """Gets details for a single politician by their ID."""
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
        politician = cur.fetchone()
        cur.close()
        if politician: return jsonify(politician)
        else: return jsonify({"error": "Politician not found"}), 404
    except Exception as e:
        print(f"Database error in /api/politician/<id>: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()

# --- *** MODIFIED VOTES ENDPOINT WITH PAGINATION *** ---
@app.route('/api/politician/<int:politician_id>/votes')
def get_votes_by_politician(politician_id):
    """
    Gets the paginated voting record for a single politician.
    Example: /api/politician/530/votes?page=1&type=hr&sort=asc
    """
    # Get query parameters
    bill_type_filter = request.args.get('type', None)
    sort_order = request.args.get('sort', 'desc')
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
    if page < 1:
        page = 1
    
    VOTES_PER_PAGE = 50 # Number of votes to return per page
    offset = (page - 1) * VOTES_PER_PAGE

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # --- Build the Query ---
        # We need two queries: one for the total count, one for the page data.
        
        # 1. Build COUNT query to get total number of votes for pagination
        count_query = "SELECT COUNT(v.VoteID) FROM Votes v JOIN Bills b ON v.BillID = b.BillID WHERE v.PoliticianID = %s"
        count_params = [politician_id]
        
        if bill_type_filter and bill_type_filter in ['hr', 's', 'hjres', 'sjres']:
            count_query += " AND b.BillNumber ~* %s" # Use regex for exact start
            count_params.append(f"^{bill_type_filter}[0-9]") # e.g., ^hr[0-9]
        
        cur.execute(count_query, tuple(count_params))
        total_votes = cur.fetchone()['count']
        total_pages = math.ceil(total_votes / VOTES_PER_PAGE)

        # 2. Build DATA query for the specific page
        data_query = """
            SELECT v.Vote, b.BillNumber, b.Title, b.Congress, b.DateIntroduced
            FROM Votes v
            JOIN Bills b ON v.BillID = b.BillID
            WHERE v.PoliticianID = %s
        """
        data_params = [politician_id]

        if bill_type_filter and bill_type_filter in ['hr', 's', 'hjres', 'sjres']:
            data_query += " AND b.BillNumber ~* %s"
            data_params.append(f"^{bill_type_filter}[0-9]")
            
        # Add sorting
        if sort_order.lower() == 'asc':
            data_query += " ORDER BY b.DateIntroduced ASC, b.BillNumber ASC"
        else:
            data_query += " ORDER BY b.DateIntroduced DESC, b.BillNumber DESC"
        
        # Add pagination (LIMIT and OFFSET)
        data_query += " LIMIT %s OFFSET %s;"
        data_params.extend([VOTES_PER_PAGE, offset])

        cur.execute(data_query, tuple(data_params))
        votes = cur.fetchall()
        cur.close()
        
        # Return the data along with pagination info
        return jsonify({
            'pagination': {
                'currentPage': page,
                'totalPages': total_pages,
                'totalVotes': total_votes,
                'perPage': VOTES_PER_PAGE
            },
            'votes': votes
        })
        
    except Exception as e:
        print(f"Database error in /api/politician/<id>/votes: {e}")
        return jsonify({"error": "An internal database error occurred."}), 500
    finally:
        if conn: conn.close()
# --- *** END MODIFICATION *** ---

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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

