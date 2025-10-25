import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request
from flask_cors import CORS
import math

# --- Add parent directory to path to find 'config' ---
# This gets the absolute path to this file (api/app.py)
current_dir = os.path.dirname(os.path.abspath(__file__))
# This gets the path to the parent 'paper-trail' folder
parent_dir = os.path.dirname(current_dir)
# This adds the 'paper-trail' folder to Python's search path
sys.path.append(parent_dir)

# Try to import the config file
try:
    import config
except ModuleNotFoundError:
    print("="*50)
    print(f"ERROR: 'config.py' not found in parent directory: {parent_dir}")
    print("Please make sure 'config.py' exists in your main 'paper-trail' folder.")
    print("="*50)
    sys.exit(1) # Stop the script

# --- App Initialization ---
app = Flask(__name__)
# Allow requests from any origin
CORS(app, resources={r"/api/*": {"origins": "*"}})

# --- Database Connection Helper ---
def get_db_connection():
    """Establishes and returns a new connection to the Supabase database."""
    if not config.DB_CONNECTION_STRING:
        raise Exception("DB_CONNECTION_STRING not set in config.py")
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

@app.route('/api/politician/<int:politician_id>/votes')
def get_votes_by_politician(politician_id):
    """
    Gets the paginated voting record for a single politician.
    Example: /api/politician/530/votes?page=1&type=hr&sort=asc
    """
    bill_type_filter = request.args.get('type', None)
    sort_order = request.args.get('sort', 'desc')
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
    if page < 1:
        page = 1
    
    VOTES_PER_PAGE = 50 
    offset = (page - 1) * VOTES_PER_PAGE
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Build COUNT query
        count_query_base = "FROM Votes v JOIN Bills b ON v.BillID = b.BillID WHERE v.PoliticianID = %s"
        count_params = [politician_id]
        
        if bill_type_filter and bill_type_filter in ['hr', 's', 'hjres', 'sjres']:
            count_query_base += " AND b.BillNumber ~* %s" # Use regex for exact start
            count_params.append(f"^{bill_type_filter}[0-9]")
        
        count_query_final = f"SELECT COUNT(v.VoteID) {count_query_base}"
        cur.execute(count_query_final, tuple(count_params))
        total_votes = cur.fetchone()['count']
        total_pages = math.ceil(total_votes / VOTES_PER_PAGE)

        # 2. Build DATA query
        data_query = f"SELECT v.Vote, b.BillNumber, b.Title, b.Congress, b.DateIntroduced, b.subjects {count_query_base}"
        data_params = count_params
            
        if sort_order.lower() == 'asc':
            data_query += " ORDER BY b.DateIntroduced ASC, b.BillNumber ASC"
        else:
            data_query += " ORDER BY b.DateIntroduced DESC, b.BillNumber DESC"
        
        data_query += " LIMIT %s OFFSET %s;"
        data_params.extend([VOTES_PER_PAGE, offset])

        cur.execute(data_query, tuple(data_params))
        votes = cur.fetchall()
        cur.close()
        
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

@app.route('/api/politician/<int:politician_id>/donations/summary')
def get_donations_summary_by_politician(politician_id):
    """Gets a summarized list of donations for a politician for a pie chart."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        industry_filter = request.args.get('industry', None)
        query_params = [politician_id]
        
        query_base = """
            FROM Donations d
            JOIN Donors dn ON d.DonorID = dn.DonorID
            WHERE d.PoliticianID = %s
        """
        
        if industry_filter:
            if industry_filter.lower() == 'pac/party':
                 query_base += " AND dn.DonorType = 'PAC/Party'"
            elif industry_filter.lower() == 'individual':
                  query_base += " AND dn.DonorType = 'Individual'"
        
        
        final_query = f"""
            WITH PoliticianDonations AS (
                SELECT 
                    d.DonorID, 
                    SUM(d.Amount) AS TotalAmount
                {query_base}
                GROUP BY d.DonorID
            ), 
            TotalReceived AS (
                SELECT SUM(TotalAmount) as GrandTotal 
                FROM PoliticianDonations 
                WHERE TotalAmount > 0
            )
            SELECT 
                dn.Name AS DonorName, 
                dn.DonorType, 
                dn.Employer, 
                dn.State AS DonorState,
                pd.TotalAmount, 
                (pd.TotalAmount / NULLIF(tr.GrandTotal, 0)) * 100 AS Percentage
            FROM PoliticianDonations pd
            JOIN Donors dn ON pd.DonorID = dn.DonorID
            JOIN TotalReceived tr ON 1=1
            WHERE pd.TotalAmount > 0 
            ORDER BY pd.TotalAmount DESC;
        """
        
        cur.execute(final_query, tuple(query_params))
        
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

# This makes the script runnable
if __name__ == '__main__':
    # host='0.0.0.0' makes it accessible on your local network
    app.run(debug=True, host='0.0.0.0', port=5000)

