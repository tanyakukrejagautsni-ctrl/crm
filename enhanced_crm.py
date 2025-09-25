import sqlite3
from contextlib import closing
from datetime import datetime, date, time
import pandas as pd
import streamlit as st
import uuid
import random
import os

# Use absolute path for database to ensure persistence
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "enhanced_leads.db")

STATUSES = ["New", "Contacted", "Qualified", "In Progress", "Won", "Lost", "Closed"]
SOURCES = ["Website", "Referral", "Email", "Phone", "Social", "Event", "Other"]

# ---------- Helpers ----------
def _now():
    return datetime.utcnow().isoformat(timespec="seconds")

def _generate_ref():
    """Generate reference in format GDC-XX-DDMMYYYY"""
    today = datetime.now()
    random_numbers = f"{random.randint(10, 99):02d}"
    date_part = today.strftime("%d%m%Y")
    return f"GDC-{random_numbers}-{date_part}"

def format_datetime(dt_str):
    """Format datetime string for display"""
    if not dt_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%Y-%m-%d %H:%M")
    except:
        return dt_str

def format_address(street, city, state, zip_code, country):
    """Format address components into a readable address"""
    address_parts = []
    if street:
        address_parts.append(street)
    if city:
        address_parts.append(city)
    if state:
        address_parts.append(state)
    if zip_code:
        address_parts.append(zip_code)
    if country:
        address_parts.append(country)
    
    return ", ".join(filter(None, address_parts)) if address_parts else ""

# ---------- Database Initialization with Migration ----------
def init_db():
    """Initialize database with proper error handling and persistence"""
    global DB_PATH
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=memory;")
            conn.execute("PRAGMA mmap_size=268435456;")  # 256MB
            
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='leads'")
            if not cur.fetchone():
                conn.execute("""
                    CREATE TABLE leads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ref_number TEXT UNIQUE,
                        name TEXT NOT NULL,
                        email TEXT,
                        phone TEXT,
                        place TEXT,
                        street_address TEXT,
                        city TEXT,
                        state TEXT,
                        zip_code TEXT,
                        country TEXT,
                        full_address TEXT,
                        source TEXT,
                        owner TEXT,
                        status TEXT DEFAULT 'New',
                        value REAL DEFAULT 0,
                        tags TEXT,
                        notes TEXT,
                        preferred_date TEXT,
                        preferred_time TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                """)
                # Create index for better performance
                conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_ref ON leads(ref_number);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);")
                conn.commit()
                return

            # Table exists, check columns
            cur.execute("PRAGMA table_info(leads)")
            cols = [c[1] for c in cur.fetchall()]

            # Add missing columns
            if "place" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN place TEXT;")
            if "street_address" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN street_address TEXT;")
            if "city" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN city TEXT;")
            if "state" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN state TEXT;")
            if "zip_code" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN zip_code TEXT;")
            if "country" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN country TEXT;")
            if "full_address" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN full_address TEXT;")
            
            if "ref_number" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN ref_number TEXT;")
                cur.execute("SELECT id FROM leads WHERE ref_number IS NULL OR ref_number=''")
                for rid, in cur.fetchall():
                    conn.execute("UPDATE leads SET ref_number=? WHERE id=?", (_generate_ref(), rid))
            else:
                # Update existing reference numbers to new format if they don't match
                cur.execute("SELECT id, ref_number FROM leads")
                for rid, ref_num in cur.fetchall():
                    if not ref_num or not ref_num.startswith("GDC-"):
                        conn.execute("UPDATE leads SET ref_number=? WHERE id=?", (_generate_ref(), rid))
            
            if "created_at" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN created_at TEXT;")
                conn.execute("UPDATE leads SET created_at=? WHERE created_at IS NULL OR created_at=''", (_now(),))
            if "updated_at" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN updated_at TEXT;")
                conn.execute("UPDATE leads SET updated_at=? WHERE updated_at IS NULL OR updated_at=''", (_now(),))
            if "preferred_date" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN preferred_date TEXT;")
            if "preferred_time" not in cols:
                conn.execute("ALTER TABLE leads ADD COLUMN preferred_time TEXT;")
            
            # Create indexes if they don't exist
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_ref ON leads(ref_number);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);")
            
            conn.commit()
            
    except Exception as e:
        st.error(f"Database initialization error: {str(e)}")
        # Fallback: try to create database in current directory
        DB_PATH = "enhanced_leads.db"
        try:
            with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS leads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ref_number TEXT UNIQUE,
                        name TEXT NOT NULL,
                        email TEXT,
                        phone TEXT,
                        place TEXT,
                        street_address TEXT,
                        city TEXT,
                        state TEXT,
                        zip_code TEXT,
                        country TEXT,
                        full_address TEXT,
                        source TEXT,
                        owner TEXT,
                        status TEXT DEFAULT 'New',
                        value REAL DEFAULT 0,
                        tags TEXT,
                        notes TEXT,
                        preferred_date TEXT,
                        preferred_time TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                """)
                conn.commit()
        except Exception as e2:
            st.error(f"Fallback database creation failed: {str(e2)}")
            raise e2

# ---------- CRUD ----------
def add_lead(data: dict):
    """Add lead with improved error handling and data validation"""
    try:
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            # Format the full address
            full_address = format_address(
                data.get("street_address"),
                data.get("city"),
                data.get("state"),
                data.get("zip_code"),
                data.get("country")
            )
            
            # Ensure required fields have values
            ref_number = data.get("ref_number") or _generate_ref()
            name = data.get("name", "").strip()
            
            if not name:
                raise ValueError("Name is required")
            
            conn.execute("""
                INSERT INTO leads
                (ref_number, name, email, phone, place, street_address, city, state, zip_code, 
                 country, full_address, source, owner, status, value, tags, notes, 
                 preferred_date, preferred_time, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ref_number,
                name,
                data.get("email", ""),
                data.get("phone", ""),
                data.get("place", ""),
                data.get("street_address", ""),
                data.get("city", ""),
                data.get("state", ""),
                data.get("zip_code", ""),
                data.get("country", ""),
                full_address,
                data.get("source", ""),
                data.get("owner", ""),
                data.get("status", "New"),
                float(data.get("value", 0)),
                data.get("tags", ""),
                data.get("notes", ""),
                data.get("preferred_date", ""),
                data.get("preferred_time", ""),
                _now(),
                _now(),
            ))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error adding lead: {str(e)}")
        return False

def update_lead(lead_id: int, updates: dict):
    if not updates:
        return
    
    try:
        # If any address component is updated, recalculate full_address
        address_fields = ["street_address", "city", "state", "zip_code", "country"]
        if any(field in updates for field in address_fields):
            # Get current address data
            current_lead = get_lead_by_id(lead_id)
            if current_lead:
                street = updates.get("street_address", current_lead.get("street_address", ""))
                city = updates.get("city", current_lead.get("city", ""))
                state = updates.get("state", current_lead.get("state", ""))
                zip_code = updates.get("zip_code", current_lead.get("zip_code", ""))
                country = updates.get("country", current_lead.get("country", ""))
                
                updates["full_address"] = format_address(street, city, state, zip_code, country)
        
        set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
        params = list(updates.values()) + [_now(), lead_id]
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            conn.execute(f"UPDATE leads SET {set_clause}, updated_at=? WHERE id=?", params)
            conn.commit()
    except Exception as e:
        st.error(f"Error updating lead: {str(e)}")

def delete_lead(lead_id: int):
    try:
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            conn.execute("DELETE FROM leads WHERE id=?", (lead_id,))
            conn.commit()
    except Exception as e:
        st.error(f"Error deleting lead: {str(e)}")

def fetch_leads(filters: dict = None) -> pd.DataFrame:
    try:
        filters = filters or {}
        clauses, params = [], []
        if filters.get("q"):
            q = f"%{filters['q']}%"
            clauses.append("(name LIKE ? OR email LIKE ? OR place LIKE ? OR owner LIKE ? OR tags LIKE ? OR notes LIKE ? OR ref_number LIKE ? OR full_address LIKE ?)")
            params += [q, q, q, q, q, q, q, q]
        if filters.get("status"):
            clauses.append("status=?")
            params.append(filters["status"])
        if filters.get("owner"):
            clauses.append("owner LIKE ?")
            params.append(f"%{filters['owner']}%")
        if filters.get("source"):
            clauses.append("source=?")
            params.append(filters["source"])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order = filters.get("order_by", "created_at DESC")
        
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            query = f"SELECT * FROM leads {where} ORDER BY {order}"
            df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error fetching leads: {str(e)}")
        return pd.DataFrame()

def get_lead_by_id(lead_id: int) -> dict:
    try:
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM leads WHERE id=?", (lead_id,))
            row = cur.fetchone()
            if row:
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))
        return None
    except Exception as e:
        st.error(f"Error fetching lead by ID: {str(e)}")
        return None

def get_database_stats():
    """Get database statistics for debugging"""
    try:
        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM leads")
            count = cur.fetchone()[0]
            
            cur.execute("SELECT created_at FROM leads ORDER BY created_at DESC LIMIT 1")
            latest = cur.fetchone()
            latest_date = latest[0] if latest else "No data"
            
            return {
                "total_leads": count,
                "latest_entry": latest_date,
                "db_path": DB_PATH,
                "db_exists": os.path.exists(DB_PATH)
            }
    except Exception as e:
        return {
            "error": str(e),
            "db_path": DB_PATH,
            "db_exists": os.path.exists(DB_PATH)
        }

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Enhanced Lead CRM", layout="wide", page_icon="🏢")

# Initialize database with error handling
try:
    init_db()
    # Show database status in sidebar
    db_stats = get_database_stats()
    if "error" not in db_stats:
        st.sidebar.success(f"✅ Database Active: {db_stats['total_leads']} leads")
    else:
        st.sidebar.error(f"❌ Database Error: {db_stats['error']}")
except Exception as e:
    st.error(f"Critical database error: {str(e)}")
    st.stop()

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
    }
    .lead-card {
        border: 1px solid #e1e5e9;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
        background: #f8f9fa;
    }
    .status-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.875rem;
        font-weight: 500;
    }
    .status-new { background: #e3f2fd; color: #1976d2; }
    .status-contacted { background: #f3e5f5; color: #7b1fa2; }
    .status-qualified { background: #e8f5e8; color: #388e3c; }
    .status-won { background: #e8f5e8; color: #2e7d32; }
    .status-lost { background: #ffebee; color: #d32f2f; }
    .address-display {
        background: #f0f2f6;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 3px solid #667eea;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>🏢 Enhanced Lead Management CRM</h1>
    <p>Comprehensive lead tracking with scheduling, address management and analytics</p>
</div>
""", unsafe_allow_html=True)

# Initialize session state
if "edit_id" not in st.session_state:
    st.session_state.edit_id = None

# Sidebar - Add Lead
with st.sidebar:
    st.header("➕ Add New Lead")
    
    # Show database status
    db_stats = get_database_stats()
    if "error" not in db_stats:
        st.info(f"📊 Current leads: {db_stats['total_leads']}")
        if db_stats['latest_entry'] != "No data":
            st.caption(f"Latest: {format_datetime(db_stats['latest_entry'])}")
    
    with st.form("add_lead_form", clear_on_submit=True):
        st.subheader("Contact Information")
        name = st.text_input("Name*", placeholder="Enter full name")
        email = st.text_input("Email", placeholder="contact@example.com")
        phone = st.text_input("Phone", placeholder="+1-234-567-8900")
        place = st.text_input("Company/Place", placeholder="Company name or location")
        
        st.subheader("Address Information")
        street_address = st.text_input("Street Address", placeholder="123 Main Street")
        col_addr1, col_addr2 = st.columns(2)
        with col_addr1:
            city = st.text_input("City", placeholder="New York")
            zip_code = st.text_input("ZIP Code", placeholder="10001")
        with col_addr2:
            state = st.text_input("State", placeholder="NY")
            country = st.text_input("Country", placeholder="USA")
        
        st.subheader("Lead Details")
        col1, col2 = st.columns(2)
        with col1:
            source = st.selectbox("Source", SOURCES, help="How did you acquire this lead?")
        with col2:
            owner = st.text_input("Owner", placeholder="Assigned to")
        
        col3, col4 = st.columns(2)
        with col3:
            status = st.selectbox("Status", STATUSES, help="Current lead status")
        with col4:
            value = st.number_input("Deal Value ($)", min_value=0.0, step=100.0, help="Potential deal value")
        
        st.subheader("Scheduling")
        col5, col6 = st.columns(2)
        with col5:
            preferred_date = st.date_input("Preferred Date", value=None, help="When to contact this lead")
        with col6:
            preferred_time = st.time_input("Preferred Time", value=None, help="Best time to contact")
        
        tags = st.text_input("Tags", placeholder="tag1, tag2, tag3", help="Comma-separated tags")
        notes = st.text_area("Notes", placeholder="Additional information about the lead")
        
        submitted = st.form_submit_button("🚀 Add Lead", use_container_width=True)
        if submitted:
            if not name.strip():
                st.error("❌ Name is required.")
            else:
                ref_number = _generate_ref()
                lead_data = {
                    "ref_number": ref_number,
                    "name": name,
                    "email": email,
                    "phone": phone,
                    "place": place,
                    "street_address": street_address,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "country": country,
                    "source": source,
                    "owner": owner,
                    "status": status,
                    "value": value,
                    "tags": tags,
                    "notes": notes,
                    "preferred_date": str(preferred_date) if preferred_date else None,
                    "preferred_time": str(preferred_time) if preferred_time else None
                }
                
                if add_lead(lead_data):
                    st.success(f"✅ Lead '{name}' added successfully!\n📋 Reference: {ref_number}")
                    st.balloons()
                    st.rerun()

# Main content tabs
tab1, tab2, tab3, tab4 = st.tabs(["📋 All Leads", "📊 Analytics", "📅 Schedule", "💾 Import/Export"])

# --- Leads Tab ---
with tab1:
    st.subheader("🔍 Lead Management")
    
    # Search and filter section
    with st.container():
        col1, col2 = st.columns([2, 1])
        with col1:
            q = st.text_input("🔍 Search leads...", placeholder="Search by name, email, company, reference number, address...")
        with col2:
            st.write("")  # spacing
    
    # Filter row
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        status_f = st.selectbox("Status Filter", ["All"] + STATUSES)
    with col2:
        owner_f = st.text_input("Owner Filter", placeholder="Filter by owner")
    with col3:
        source_f = st.selectbox("Source Filter", ["All"] + SOURCES)
    with col4:
        order_options = {
            "Newest First": "created_at DESC",
            "Oldest First": "created_at ASC", 
            "Highest Value": "value DESC",
            "Lowest Value": "value ASC",
            "Name A-Z": "name ASC",
            "Name Z-A": "name DESC"
        }
        order_by = st.selectbox("Sort By", list(order_options.keys()))
    with col5:
        st.write("")  # spacing

    # Fetch and display leads
    filters = {
        "q": q,
        "status": status_f if status_f != "All" else None,
        "owner": owner_f,
        "source": source_f if source_f != "All" else None,
        "order_by": order_options[order_by]
    }
    
    df = fetch_leads(filters)
    
    # Results summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Total Leads", len(df))
    with col2:
        if not df.empty:
            total_value = df['value'].sum()
            st.metric("💰 Total Value", f"${total_value:,.2f}")
    with col3:
        if not df.empty:
            avg_value = df['value'].mean()
            st.metric("📈 Average Value", f"${avg_value:,.2f}")

    if df.empty:
        st.info("🔍 No leads found matching your criteria.")
        # Show database stats for debugging
        db_stats = get_database_stats()
        if "error" not in db_stats:
            st.info(f"📊 Database contains {db_stats['total_leads']} total leads")
            if db_stats['total_leads'] > 0:
                st.info("Try clearing your search filters to see all leads")
        else:
            st.error(f"Database error: {db_stats['error']}")
    else:
        # Display leads
        for _, row in df.iterrows():
            with st.container():
                # Lead card header
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                
                with col1:
                    status_class = f"status-{row['status'].lower().replace(' ', '-')}"
                    preferred_info = ""
                    if row['preferred_date'] or row['preferred_time']:
                        pdate = row['preferred_date'] if row['preferred_date'] else "No date"
                        ptime = row['preferred_time'] if row['preferred_time'] else "No time"
                        preferred_info = f"📅 {pdate} ⏰ {ptime}"
                    
                    st.markdown(f"""
                    **🧑‍💼 {row['name']}** (📋 {row['ref_number']})  
                    📧 {row['email'] or 'No email'} | 🏢 {row['place'] or 'No company'} | 
                    <span class="status-badge {status_class}">{row['status']}</span>  
                    📅 Created: {format_datetime(row['created_at'])}  
                    {preferred_info}
                    """, unsafe_allow_html=True)
                    
                    # Display address if available
                    if row['full_address']:
                        st.markdown(f"""
                        <div class="address-display">
                            📍 <strong>Address:</strong> {row['full_address']}
                        </div>
                        """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"**💰 ${row['value']:,.2f}**")
                    if row['owner']:
                        st.caption(f"👤 {row['owner']}")
                
                with col3:
                    if st.button("✏️ Edit", key=f"edit{row['id']}", use_container_width=True):
                        st.session_state.edit_id = row["id"]
                        st.rerun()
                
                with col4:
                    if st.button("🗑️ Delete", key=f"del{row['id']}", use_container_width=True, type="secondary"):
                        delete_lead(int(row["id"]))
                        st.success(f"🗑️ Deleted lead: {row['name']}")
                        st.rerun()

                # Show additional info if available
                if row['tags'] or row['notes']:
                    with st.expander("📝 Additional Information"):
                        if row['tags']:
                            st.write(f"🏷️ **Tags:** {row['tags']}")
                        if row['notes']:
                            st.write(f"📝 **Notes:** {row['notes']}")

                # Edit form
                if st.session_state.edit_id == row["id"]:
                    st.markdown("---")
                    st.subheader(f"✏️ Editing: {row['name']}")
                    
                    with st.form(f"edit_lead_form_{row['id']}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            new_name = st.text_input("Name", value=row["name"])
                            new_email = st.text_input("Email", value=row["email"] or "")
                            new_phone = st.text_input("Phone", value=row["phone"] or "")
                            new_place = st.text_input("Company/Place", value=row["place"] or "")
                        
                        with col2:
                            new_source = st.selectbox("Source", SOURCES, index=SOURCES.index(row["source"]) if row["source"] in SOURCES else 0)
                            new_owner = st.text_input("Owner", value=row["owner"] or "")
                            new_status = st.selectbox("Status", STATUSES, index=STATUSES.index(row["status"]))
                            new_value = st.number_input("Deal Value", min_value=0.0, step=100.0, value=float(row["value"]))
                        
                        # Address fields
                        st.subheader("Address Information")
                        new_street_address = st.text_input("Street Address", value=row["street_address"] or "")
                        col_addr1, col_addr2 = st.columns(2)
                        with col_addr1:
                            new_city = st.text_input("City", value=row["city"] or "")
                            new_zip_code = st.text_input("ZIP Code", value=row["zip_code"] or "")
                        with col_addr2:
                            new_state = st.text_input("State", value=row["state"] or "")
                            new_country = st.text_input("Country", value=row["country"] or "")
                        
                        # Scheduling
                        col3, col4 = st.columns(2)
                        with col3:
                            try:
                                current_date = datetime.fromisoformat(row["preferred_date"]).date() if row["preferred_date"] else None
                            except (ValueError, TypeError):
                                current_date = None
                            new_preferred_date = st.date_input("Preferred Date", value=current_date)
                        with col4:
                            try:
                                current_time = datetime.fromisoformat(f"2000-01-01 {row['preferred_time']}").time() if row["preferred_time"] else None
                            except (ValueError, TypeError):
                                current_time = None
                            new_preferred_time = st.time_input("Preferred Time", value=current_time)
                        
                        new_tags = st.text_input("Tags", value=row["tags"] or "")
                        new_notes = st.text_area("Notes", value=row["notes"] or "")
                        
                        col_save, col_cancel = st.columns(2)
                        with col_save:
                            save_changes = st.form_submit_button("💾 Save Changes", use_container_width=True)
                        with col_cancel:
                            cancel_edit = st.form_submit_button("❌ Cancel", use_container_width=True)
                        
                        if save_changes:
                            update_lead(int(row["id"]), {
                                "name": new_name,
                                "email": new_email,
                                "phone": new_phone,
                                "place": new_place,
                                "street_address": new_street_address,
                                "city": new_city,
                                "state": new_state,
                                "zip_code": new_zip_code,
                                "country": new_country,
                                "source": new_source,
                                "owner": new_owner,
                                "status": new_status,
                                "value": new_value,
                                "tags": new_tags,
                                "notes": new_notes,
                                "preferred_date": str(new_preferred_date) if new_preferred_date else None,
                                "preferred_time": str(new_preferred_time) if new_preferred_time else None
                            })
                            st.success(f"✅ Updated lead: {new_name}")
                            st.session_state.edit_id = None
                            st.rerun()
                        
                        if cancel_edit:
                            st.session_state.edit_id = None
                            st.rerun()

                st.divider()

# --- Analytics Tab ---
with tab2:
    st.subheader("📊 Lead Analytics")
    
    df_all = fetch_leads()
    
    if df_all.empty:
        st.info("📊 No data available for analytics.")
    else:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Leads", len(df_all))
        with col2:
            st.metric("Total Value", f"${df_all['value'].sum():,.2f}")
        with col3:
            won_leads = len(df_all[df_all['status'] == 'Won'])
            win_rate = (won_leads / len(df_all)) * 100 if len(df_all) > 0 else 0
            st.metric("Win Rate", f"{win_rate:.1f}%")
        with col4:
            avg_value = df_all['value'].mean()
            st.metric("Avg Deal Value", f"${avg_value:,.2f}")

        # Charts
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Leads by Status")
            status_counts = df_all["status"].value_counts()
            st.bar_chart(status_counts)

        with col2:
            st.subheader("Value by Status")
            value_by_status = df_all.groupby("status")["value"].sum()
            st.bar_chart(value_by_status)

        col3, col4 = st.columns(2)
        with col3:
            st.subheader("Leads by Source")
            source_counts = df_all["source"].value_counts()
            st.bar_chart(source_counts)

        with col4:
            st.subheader("Leads by Location")
            if df_all['city'].notna().any():
                city_counts = df_all[df_all['city'].notna()]["city"].value_counts().head(10)
                st.bar_chart(city_counts)
            else:
                st.info("No location data available")

# --- Schedule Tab ---
with tab3:
    st.subheader("📅 Scheduled Contacts")
    
    # Filter for leads with scheduling info
    df_scheduled = fetch_leads()
    df_scheduled = df_scheduled[
        (df_scheduled['preferred_date'].notna()) | 
        (df_scheduled['preferred_time'].notna())
    ]
    
    if df_scheduled.empty:
        st.info("📅 No scheduled contacts found.")
    else:
        # Sort by preferred date (compatible with older pandas versions)
        try:
            df_scheduled = df_scheduled.sort_values('preferred_date', na_position='last')
        except TypeError:
            # Fallback for older pandas versions
            df_scheduled = df_scheduled.sort_values('preferred_date')
        
        for _, row in df_scheduled.iterrows():
            with st.container():
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    pdate = row['preferred_date'] if row['preferred_date'] else "No date set"
                    ptime = row['preferred_time'] if row['preferred_time'] else "No time set"
                    st.write(f"**{row['name']}** ({row['ref_number']})")
                    st.write(f"📧 {row['email']} | 📞 {row['phone']}")
                    if row['full_address']:
                        st.write(f"📍 {row['full_address']}")
                
                with col2:
                    st.write(f"📅 **{pdate}**")
                    st.write(f"⏰ **{ptime}**")
                
                with col3:
                    st.write(f"Status: **{row['status']}**")
                    if row['owner']:
                        st.write(f"Owner: {row['owner']}")
                
                st.divider()

# --- Import/Export Tab ---
with tab4:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⬇️ Export Data")
        df_all = fetch_leads()
        if not df_all.empty:
            csv_data = df_all.to_csv(index=False)
            st.download_button(
                "📁 Download All Leads (CSV)",
                csv_data,
                "enhanced_leads_with_address.csv",
                "text/csv",
                use_container_width=True
            )
            st.success(f"✅ Ready to export {len(df_all)} leads")
        else:
            st.info("No data to export")

    with col2:
        st.subheader("⬆️ Import Data")
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
        
        if uploaded_file is not None:
            try:
                new_df = pd.read_csv(uploaded_file)
                
                st.write("Preview of uploaded data:")
                st.dataframe(new_df.head())
                
                if st.button("🚀 Import Data", key="import_data_btn", use_container_width=True):
                    # Handle column mapping
                    if "company" in new_df.columns and "place" not in new_df.columns:
                        new_df = new_df.rename(columns={"company": "place"})
                    
                    # Map address columns if they exist under different names
                    column_mapping = {
                        "address": "street_address",
                        "street": "street_address",
                        "postal_code": "zip_code",
                        "postcode": "zip_code",
                        "zip": "zip_code"
                    }
                    
                    for old_col, new_col in column_mapping.items():
                        if old_col in new_df.columns and new_col not in new_df.columns:
                            new_df = new_df.rename(columns={old_col: new_col})
                    
                    # Remove existing ID column if present
                    if "id" in new_df.columns:
                        new_df = new_df.drop(columns=["id"])
                    
                    # Add required columns
                    if "ref_number" not in new_df.columns:
                        new_df["ref_number"] = [_generate_ref() for _ in range(len(new_df))]
                    
                    now = _now()
                    if "created_at" not in new_df.columns:
                        new_df["created_at"] = now
                    if "updated_at" not in new_df.columns:
                        new_df["updated_at"] = now
                    
                    # Ensure numeric value column
                    new_df["value"] = pd.to_numeric(new_df.get("value", 0), errors="coerce").fillna(0)
                    
                    # Create full_address from components if not present
                    if "full_address" not in new_df.columns:
                        new_df["full_address"] = ""
                        for idx, row in new_df.iterrows():
                            full_addr = format_address(
                                row.get("street_address"),
                                row.get("city"),
                                row.get("state"),
                                row.get("zip_code"),
                                row.get("country")
                            )
                            new_df.at[idx, "full_address"] = full_addr
                    
                    # Ensure all required columns exist with default values
                    required_columns = [
                        "street_address", "city", "state", "zip_code", "country",
                        "place", "email", "phone", "source", "owner", "status",
                        "tags", "notes", "preferred_date", "preferred_time"
                    ]
                    
                    for col in required_columns:
                        if col not in new_df.columns:
                            new_df[col] = None
                    
                    # Set default status if not provided
                    new_df["status"] = new_df["status"].fillna("New")
                    
                    # Import to database
                    try:
                        with closing(sqlite3.connect(DB_PATH, timeout=30.0)) as conn:
                            new_df.to_sql("leads", conn, if_exists="append", index=False)
                        
                        st.success(f"✅ Successfully imported {len(new_df)} leads!")
                        st.balloons()
                        st.rerun()
                    except Exception as import_error:
                        st.error(f"❌ Database import failed: {str(import_error)}")
                        
            except Exception as e:
                st.error(f"❌ Import failed: {e}")

# Sample data generation section
st.markdown("---")
with st.expander("🎯 Generate Sample Data for Testing"):
    st.write("Generate sample leads with addresses for testing purposes")
    
    col1, col2 = st.columns(2)
    with col1:
        num_samples = st.number_input("Number of sample leads", min_value=1, max_value=50, value=5)
    with col2:
        if st.button("🎲 Generate Sample Data", use_container_width=True):
            try:
                sample_data = [
                    {
                        "name": f"Sample Lead {i+1}",
                        "email": f"lead{i+1}@example.com",
                        "phone": f"+1-555-{1000+i:04d}",
                        "place": f"Sample Company {i+1}",
                        "street_address": f"{100+i*10} Sample Street",
                        "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][i % 5],
                        "state": ["NY", "CA", "IL", "TX", "AZ"][i % 5],
                        "zip_code": f"{10001+i:05d}",
                        "country": "USA",
                        "source": SOURCES[i % len(SOURCES)],
                        "owner": f"Owner {i % 3 + 1}",
                        "status": STATUSES[i % len(STATUSES)],
                        "value": (i + 1) * 1000,
                        "tags": f"sample, test, lead{i+1}",
                        "notes": f"This is a sample lead #{i+1} for testing purposes."
                    }
                    for i in range(num_samples)
                ]
                
                success_count = 0
                for data in sample_data:
                    if add_lead(data):
                        success_count += 1
                
                st.success(f"✅ Generated {success_count} sample leads successfully!")
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error generating sample data: {str(e)}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        🏢 Enhanced Lead Management CRM with Address Management | Built with Streamlit<br>
        Features: Lead tracking, Address management, Scheduling, Analytics, Import/Export<br>
        Reference Format: GDC-XX-DDMMYYYY | Address Support: Street, City, State, ZIP, Country
    </div>
    """, 
    unsafe_allow_html=True
)
