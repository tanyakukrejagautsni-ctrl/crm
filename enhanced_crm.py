import sqlite3
from contextlib import closing
from datetime import datetime, date, time
import pandas as pd
import streamlit as st
import uuid

DB_PATH = "enhanced_leads.db"

STATUSES = ["New", "Contacted", "Qualified", "In Progress", "Won", "Lost", "Closed"]
SOURCES = ["Website", "Referral", "Email", "Phone", "Social", "Event", "Other"]

# ---------- Helpers ----------
def _now():
    return datetime.utcnow().isoformat(timespec="seconds")

def _generate_ref():
    return "REF-" + uuid.uuid4().hex[:8].upper()

def format_datetime(dt_str):
    """Format datetime string for display"""
    if not dt_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%Y-%m-%d %H:%M")
    except:
        return dt_str

# ---------- Database Initialization with Migration ----------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
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
            return

        # Table exists, check columns
        cur.execute("PRAGMA table_info(leads)")
        cols = [c[1] for c in cur.fetchall()]

        # Add missing columns
        if "place" not in cols:
            conn.execute("ALTER TABLE leads ADD COLUMN place TEXT;")
        if "ref_number" not in cols:
            conn.execute("ALTER TABLE leads ADD COLUMN ref_number TEXT;")
            cur.execute("SELECT id FROM leads WHERE ref_number IS NULL OR ref_number=''")
            for rid, in cur.fetchall():
                conn.execute("UPDATE leads SET ref_number=? WHERE id=?", (_generate_ref(), rid))
        if "created_at" not in cols:
            conn.execute("ALTER TABLE leads ADD COLUMN created_at TEXT;")
            conn.execute("UPDATE leads SET created_at=?", (_now(),))
        if "updated_at" not in cols:
            conn.execute("ALTER TABLE leads ADD COLUMN updated_at TEXT;")
            conn.execute("UPDATE leads SET updated_at=?", (_now(),))
        if "preferred_date" not in cols:
            conn.execute("ALTER TABLE leads ADD COLUMN preferred_date TEXT;")
        if "preferred_time" not in cols:
            conn.execute("ALTER TABLE leads ADD COLUMN preferred_time TEXT;")
        
        conn.commit()

# ---------- CRUD ----------
def add_lead(data: dict):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
            INSERT INTO leads
            (ref_number, name, email, phone, place, source, owner, status, value, tags, notes, 
             preferred_date, preferred_time, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("ref_number") or _generate_ref(),
            data.get("name"),
            data.get("email"),
            data.get("phone"),
            data.get("place"),
            data.get("source"),
            data.get("owner"),
            data.get("status", "New"),
            float(data.get("value") or 0),
            data.get("tags"),
            data.get("notes"),
            data.get("preferred_date"),
            data.get("preferred_time"),
            _now(),
            _now(),
        ))
        conn.commit()

def update_lead(lead_id: int, updates: dict):
    if not updates:
        return
    set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
    params = list(updates.values()) + [_now(), lead_id]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(f"UPDATE leads SET {set_clause}, updated_at=? WHERE id=?", params)
        conn.commit()

def delete_lead(lead_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM leads WHERE id=?", (lead_id,))
        conn.commit()

def fetch_leads(filters: dict = None) -> pd.DataFrame:
    filters = filters or {}
    clauses, params = [], []
    if filters.get("q"):
        q = f"%{filters['q']}%"
        clauses.append("(name LIKE ? OR email LIKE ? OR place LIKE ? OR owner LIKE ? OR tags LIKE ? OR notes LIKE ? OR ref_number LIKE ?)")
        params += [q, q, q, q, q, q, q]
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
    order = filters.get("order_by") or "created_at DESC"
    with closing(sqlite3.connect(DB_PATH)) as conn:
        df = pd.read_sql_query(f"SELECT * FROM leads {where} ORDER BY {order}", conn, params=params)
    return df

def get_lead_by_id(lead_id: int) -> dict:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM leads WHERE id=?", (lead_id,))
        row = cur.fetchone()
        if row:
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    return None

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Enhanced Lead CRM", layout="wide", page_icon="🏢")

# Initialize database
try:
    init_db()
except Exception as e:
    st.error(f"Database initialization error: {str(e)}")
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
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>🏢 Enhanced Lead Management CRM</h1>
    <p>Comprehensive lead tracking with scheduling and analytics</p>
</div>
""", unsafe_allow_html=True)

# Initialize session state
if "edit_id" not in st.session_state:
    st.session_state.edit_id = None

# Sidebar - Add Lead
with st.sidebar:
    st.header("➕ Add New Lead")
    with st.form("add_lead_form", clear_on_submit=True):
        st.subheader("Contact Information")
        name = st.text_input("Name*", placeholder="Enter full name")
        email = st.text_input("Email", placeholder="contact@example.com")
        phone = st.text_input("Phone", placeholder="+1-234-567-8900")
        place = st.text_input("Company/Place", placeholder="Company name or location")
        
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
                try:
                    add_lead({
                        "ref_number": ref_number,
                        "name": name,
                        "email": email,
                        "phone": phone,
                        "place": place,
                        "source": source,
                        "owner": owner,
                        "status": status,
                        "value": value,
                        "tags": tags,
                        "notes": notes,
                        "preferred_date": str(preferred_date) if preferred_date else None,
                        "preferred_time": str(preferred_time) if preferred_time else None
                    })
                    st.success(f"✅ Lead '{name}' added successfully!\n📋 Reference: {ref_number}")
                    st.balloons()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error adding lead: {str(e)}")

# Main content tabs
tab1, tab2, tab3, tab4 = st.tabs(["📋 All Leads", "📊 Analytics", "📅 Schedule", "💾 Import/Export"])

# --- Leads Tab ---
with tab1:
    st.subheader("🔍 Lead Management")
    
    # Search and filter section
    with st.container():
        col1, col2 = st.columns([2, 1])
        with col1:
            q = st.text_input("🔍 Search leads...", placeholder="Search by name, email, company, reference number...")
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
    
    try:
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
                            try:
                                delete_lead(int(row["id"]))
                                st.success(f"🗑️ Deleted lead: {row['name']}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error deleting lead: {str(e)}")

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
                                try:
                                    update_lead(int(row["id"]), {
                                        "name": new_name,
                                        "email": new_email,
                                        "phone": new_phone,
                                        "place": new_place,
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
                                except Exception as e:
                                    st.error(f"❌ Error updating lead: {str(e)}")
                            
                            if cancel_edit:
                                st.session_state.edit_id = None
                                st.rerun()

                    st.divider()
    
    except Exception as e:
        st.error(f"❌ Error fetching leads: {str(e)}")

# --- Analytics Tab ---
with tab2:
    st.subheader("📊 Lead Analytics")
    
    try:
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
                st.subheader("Leads by Owner")
                if df_all['owner'].notna().any():
                    owner_counts = df_all[df_all['owner'].notna()]["owner"].value_counts()
                    st.bar_chart(owner_counts)
                else:
                    st.info("No owner data available")
    
    except Exception as e:
        st.error(f"❌ Error generating analytics: {str(e)}")

# --- Schedule Tab ---
with tab3:
    st.subheader("📅 Scheduled Contacts")
    
    try:
        # Filter for leads with scheduling info
        df_scheduled = fetch_leads()
        df_scheduled = df_scheduled[
            (df_scheduled['preferred_date'].notna()) | 
            (df_scheduled['preferred_time'].notna())
        ]
        
        if df_scheduled.empty:
            st.info("📅 No scheduled contacts found.")
        else:
            # Sort by preferred date
            df_scheduled = df_scheduled.sort_values('preferred_date', na_last=True)
            
            for _, row in df_scheduled.iterrows():
                with st.container():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        pdate = row['preferred_date'] if row['preferred_date'] else "No date set"
                        ptime = row['preferred_time'] if row['preferred_time'] else "No time set"
                        st.write(f"**{row['name']}** ({row['ref_number']})")
                        st.write(f"📧 {row['email']} | 📞 {row['phone']}")
                    
                    with col2:
                        st.write(f"📅 **{pdate}**")
                        st.write(f"⏰ **{ptime}**")
                    
                    with col3:
                        st.write(f"Status: **{row['status']}**")
                        if row['owner']:
                            st.write(f"Owner: {row['owner']}")
                    
                    st.divider()
    
    except Exception as e:
        st.error(f"❌ Error fetching scheduled leads: {str(e)}")

# --- Import/Export Tab ---
with tab4:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⬇️ Export Data")
        try:
            df_all = fetch_leads()
            if not df_all.empty:
                csv_data = df_all.to_csv(index=False)
                st.download_button(
                    "📁 Download All Leads (CSV)",
                    csv_data,
                    "enhanced_leads.csv",
                    "text/csv",
                    use_container_width=True
                )
                st.success(f"✅ Ready to export {len(df_all)} leads")
            else:
                st.info("No data to export")
        except Exception as e:
            st.error(f"❌ Error preparing export: {str(e)}")

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
                    
                    # Import to database
                    try:
                        with closing(sqlite3.connect(DB_PATH)) as conn:
                            new_df.to_sql("leads", conn, if_exists="append", index=False)
                        
                        st.success(f"✅ Successfully imported {len(new_df)} leads!")
                        st.balloons()
                        st.rerun()
                    except Exception as import_error:
                        st.error(f"❌ Database import failed: {str(import_error)}")
                        
            except Exception as e:
                st.error(f"❌ Import failed: {e}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 20px;'>
        🏢 Enhanced Lead Management CRM | Built with Streamlit | 
        Features: Lead tracking, Scheduling, Analytics, Import/Export
    </div>
    """, 
    unsafe_allow_html=True
)
