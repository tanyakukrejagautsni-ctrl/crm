import sqlite3
from contextlib import closing
from datetime import datetime
import pandas as pd
import streamlit as st
import uuid
import os

DB_PATH = "leads.db"

STATUSES = ["New", "Contacted", "Qualified", "In Progress", "Won", "Lost", "Closed"]
SOURCES = ["Website", "Referral", "Email", "Phone", "Social", "Event", "Other"]

# ---------- Database ----------
def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_number TEXT UNIQUE NOT NULL,
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
                preferred_time TEXT,
                created_at TEXT NOT NULL,
                created_time TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)
        conn.commit()

def _now():
    return datetime.utcnow().isoformat(timespec="seconds")

def _time():
    return datetime.utcnow().strftime("%H:%M:%S")

def _ref():
    return str(uuid.uuid4())[:8].upper()

def add_lead(data: dict):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
            INSERT INTO leads (ref_number, name, email, phone, place, source, owner, status, value, tags, notes, preferred_time, created_at, created_time, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            _ref(),
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
            data.get("preferred_time"),
            _now(),
            _time(),
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
        clauses.append("(name LIKE ? OR email LIKE ? OR place LIKE ? OR owner LIKE ? OR tags LIKE ? OR notes LIKE ?)")
        params += [q, q, q, q, q, q]
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

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Lead CRM", layout="wide")
init_db()

st.title("🗂️ Lead Management CRM (Enhanced)")

# Sidebar - Add Lead
with st.sidebar:
    st.header("➕ Add Lead")
    with st.form("add_form"):
        name = st.text_input("Name*")
        email = st.text_input("Email")
        phone = st.text_input("Phone")
        place = st.text_input("Place")
        col1, col2 = st.columns(2)
        with col1:
            source = st.selectbox("Source", SOURCES)
        with col2:
            owner = st.text_input("Owner")
        col3, col4 = st.columns(2)
        with col3:
            status = st.selectbox("Status", STATUSES)
        with col4:
            value = st.number_input("Deal Value", min_value=0.0, step=100.0)
        preferred_time = st.time_input("Preferred Time to Call")
        tags = st.text_input("Tags (comma-separated)")
        notes = st.text_area("Notes")
        submitted = st.form_submit_button("Add Lead")
        if submitted:
            if not name.strip():
                st.error("Name is required.")
            else:
                add_lead({
                    "name": name, "email": email, "phone": phone, "place": place,
                    "source": source, "owner": owner, "status": status,
                    "value": value, "tags": tags, "notes": notes,
                    "preferred_time": preferred_time.strftime("%H:%M") if preferred_time else None
                })
                st.success(f"Lead '{name}' added.")
                st.rerun()

# Tabs
tab1, tab2, tab3 = st.tabs(["Leads", "Analytics", "Import/Export"])

# --- Leads Tab ---
with tab1:
    st.subheader("All Leads")
    q = st.text_input("Search")
    col1, col2, col3, col4 = st.columns(4)
    with col1: status_f = st.selectbox("Status", [""] + STATUSES)
    with col2: owner_f = st.text_input("Owner filter")
    with col3: source_f = st.selectbox("Source", [""] + SOURCES)
    with col4: order_by = st.selectbox("Order", ["created_at DESC","value DESC","value ASC","name ASC","name DESC"])

    df = fetch_leads({"q": q, "status": status_f or None, "owner": owner_f, "source": source_f or None, "order_by": order_by})
    st.write(f"{len(df)} lead(s) found")

    if df.empty:
        st.info("No leads yet.")
    else:
        if "edit_id" not in st.session_state:
            st.session_state.edit_id = None

        for _, row in df.iterrows():
            col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
            with col1:
                st.write(f"**{row['name']}** (Ref: {row['ref_number']})  \n{row['email']} | {row['place']} | {row['status']} | Call: {row['preferred_time']}")
            with col2:
                st.write(f"💰 {row['value']}")
            with col3:
                st.write(f"🕒 {row['created_time']}")
            with col4:
                if st.button("✏️ Edit", key=f"edit{row['id']}"):
                    st.session_state.edit_id = row["id"]
            with col5:
                if st.button("🗑️ Delete", key=f"del{row['id']}"):
                    delete_lead(int(row["id"]))
                    st.success(f"Deleted lead: {row['name']}")
                    st.rerun()

            if st.session_state.edit_id == row["id"]:
                st.markdown("---")
                with st.form(f"edit_form_{row['id']}"):
                    new_name = st.text_input("Name", value=row["name"])
                    new_email = st.text_input("Email", value=row["email"] or "")
                    new_phone = st.text_input("Phone", value=row["phone"] or "")
                    new_place = st.text_input("Place", value=row["place"] or "")
                    new_source = st.selectbox("Source", SOURCES, index=SOURCES.index(row["source"]) if row["source"] in SOURCES else 0)
                    new_owner = st.text_input("Owner", value=row["owner"] or "")
                    new_status = st.selectbox("Status", STATUSES, index=STATUSES.index(row["status"]))
                    new_value = st.number_input("Deal Value", min_value=0.0, step=100.0, value=float(row["value"]))
                    new_preferred_time = st.text_input("Preferred Time", value=row["preferred_time"] or "")
                    new_tags = st.text_input("Tags", value=row["tags"] or "")
                    new_notes = st.text_area("Notes", value=row["notes"] or "")
                    save_changes = st.form_submit_button("💾 Save Changes")
                    if save_changes:
                        update_lead(int(row["id"]), {
                            "name": new_name,
                            "email": new_email,
                            "phone": new_phone,
                            "place": new_place,
                            "source": new_source,
                            "owner": new_owner,
                            "status": new_status,
                            "value": new_value,
                            "preferred_time": new_preferred_time,
                            "tags": new_tags,
                            "notes": new_notes
                        })
                        st.success(f"Updated lead: {new_name}")
                        st.session_state.edit_id = None
                        st.rerun()

# --- Analytics Tab ---
with tab2:
    st.subheader("Analytics")
    df_all = fetch_leads()
    if df_all.empty:
        st.info("No data for analytics.")
    else:
        st.bar_chart(df_all["status"].value_counts())
        st.bar_chart(df_all.groupby("status")["value"].sum())

# --- Import/Export Tab ---
with tab3:
    st.subheader("Export")
    df_all = fetch_leads()
    if not df_all.empty:
        st.download_button("⬇️ Download CSV", df_all.to_csv(index=False), "leads.csv")
        st.download_button("⬇️ Download Excel", df_all.to_excel("leads.xlsx", index=False), "leads.xlsx")

    st.subheader("Import")
    file = st.file_uploader("Upload file", type=["csv", "xlsx"])
    if file:
        try:
            if file.name.endswith(".csv"):
                new = pd.read_csv(file)
            else:
                new = pd.read_excel(file)

            with closing(sqlite3.connect(DB_PATH)) as conn:
                existing_refs = pd.read_sql_query("SELECT ref_number FROM leads", conn)["ref_number"].tolist()
                if "ref_number" in new.columns:
                    new = new[~new["ref_number"].isin(existing_refs)]
                new.to_sql("leads", conn, if_exists="append", index=False)

            st.success("File imported successfully (duplicates skipped)")
            st.rerun()
        except Exception as e:
            st.error(f"Import failed: {e}")
