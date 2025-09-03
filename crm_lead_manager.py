
# crm_lead_manager.py
import sqlite3
from contextlib import closing
from datetime import datetime
import pandas as pd
import streamlit as st

DB_PATH = "leads.db"

STATUSES = ["New", "Contacted", "Qualified", "In Progress", "Won", "Lost", "Closed"]
SOURCES = ["Website", "Referral", "Email", "Phone", "Social", "Event", "Other"]

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                company TEXT,
                source TEXT,
                owner TEXT,
                status TEXT DEFAULT 'New',
                value REAL DEFAULT 0,
                tags TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.commit()

def _now():
    return datetime.utcnow().isoformat(timespec="seconds")

def add_lead(data: dict):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO leads (name, email, phone, company, source, owner, status, value, tags, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("name"),
                data.get("email"),
                data.get("phone"),
                data.get("company"),
                data.get("source"),
                data.get("owner"),
                data.get("status", "New"),
                float(data.get("value") or 0),
                data.get("tags"),
                data.get("notes"),
                _now(),
                _now(),
            ),
        )
        conn.commit()
        return cur.lastrowid

def update_lead(lead_id: int, updates: dict):
    if not updates:
        return
    set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
    params = list(updates.values()) + [_now(), lead_id]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            f"UPDATE leads SET {set_clause}, updated_at=? WHERE id=?",
            params,
        )
        conn.commit()

def delete_lead(lead_id: int):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM leads WHERE id=?", (lead_id,))
        conn.commit()

def fetch_leads(filters: dict = None) -> pd.DataFrame:
    filters = filters or {}
    clauses = []
    params = []
    if filters.get("q"):
        q = filters.get("q")
        clauses.append("(name LIKE ? OR email LIKE ? OR company LIKE ? OR owner LIKE ? OR tags LIKE ? OR notes LIKE ?)")
        like = f"%{q}%"
        params += [like, like, like, like, like, like]
    if filters.get("status"):
        clauses.append("status = ?")
        params.append(filters.get("status"))
    if filters.get("owner"):
        clauses.append("owner LIKE ?")
        params.append(f"%{filters.get('owner')}%")
    if filters.get("source"):
        clauses.append("source = ?")
        params.append(filters.get("source"))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order = filters.get("order_by") or "created_at DESC"
    with closing(sqlite3.connect(DB_PATH)) as conn:
        df = pd.read_sql_query(f"SELECT * FROM leads {where} ORDER BY {order}", conn, params=params)
    return df

def export_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def import_csv(file) -> int:
    df = pd.read_csv(file)
    required = {"name"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        count = 0
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO leads (name, email, phone, company, source, owner, status, value, tags, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("name"),
                    row.get("email"),
                    row.get("phone"),
                    row.get("company"),
                    row.get("source"),
                    row.get("owner"),
                    row.get("status") if str(row.get("status")) in STATUSES else "New",
                    float(row.get("value") or 0),
                    row.get("tags"),
                    row.get("notes"),
                    _now(),
                    _now(),
                ),
            )
            count += 1
        conn.commit()
    return count

# ------------------------ UI ------------------------

st.set_page_config(page_title="Lead CRM (Python)", layout="wide")
init_db()

st.title("🗂️ Lead Management CRM (Python)")

# Sidebar: add new lead
with st.sidebar:
    st.header("➕ Add Lead")
    with st.form("add_lead"):
        name = st.text_input("Name*", placeholder="Jane Doe")
        email = st.text_input("Email", placeholder="jane@acme.com")
        phone = st.text_input("Phone", placeholder="+1 555 123 4567")
        company = st.text_input("Company", placeholder="Acme Inc.")
        col1, col2 = st.columns(2)
        with col1:
            source = st.selectbox("Source", options=SOURCES, index=len(SOURCES) - 1)
        with col2:
            owner = st.text_input("Owner", placeholder="You")
        col3, col4 = st.columns(2)
        with col3:
            status = st.selectbox("Status", options=STATUSES, index=0)
        with col4:
            value = st.number_input("Deal Value", min_value=0.0, step=100.0)
        tags = st.text_input("Tags (comma-separated)", placeholder="priority, demo")
        notes = st.text_area("Notes")
        submitted = st.form_submit_button("Add Lead", use_container_width=True)
        if submitted:
            if not name.strip():
                st.error("Name is required.")
            else:
                add_lead({
                    "name": name.strip(),
                    "email": email.strip() or None,
                    "phone": phone.strip() or None,
                    "company": company.strip() or None,
                    "source": source,
                    "owner": owner.strip() or None,
                    "status": status,
                    "value": value,
                    "tags": tags.strip() or None,
                    "notes": notes.strip() or None,
                })
                st.success(f"Lead '{name}' added.")

# Main: tabs
tab1, tab2, tab3 = st.tabs(["Leads", "Analytics", "Import/Export"])

with tab1:
    st.subheader("All Leads")
    q = st.text_input("Search", placeholder="Search name, email, company, owner, tags, notes")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        status_filter = st.selectbox("Status filter", options=[""] + STATUSES, index=0)
    with c2:
        owner_filter = st.text_input("Owner filter", placeholder="Owner name...")
    with c3:
        source_filter = st.selectbox("Source filter", options=[""] + SOURCES, index=0)
    with c4:
        order_by = st.selectbox(
            "Order by",
            options=[
                "created_at DESC",
                "created_at ASC",
                "updated_at DESC",
                "updated_at ASC",
                "value DESC",
                "value ASC",
                "name ASC",
                "name DESC",
            ],
            index=0,
        )

    df = fetch_leads({
        "q": q.strip() or None,
        "status": status_filter or None,
        "owner": owner_filter.strip() or None,
        "source": source_filter or None,
        "order_by": order_by,
    })

    st.caption(f"{len(df)} lead(s)")
    if len(df) == 0:
        st.info("No leads yet. Add one from the sidebar ➕")
    else:
        # Show editable grid
        editable_cols = ["name", "email", "phone", "company", "source", "owner", "status", "value", "tags", "notes"]
        df_display = df.copy()
        df_display["value"] = df_display["value"].astype(float)
        edited = st.data_editor(
            df_display,
            num_rows="dynamic",
            column_config={
                "status": st.column_config.SelectboxColumn("status", options=STATUSES),
                "source": st.column_config.SelectboxColumn("source", options=SOURCES),
                "value": st.column_config.NumberColumn("value", step=100.0, format="%.2f"),
                "created_at": st.column_config.DatetimeColumn("created_at", disabled=True),
                "updated_at": st.column_config.DatetimeColumn("updated_at", disabled=True),
                "id": st.column_config.Column("id", disabled=True),
            },
            hide_index=True,
        )

        # Detect changes and persist
        if st.button("💾 Save Changes", use_container_width=True):
            # Compare row by row using id
            for _, row in edited.iterrows():
                original = df[df["id"] == row["id"]].iloc[0].to_dict()
                updates = {}
                for col in editable_cols:
                    if str(row[col]) != str(original.get(col)):
                        updates[col] = row[col]
                if updates:
                    update_lead(int(row["id"]), updates)

            # Detect deletions: ids in original but not in edited
            deleted_ids = set(df["id"]).difference(set(edited["id"]))
            for did in deleted_ids:
                delete_lead(int(did))

            st.success("Changes saved. Refresh the table to see timestamps update.")

with tab2:
    st.subheader("Analytics")
    df_all = fetch_leads()
    if len(df_all) == 0:
        st.info("Add some leads to see analytics.")
    else:
        # Status counts
        status_counts = df_all["status"].value_counts().reindex(STATUSES, fill_value=0)
        st.write("""**Leads by Status**""")
        st.bar_chart(status_counts)

        # Pipeline value by status
        value_by_status = df_all.groupby("status")["value"].sum().reindex(STATUSES, fill_value=0.0)
        st.write("""**Pipeline Value by Status**""")
        st.bar_chart(value_by_status)

        # Owner leaderboard
        owner_counts = df_all["owner"].fillna("Unassigned").value_counts()
        st.write("""**Leads per Owner**""")
        st.bar_chart(owner_counts)

with tab3:
    st.subheader("Import / Export")
    df_all = fetch_leads()
    colL, colR = st.columns(2)
    with colL:
        st.write("""**Export CSV**""")
        if len(df_all) == 0:
            st.caption("No data to export yet.")
        else:
            st.download_button(
                "⬇️ Download leads.csv",
                data=export_csv(df_all),
                file_name="leads.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with colR:
        st.write("""**Import CSV**""")
        st.caption("CSV columns supported: name, email, phone, company, source, owner, status, value, tags, notes")
        upload = st.file_uploader("Upload CSV", type=["csv"])
        if upload:
            try:
                count = import_csv(upload)
                st.success(f"Imported {count} row(s).")
            except Exception as e:
                st.error(f"Import failed: {e}")
