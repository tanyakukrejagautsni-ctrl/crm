import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, date
import sqlite3
import plotly.express as px
import plotly.graph_objects as go

# Configure the page
st.set_page_config(
    page_title="Enhanced CRM System",
    page_icon="📊",
    layout="wide"
)

# Database setup for persistent storage
def init_database():
    """Initialize SQLite database for persistent storage"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    
    # Create leads table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            company TEXT,
            source TEXT,
            status TEXT DEFAULT 'New',
            notes TEXT,
            created_date DATE,
            created_time TEXT,
            follow_up_date DATE,
            value REAL DEFAULT 0
        )
    ''')
    
    # Create customers table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            company TEXT,
            address TEXT,
            status TEXT DEFAULT 'Active',
            notes TEXT,
            created_date DATE,
            created_time TEXT,
            customer_value REAL DEFAULT 0
        )
    ''')
    
    # Create activities table for tracking interactions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            customer_id INTEGER,
            activity_type TEXT,
            description TEXT,
            activity_date DATE,
            activity_time TEXT,
            FOREIGN KEY (lead_id) REFERENCES leads (id),
            FOREIGN KEY (customer_id) REFERENCES customers (id)
        )
    ''')
    
    conn.commit()
    conn.close()

# Database operations for leads
def save_lead_to_db(lead_data):
    """Save lead to database"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO leads (name, email, phone, company, source, status, notes, created_date, created_time, follow_up_date, value)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        lead_data['name'],
        lead_data['email'],
        lead_data['phone'],
        lead_data['company'],
        lead_data['source'],
        lead_data['status'],
        lead_data['notes'],
        lead_data['created_date'],
        lead_data['created_time'],
        lead_data.get('follow_up_date'),
        lead_data.get('value', 0)
    ))
    
    lead_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return lead_id

def get_all_leads():
    """Retrieve all leads from database"""
    conn = sqlite3.connect('crm_data.db')
    try:
        df = pd.read_sql_query("SELECT * FROM leads ORDER BY created_date DESC", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def update_lead_status(lead_id, new_status):
    """Update lead status in database"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE leads SET status = ? WHERE id = ?", (new_status, lead_id))
    
    # Add activity log
    cursor.execute('''
        INSERT INTO activities (lead_id, activity_type, description, activity_date, activity_time)
        VALUES (?, ?, ?, ?, ?)
    ''', (lead_id, 'Status Update', f'Status changed to {new_status}', str(date.today()), datetime.now().strftime("%H:%M:%S")))
    
    conn.commit()
    conn.close()

def update_lead(lead_id, lead_data):
    """Update complete lead information"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        UPDATE leads SET name=?, email=?, phone=?, company=?, source=?, status=?, notes=?, follow_up_date=?, value=?
        WHERE id=?
    ''', (
        lead_data['name'],
        lead_data['email'],
        lead_data['phone'],
        lead_data['company'],
        lead_data['source'],
        lead_data['status'],
        lead_data['notes'],
        lead_data.get('follow_up_date'),
        lead_data.get('value', 0),
        lead_id
    ))
    
    conn.commit()
    conn.close()

def delete_lead(lead_id):
    """Delete lead from database"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    cursor.execute("DELETE FROM activities WHERE lead_id = ?", (lead_id,))
    conn.commit()
    conn.close()

def get_lead_by_id(lead_id):
    """Get specific lead by ID"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
    lead = cursor.fetchone()
    conn.close()
    return lead

# Database operations for customers
def save_customer_to_db(customer_data):
    """Save customer to database"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO customers (name, email, phone, company, address, status, notes, created_date, created_time, customer_value)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        customer_data['name'],
        customer_data['email'],
        customer_data['phone'],
        customer_data['company'],
        customer_data['address'],
        customer_data['status'],
        customer_data['notes'],
        customer_data['created_date'],
        customer_data['created_time'],
        customer_data.get('customer_value', 0)
    ))
    
    customer_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return customer_id

def get_all_customers():
    """Retrieve all customers from database"""
    conn = sqlite3.connect('crm_data.db')
    try:
        df = pd.read_sql_query("SELECT * FROM customers ORDER BY created_date DESC", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def delete_customer(customer_id):
    """Delete customer from database"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM customers WHERE id = ?", (customer_id,))
    cursor.execute("DELETE FROM activities WHERE customer_id = ?", (customer_id,))
    conn.commit()
    conn.close()

# Activity logging
def log_activity(lead_id=None, customer_id=None, activity_type="", description=""):
    """Log an activity"""
    conn = sqlite3.connect('crm_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO activities (lead_id, customer_id, activity_type, description, activity_date, activity_time)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (lead_id, customer_id, activity_type, description, str(date.today()), datetime.now().strftime("%H:%M:%S")))
    
    conn.commit()
    conn.close()

def get_recent_activities(limit=10):
    """Get recent activities"""
    conn = sqlite3.connect('crm_data.db')
    try:
        df = pd.read_sql_query("""
            SELECT a.*, l.name as lead_name, c.name as customer_name
            FROM activities a
            LEFT JOIN leads l ON a.lead_id = l.id
            LEFT JOIN customers c ON a.customer_id = c.id
            ORDER BY a.activity_date DESC, a.activity_time DESC
            LIMIT ?
        """, conn, params=(limit,))
    except:
        df = pd.DataFrame()
    conn.close()
    return df

# Initialize database
init_database()

# Custom CSS for better styling
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #1E88E5;
    text-align: center;
    margin-bottom: 2rem;
}
.metric-card {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    padding: 1rem;
    border-radius: 10px;
    color: white;
    text-align: center;
}
.sidebar-info {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 10px;
    margin-top: 1rem;
}
</style>
""", unsafe_allow_html=True)

# Sidebar navigation
st.sidebar.title("🏢 CRM System")
st.sidebar.markdown("---")

page = st.sidebar.selectbox(
    "📍 Navigate to:",
    ["🏠 Dashboard", "➕ Add Lead", "👥 View Leads", "👤 Add Customer", "🏢 View Customers", "📊 Analytics", "🔧 Data Management", "📋 Activities"]
)

st.sidebar.markdown("---")

# Quick stats in sidebar
try:
    leads_df = get_all_leads()
    customers_df = get_all_customers()
    
    st.sidebar.markdown("### 📈 Quick Stats")
    st.sidebar.metric("Total Leads", len(leads_df))
    st.sidebar.metric("Total Customers", len(customers_df))
    
    if not leads_df.empty:
        new_leads = len(leads_df[leads_df['status'] == 'New'])
        st.sidebar.metric("New Leads", new_leads)
except:
    st.sidebar.error("Database connection issue")

# Main content area
if page == "🏠 Dashboard":
    st.markdown('<h1 class="main-header">📊 CRM Dashboard</h1>', unsafe_allow_html=True)
    
    # Get current data
    leads_df = get_all_leads()
    customers_df = get_all_customers()
    activities_df = get_recent_activities(5)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_leads = len(leads_df)
        st.metric("📋 Total Leads", total_leads)
    
    with col2:
        new_leads = len(leads_df[leads_df['status'] == 'New']) if not leads_df.empty else 0
        st.metric("🆕 New Leads", new_leads)
    
    with col3:
        total_customers = len(customers_df)
        st.metric("👥 Total Customers", total_customers)
    
    with col4:
        converted = len(leads_df[leads_df['status'] == 'Converted']) if not leads_df.empty else 0
        conversion_rate = round((converted / total_leads * 100), 1) if total_leads > 0 else 0
        st.metric("🎯 Conversion Rate", f"{conversion_rate}%")
    
    # Charts and recent data
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Recent Leads")
        if not leads_df.empty:
            recent_leads = leads_df.head(5)[['name', 'company', 'status', 'created_date']]
            st.dataframe(recent_leads, use_container_width=True, hide_index=True)
        else:
            st.info("No leads found. Add your first lead!")
    
    with col2:
        st.subheader("🔄 Recent Activities")
        if not activities_df.empty:
            for _, activity in activities_df.iterrows():
                name = activity['lead_name'] if activity['lead_name'] else activity['customer_name']
                st.write(f"• **{activity['activity_type']}** - {name}: {activity['description']}")
        else:
            st.info("No recent activities")
    
    # Lead status distribution
    if not leads_df.empty:
        st.subheader("📊 Lead Status Distribution")
        status_counts = leads_df['status'].value_counts()
        fig = px.pie(values=status_counts.values, names=status_counts.index, 
                    title="Lead Status Distribution")
        st.plotly_chart(fig, use_container_width=True)

elif page == "➕ Add Lead":
    st.title("➕ Add New Lead")
    
    with st.form("lead_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("Full Name *", placeholder="Enter lead's full name")
            email = st.text_input("Email", placeholder="example@email.com")
            phone = st.text_input("Phone", placeholder="+1234567890")
            company = st.text_input("Company", placeholder="Company name")
        
        with col2:
            source = st.selectbox("Lead Source", 
                                ["Website", "Social Media", "Referral", "Cold Call", 
                                "Email Campaign", "Trade Show", "Advertisement", "Other"])
            status = st.selectbox("Status", 
                                ["New", "Contacted", "Qualified", "Proposal", "Converted", "Lost"])
            follow_up_date = st.date_input("Follow-up Date", value=None)
            value = st.number_input("Potential Value ($)", min_value=0.0, format="%.2f")
        
        notes = st.text_area("Notes", placeholder="Additional notes about this lead...")
        
        submitted = st.form_submit_button("💾 Save Lead", type="primary")
        
        if submitted:
            if name.strip():
                lead_data = {
                    'name': name.strip(),
                    'email': email.strip(),
                    'phone': phone.strip(),
                    'company': company.strip(),
                    'source': source,
                    'status': status,
                    'notes': notes.strip(),
                    'created_date': str(date.today()),
                    'created_time': datetime.now().strftime("%H:%M:%S"),
                    'follow_up_date': str(follow_up_date) if follow_up_date else None,
                    'value': value
                }
                
                try:
                    lead_id = save_lead_to_db(lead_data)
                    log_activity(lead_id=lead_id, activity_type="Lead Created", 
                               description=f"New lead '{name}' added to CRM")
                    st.success(f"✅ Lead '{name}' has been saved successfully!")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Error saving lead: {str(e)}")
            else:
                st.error("❌ Name is required!")

elif page == "👥 View Leads":
    st.title("👥 All Leads")
    
    leads_df = get_all_leads()
    
    if not leads_df.empty:
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_filter = st.selectbox("🔍 Filter by Status", ["All"] + list(leads_df['status'].unique()))
        
        with col2:
            source_filter = st.selectbox("📍 Filter by Source", ["All"] + list(leads_df['source'].unique()))
        
        with col3:
            search_term = st.text_input("🔎 Search", placeholder="Name/Company...")
        
        with col4:
            sort_by = st.selectbox("📋 Sort by", ["Created Date", "Name", "Company", "Value"])
        
        # Apply filters
        filtered_df = leads_df.copy()
        
        if status_filter != "All":
            filtered_df = filtered_df[filtered_df['status'] == status_filter]
        
        if source_filter != "All":
            filtered_df = filtered_df[filtered_df['source'] == source_filter]
        
        if search_term:
            filtered_df = filtered_df[
                filtered_df['name'].str.contains(search_term, case=False, na=False) |
                filtered_df['company'].str.contains(search_term, case=False, na=False)
            ]
        
        # Sort data
        if sort_by == "Name":
            filtered_df = filtered_df.sort_values('name')
        elif sort_by == "Company":
            filtered_df = filtered_df.sort_values('company')
        elif sort_by == "Value":
            filtered_df = filtered_df.sort_values('value', ascending=False)
        
        st.subheader(f"📊 Showing {len(filtered_df)} of {len(leads_df)} leads")
        
        # Display leads
        for index, row in filtered_df.iterrows():
            with st.expander(f"👤 {row['name']} - {row['company']} ({row['status']}) - ${row.get('value', 0):,.2f}"):
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.write(f"**📧 Email:** {row['email']}")
                    st.write(f"**📱 Phone:** {row['phone']}")
                    st.write(f"**🏢 Company:** {row['company']}")
                    st.write(f"**📍 Source:** {row['source']}")
                
                with col2:
                    st.write(f"**📅 Created:** {row['created_date']}")
                    if row.get('follow_up_date'):
                        st.write(f"**⏰ Follow-up:** {row['follow_up_date']}")
                    st.write(f"**💰 Value:** ${row.get('value', 0):,.2f}")
                    if row.get('notes'):
                        st.write(f"**📝 Notes:** {row['notes']}")
                
                with col3:
                    # Status update
                    new_status = st.selectbox(
                        "Update Status",
                        ["New", "Contacted", "Qualified", "Proposal", "Converted", "Lost"],
                        index=["New", "Contacted", "Qualified", "Proposal", "Converted", "Lost"].index(row['status']),
                        key=f"status_{row['id']}"
                    )
                    
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("✏️ Update", key=f"update_{row['id']}", type="primary"):
                            if new_status != row['status']:
                                update_lead_status(row['id'], new_status)
                                st.success("✅ Updated!")
                                st.rerun()
                    
                    with col_b:
                        if st.button("🗑️ Delete", key=f"delete_{row['id']}", type="secondary"):
                            delete_lead(row['id'])
                            st.success("🗑️ Deleted!")
                            st.rerun()
        
        # Export functionality
        st.subheader("📤 Export Data")
        col1, col2 = st.columns(2)
        with col1:
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                type="primary"
            )
        
        with col2:
            if st.button("🔄 Refresh Data"):
                st.rerun()
    
    else:
        st.info("🔍 No leads found. Add your first lead to get started!")
        if st.button("➕ Add First Lead"):
            st.switch_page("Add Lead")

elif page == "👤 Add Customer":
    st.title("👤 Add New Customer")
    
    with st.form("customer_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            name = st.text_input("Customer Name *", placeholder="Enter customer's name")
            email = st.text_input("Email", placeholder="customer@email.com")
            phone = st.text_input("Phone", placeholder="+1234567890")
            company = st.text_input("Company", placeholder="Company name")
        
        with col2:
            status = st.selectbox("Status", ["Active", "Inactive", "Pending", "VIP"])
            customer_value = st.number_input("Customer Value ($)", min_value=0.0, format="%.2f")
        
        address = st.text_area("Address", placeholder="Customer's address...")
        notes = st.text_area("Notes", placeholder="Additional notes...")
        
        submitted = st.form_submit_button("💾 Save Customer", type="primary")
        
        if submitted:
            if name.strip():
                customer_data = {
                    'name': name.strip(),
                    'email': email.strip(),
                    'phone': phone.strip(),
                    'company': company.strip(),
                    'address': address.strip(),
                    'status': status,
                    'notes': notes.strip(),
                    'created_date': str(date.today()),
                    'created_time': datetime.now().strftime("%H:%M:%S"),
                    'customer_value': customer_value
                }
                
                try:
                    customer_id = save_customer_to_db(customer_data)
                    log_activity(customer_id=customer_id, activity_type="Customer Added", 
                               description=f"New customer '{name}' added to CRM")
                    st.success(f"✅ Customer '{name}' has been saved successfully!")
                    st.balloons()
                except Exception as e:
                    st.error(f"❌ Error saving customer: {str(e)}")
            else:
                st.error("❌ Name is required!")

elif page == "🏢 View Customers":
    st.title("🏢 All Customers")
    
    customers_df = get_all_customers()
    
    if not customers_df.empty:
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.selectbox("🔍 Filter by Status", ["All"] + list(customers_df['status'].unique()))
        
        with col2:
            search_term = st.text_input("🔎 Search", placeholder="Name/Company...")
        
        with col3:
            sort_by = st.selectbox("📋 Sort by", ["Created Date", "Name", "Company", "Value"])
        
        # Apply filters
        filtered_df = customers_df.copy()
        
        if status_filter != "All":
            filtered_df = filtered_df[filtered_df['status'] == status_filter]
        
        if search_term:
            filtered_df = filtered_df[
                filtered_df['name'].str.contains(search_term, case=False, na=False) |
                filtered_df['company'].str.contains(search_term, case=False, na=False)
            ]
        
        st.subheader(f"📊 Showing {len(filtered_df)} of {len(customers_df)} customers")
        
        # Display in a more compact table format
        display_df = filtered_df[['name', 'email', 'phone', 'company', 'status', 'customer_value', 'created_date']].copy()
        display_df['customer_value'] = display_df['customer_value'].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        # Detailed view
        if st.toggle("Show Detailed View"):
            for index, row in filtered_df.iterrows():
                with st.expander(f"👤 {row['name']} - {row['company']} ({row['status']})"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**📧 Email:** {row['email']}")
                        st.write(f"**📱 Phone:** {row['phone']}")
                        st.write(f"**🏢 Company:** {row['company']}")
                        st.write(f"**📍 Address:** {row['address']}")
                        st.write(f"**💰 Value:** ${row.get('customer_value', 0):,.2f}")
                        st.write(f"**📅 Created:** {row['created_date']}")
                        if row.get('notes'):
                            st.write(f"**📝 Notes:** {row['notes']}")
                    
                    with col2:
                        if st.button("🗑️ Delete", key=f"delete_customer_{row['id']}", type="secondary"):
                            delete_customer(row['id'])
                            st.success("🗑️ Customer deleted!")
                            st.rerun()
        
        # Export
        st.subheader("📤 Export")
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Customers CSV",
            data=csv,
            file_name=f"customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    else:
        st.info("🔍 No customers found. Add your first customer!")

elif page == "📊 Analytics":
    st.title("📊 Analytics Dashboard")
    
    leads_df = get_all_leads()
    customers_df = get_all_customers()
    
    if not leads_df.empty or not customers_df.empty:
        
        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_leads = len(leads_df)
            st.metric("📋 Total Leads", total_leads)
        
        with col2:
            total_value = leads_df['value'].sum() if not leads_df.empty else 0
            st.metric("💰 Total Lead Value", f"${total_value:,.2f}")
        
        with col3:
            converted_leads = len(leads_df[leads_df['status'] == 'Converted']) if not leads_df.empty else 0
            st.metric("🎯 Converted Leads", converted_leads)
        
        with col4:
            conversion_rate = (converted_leads / total_leads * 100) if total_leads > 0 else 0
            st.metric("📈 Conversion Rate", f"{conversion_rate:.1f}%")
        
        if not leads_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Lead status distribution
                st.subheader("📊 Lead Status Distribution")
                status_counts = leads_df['status'].value_counts()
                fig_pie = px.pie(values=status_counts.values, names=status_counts.index)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                # Lead source analysis
                st.subheader("📍 Lead Sources")
                source_counts = leads_df['source'].value_counts()
                fig_bar = px.bar(x=source_counts.index, y=source_counts.values)
                fig_bar.update_layout(xaxis_title="Source", yaxis_title="Count")
                st.plotly_chart(fig_bar, use_container_width=True)
            
            # Timeline analysis
            st.subheader("📅 Leads Over Time")
            leads_df['created_date'] = pd.to_datetime(leads_df['created_date'])
            daily_leads = leads_df.groupby(leads_df['created_date'].dt.date).size().reset_index()
            daily_leads.columns = ['Date', 'Count']
            
            fig_line = px.line(daily_leads, x='Date', y='Count', title='Daily Lead Creation')
            st.plotly_chart(fig_line, use_container_width=True)
            
            # Value analysis
            if leads_df['value'].sum() > 0:
                st.subheader("💰 Lead Value Analysis")
                col1, col2 = st.columns(2)
                
                with col1:
                    # Value by status
                    value_by_status = leads_df.groupby('status')['value'].sum().reset_index()
                    fig_value = px.bar(value_by_status, x='status', y='value', 
                                     title='Total Value by Lead Status')
                    st.plotly_chart(fig_value, use_container_width=True)
                
                with col2:
                    # Top leads by value
                    st.write("**🏆 Top Leads by Value**")
                    top_leads = leads_df.nlargest(5, 'value')[['name', 'company', 'value', 'status']]
                    for _, lead in top_leads.iterrows():
                        st.write(f"• {lead['name']} ({lead['company']}): ${lead['value']:,.2f} - {lead['status']}")
    
    else:
        st.info("📊 No data available for analytics. Add some leads and customers first!")

elif page == "🔧 Data Management":
    st.title("🔧 Data Management")
    
    leads_df = get_all_leads()
    customers_df = get_all_customers()
    activities_df = get_recent_activities(50)
    
    # Database stats
    st.subheader("📊 Database Statistics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📋 Total Leads", len(leads_df))
    
    with col2:
        st.metric("👥 Total Customers", len(customers_df))
    
    with col3:
        st.metric("🔄 Total Activities", len(activities_df))
    
    # Data status
    st.subheader("💾 Data Persistence Status")
    st.success("✅ SQLite Database - All data is permanently stored!")
    st.info("🔒 Your data persists across sessions, browser refreshes, and app restarts!")
    
    # Database file info
    try:
        db_size = os.path.getsize('crm_data.db') / 1024  # Size in KB
        st.write(f"📁 Database file size: {db_size:.2f} KB")
    except:
        st.write("📁 Database file: crm_data.db")
    
    # Export all data
    st.subheader("📤 Export Data")
    col1, col2 = st.columns(2)
    
    with col1:
        if not leads_df.empty:
            leads_csv = leads_df.to_csv(index=False)
            st.download_button(
                label="📥 Export All Leads",
                data=leads_csv,
                file_name=f"all_leads_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                type="primary"
            )
        else:
            st.info("No leads to export")
    
    with col2:
        if not customers_df.empty:
            customers_csv = customers_df.to_csv(index=False)
            st.download_button(
                label="📥 Export All Customers",
                data=customers_csv,
                file_name=f"all_customers_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                type="primary"
            )
        else:
            st.info("No customers to export")
    
    # Import data
    st.subheader("📥 Import Data")
    st.warning("⚠️ Importing will add to existing data, not replace it.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Import Leads**")
        leads_file = st.file_uploader("Upload Leads CSV", type=['csv'], key="leads_import")
        if leads_file is not None:
            try:
                import_df = pd.read_csv(leads_file)
                st.write("Preview:")
                st.dataframe(import_df.head(), use_container_width=True)
                
                if st.button("Import Leads Data", type="primary"):
                    imported_count = 0
                    for _, row in import_df.iterrows():
                        try:
                            lead_data = {
                                'name': str(row.get('name', '')),
                                'email': str(row.get('email', '')),
                                'phone': str(row.get('phone', '')),
                                'company': str(row.get('company', '')),
                                'source': str(row.get('source', 'Import')),
                                'status': str(row.get('status', 'New')),
                                'notes': str(row.get('notes', '')),
                                'created_date': str(row.get('created_date', date.today())),
                                'created_time': str(row.get('created_time', datetime.now().strftime("%H:%M:%S"))),
                                'follow_up_date': str(row.get('follow_up_date', '')) if row.get('follow_up_date') else None,
                                'value': float(row.get('value', 0))
                            }
                            if lead_data['name'].strip():
                                save_lead_to_db(lead_data)
                                imported_count += 1
                        except Exception as e:
                            st.error(f"Error importing row: {e}")
                    
                    st.success(f"✅ Imported {imported_count} leads successfully!")
                    st.rerun()
                        
            except Exception as e:
                st.error(f"Error reading CSV file: {str(e)}")
    
    with col2:
        st.write("**Import Customers**")
        customers_file = st.file_uploader("Upload Customers CSV", type=['csv'], key="customers_import")
        if customers_file is not None:
            try:
                import_df = pd.read_csv(customers_file)
                st.write("Preview:")
                st.dataframe(import_df.head(), use_container_width=True)
                
                if st.button("Import Customers Data", type="primary"):
                    imported_count = 0
                    for _, row in import_df.iterrows():
                        try:
                            customer_data = {
                                'name': str(row.get('name', '')),
                                'email': str(row.get('email', '')),
                                'phone': str(row.get('phone', '')),
                                'company': str(row.get('company', '')),
                                'address': str(row.get('address', '')),
                                'status': str(row.get('status', 'Active')),
                                'notes': str(row.get('notes', '')),
                                'created_date': str(row.get('created_date', date.today())),
                                'created_time': str(row.get('created_time', datetime.now().strftime("%H:%M:%S"))),
                                'customer_value': float(row.get('customer_value', 0))
                            }
                            if customer_data['name'].strip():
                                save_customer_to_db(customer_data)
                                imported_count += 1
                        except Exception as e:
                            st.error(f"Error importing row: {e}")
                    
                    st.success(f"✅ Imported {imported_count} customers successfully!")
                    st.rerun()
                        
            except Exception as e:
                st.error(f"Error reading CSV file: {str(e)}")
    
    # Database maintenance
    st.subheader("🔧 Database Maintenance")
    st.info("Your SQLite database is self-maintaining. No manual maintenance required.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Refresh All Data", type="secondary"):
            st.success("✅ Data refreshed!")
            st.rerun()
    
    with col2:
        if st.button("📊 Check Database Integrity", type="secondary"):
            try:
                conn = sqlite3.connect('crm_data.db')
                cursor = conn.cursor()
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                conn.close()
                
                if result[0] == 'ok':
                    st.success("✅ Database integrity check passed!")
                else:
                    st.warning(f"⚠️ Database issues found: {result[0]}")
            except Exception as e:
                st.error(f"❌ Error checking database: {str(e)}")

elif page == "📋 Activities":
    st.title("📋 Activity Log")
    
    activities_df = get_recent_activities(100)
    
    if not activities_df.empty:
        st.subheader(f"📊 Recent Activities ({len(activities_df)} total)")
        
        # Filter activities
        col1, col2, col3 = st.columns(3)
        
        with col1:
            activity_filter = st.selectbox("Filter by Type", 
                                         ["All"] + list(activities_df['activity_type'].unique()))
        
        with col2:
            date_filter = st.date_input("Filter by Date", value=None)
        
        with col3:
            limit = st.selectbox("Show entries", [10, 25, 50, 100], index=1)
        
        # Apply filters
        filtered_activities = activities_df.copy()
        
        if activity_filter != "All":
            filtered_activities = filtered_activities[filtered_activities['activity_type'] == activity_filter]
        
        if date_filter:
            filtered_activities = filtered_activities[filtered_activities['activity_date'] == str(date_filter)]
        
        filtered_activities = filtered_activities.head(limit)
        
        # Display activities
        for _, activity in filtered_activities.iterrows():
            name = activity['lead_name'] if activity['lead_name'] else activity['customer_name']
            entity_type = "Lead" if activity['lead_name'] else "Customer"
            
            with st.container():
                st.markdown(f"""
                <div style="border-left: 4px solid #1f77b4; padding-left: 12px; margin: 8px 0;">
                    <strong>{activity['activity_type']}</strong> - {entity_type}: {name}<br>
                    <small>{activity['description']}</small><br>
                    <small style="color: #666;">📅 {activity['activity_date']} at {activity['activity_time']}</small>
                </div>
                """, unsafe_allow_html=True)
        
        # Export activities
        if st.button("📥 Export Activity Log"):
            csv = filtered_activities.to_csv(index=False)
            st.download_button(
                label="Download Activity Log CSV",
                data=csv,
                file_name=f"activity_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    else:
        st.info("📋 No activities recorded yet. Start adding leads and customers!")

# Sidebar footer
st.sidebar.markdown("---")
st.sidebar.markdown("""
<div class="sidebar-info">
<h4>💾 Data Status</h4>
<p>✅ <strong>Persistent Storage</strong><br>
🔒 <strong>SQLite Database</strong><br>
🚀 <strong>Always Available</strong></p>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# Add some spacing at the bottom
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 20px;">
    <p>🏢 <strong>Enhanced CRM System</strong> | Built with Streamlit | Data Persistence Guaranteed</p>
</div>
""", unsafe_allow_html=True)
