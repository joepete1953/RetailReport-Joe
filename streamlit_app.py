import streamlit as st
import pandas as pd
import psycopg2
import re
import bcrypt
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime

# -------------------------- Load secrets / env --------------------------
load_dotenv()
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

# -------------------------- Streamlit setup --------------------------
st.set_page_config(
    page_title="RetailX Ultra Studio",
    page_icon="🛍️",
    layout="wide"
)

# -------------------------- Base CSS (global) --------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'Poppins', sans-serif !important;
}

/* HEADER STYLING */
.ultra-header {
    background: linear-gradient(135deg, #7c3aed, #4c1d95, #db2777, #f59e0b);
    background-size: 350% 350%;
    animation: gradientMove 9s ease infinite;
    padding: 42px;
    border-radius: 20px;
    color: white;
    margin-bottom: 25px;
}
@keyframes gradientMove {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

/* GLASS CARD */
.glass-card {
    background: rgba(255,255,255,0.14);
    padding: 24px;
    border-radius: 18px;
    backdrop-filter: blur(8px);
    margin-bottom: 20px;
}

/* BUTTONS */
.stButton > button {
    background: linear-gradient(90deg, #6d28d9, #db2777);
    color: #fff;
    border: none;
    padding: 10px 22px;
    border-radius: 10px;
    font-weight: 600;
    transition: 0.15s;
}
.stButton > button:hover {
    opacity: 0.95;
    transform: scale(1.02);
}

/* HISTORY CARD */
.history-item {
    background: rgba(255,255,255,0.12);
    padding: 15px;
    border-radius: 12px;
    margin-bottom: 12px;
}
</style>
""", unsafe_allow_html=True)

# ======================================================================
#                           RETAIL DB SCHEMA
# ======================================================================
DATABASE_SCHEMA = """
Retail Database (Simplified)

Dimensions:
- Region(regionid, region)
- Country(countryid, country, regionid)
- ProductCategory(productcategoryid, productcategory, productcategorydescription)
- Customer(customerid, firstname, lastname, address, city, countryid)
- Product(productid, productname, productunitprice, productcategoryid)

Fact:
- OrderDetail(orderid, customerid, productid, orderdate, quantityordered)
"""

# ======================================================================
#                           LOGIN SCREEN (THEME A)
# ======================================================================
def login_screen():

    # REMOVE Streamlit default padding + grey top bar
    st.markdown("""
    <style>

    /* Remove Streamlit top header */
    header[data-testid="stHeader"] {
        height: 0 !important;
        background: transparent !important;
    }

    /* Remove top decoration bar */
    div[data-testid="stDecoration"] {
        display: none !important;
    }

    /* Remove padding inside main container */
    .block-container {
        padding-top: 0 !important;
        margin-top: 0 !important;
    }

    /* Remove view container padding */
    main[data-testid="stAppViewContainer"] {
        padding-top: 0 !important;
        margin-top: 0 !important;
    }

    /* Remove the unwanted grey block */
    div[data-testid="stAppViewBlockContainer"] {
        padding-top: 0 !important;
        margin-top: -80px !important;   /* key line */
    }

    </style>
    """, unsafe_allow_html=True)

    # Neon bubble cinematic CSS
    st.markdown("""
    <style>

    body {
        background: radial-gradient(circle at top left, #0f0f23, #08080f, #000000);
        overflow: hidden;
    }

    /* Floating bubbles */
    .bubble {
        position: absolute;
        border-radius: 50%;
        background: rgba(138, 43, 226, 0.25);
        box-shadow: 0 0 18px rgba(138, 43, 226, 0.45);
        animation: floatUp 14s infinite ease-in;
    }

    @keyframes floatUp {
        0% { transform: translateY(0) scale(0.9); opacity: 0.8; }
        50% { transform: translateY(-400px) scale(1.1); opacity: 0.5; }
        100% { transform: translateY(-800px) scale(0.8); opacity: 0; }
    }

    .bubble:nth-child(1) { left: 5%; width: 80px; height: 80px; animation-duration: 13s; }
    .bubble:nth-child(2) { left: 22%; width: 60px; height: 60px; animation-duration: 11s; }
    .bubble:nth-child(3) { left: 48%; width: 120px; height: 120px; animation-duration: 15s; }
    .bubble:nth-child(4) { left: 72%; width: 90px; height: 90px; animation-duration: 10s; }
    .bubble:nth-child(5) { left: 88%; width: 70px; height: 70px; animation-duration: 16s; }

    /* LOGIN CARD */
    .login-box {
        width: 430px;
        padding: 38px;
        margin: 12vh auto;
        background: rgba(255,255,255,0.15);
        border-radius: 18px;
        backdrop-filter: blur(18px);
        box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        animation: fadeIn 1.2s ease;
    }

    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(14px); }
        to { opacity: 1; transform: translateY(0); }
    }

    .login-title {
        text-align: center;
        font-size: 32px;
        font-weight: 700;
        color: white;
        margin-bottom: 10px;
    }

    .login-sub {
        text-align: center;
        color: #d4d4d4;
        margin-bottom: 28px;
        font-size: 15px;
    }

    .logo-glow {
        width: 90px;
        height: 90px;
        border-radius: 50%;
        margin: auto;
        display: flex;
        justify-content: center;
        align-items: center;
        font-size: 42px;
        color: white;
        background: radial-gradient(circle, #db2777, #7c3aed);
        box-shadow: 0 0 32px #db2777aa;
        margin-bottom: 18px;
        animation: pulse 2.5s infinite ease-in-out;
    }

    @keyframes pulse {
        0% { transform: scale(1); box-shadow: 0 0 28px #db2777aa; }
        50% { transform: scale(1.06); box-shadow: 0 0 40px #7c3aedcc; }
        100% { transform: scale(1); box-shadow: 0 0 28px #db2777aa; }
    }

    </style>
    """, unsafe_allow_html=True)

    # Bubble layer
    st.markdown("""
    <div class="bubble"></div>
    <div class="bubble"></div>
    <div class="bubble"></div>
    <div class="bubble"></div>
    <div class="bubble"></div>
    """, unsafe_allow_html=True)

    # Login box
    st.markdown("<div class='login-box'>", unsafe_allow_html=True)

    st.markdown("""
        <div class="logo-glow">🛍️</div>
        <div class="login-title">RetailX Login</div>
        <div class="login-sub">Enter your password to continue</div>
    """, unsafe_allow_html=True)

    pwd = st.text_input("Password", type="password")

    if st.button("Login"):
        if pwd and bcrypt.checkpw(pwd.encode(), HASHED_PASSWORD):
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("Incorrect password.")

    st.markdown("</div>", unsafe_allow_html=True)

# ======================================================================
#                         LOGIN GUARD (REQUIRED)
# ======================================================================
def require_login():
    """Force login before accessing the app."""
    if not st.session_state.get("logged_in", False):
        login_screen()
        st.stop()

# ======================================================================
#                        DB + OPENAI HELPERS (unchanged)
# ======================================================================
@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(
            f"postgresql://{st.secrets['POSTGRES_USERNAME']}:{st.secrets['POSTGRES_PASSWORD']}@"
            f"{st.secrets['POSTGRES_SERVER']}/{st.secrets['POSTGRES_DATABASE']}"
        )
        return conn
    except Exception as e:
        st.error(f"DB Connection Failed: {e}")
        return None

def run_query(sql):
    conn = get_db_connection()
    try:
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        st.error(f"Query Error: {e}")
        return None

@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)

def extract_sql(text):
    return re.sub(r"```sql|```", "", text).strip()

def generate_sql(q):
    prompt = f"""
Retail database:

{DATABASE_SCHEMA}

Question: {q}

Rules:
- Return ONLY SQL
- Use Customer→Country→Region joins
- Convert orderdate using TO_DATE(orderdate::text,'YYYYMMDD')
- Limit to 100 if large
"""

    try:
        out = get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        return extract_sql(out.choices[0].message.content)
    except:
        return None

def generate_insights(df):
    prompt = f"Summarize useful retail insights:\n{df.head().to_string(index=False)}"
    try:
        out = get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.25,
        )
        return out.choices[0].message.content
    except:
        return "No insights."

# ======================================================================
#                             MAIN APP
# ======================================================================
def main():
    require_login()

    if "history" not in st.session_state:
        st.session_state.history = []

    # HEADER
    st.markdown("""
        <div class='ultra-header'>
            <h1 style='margin-bottom:6px;'>RetailX Ultra Studio</h1>
            <p style='margin-top:-6px;'>AI-powered retail analytics that feel magical.</p>
        </div>
    """, unsafe_allow_html=True)

    # Sidebar navigation + Suggested prompts
    st.sidebar.title("📌 Navigation")
    menu = st.sidebar.radio(
        "",
        ["🏠 Dashboard", "🤖 Ask AI", "💻 SQL Editor", "📜 History", "📘 Schema", "🚪 Logout"]
    )

    st.sidebar.markdown("---")
    st.sidebar.title("💡 Suggested Prompts")
    suggestions = [
        "What is total revenue by region?",
        "Which products generate the highest sales?",
        "Show total orders per country.",
        "Top 10 customers by spending.",
        "Monthly revenue trend.",
        "Which product categories sell best?",
    ]
    for s in suggestions:
        if st.sidebar.button(s):
            st.session_state.pre_fill = s

    # ---------------------- Dashboard ----------------------
    if menu == "🏠 Dashboard":
        st.markdown("### 📊 Overview")

        summary_sql = """
        SELECT
            SUM(p.productunitprice * o.quantityordered) AS revenue,
            COUNT(*) AS total_orders,
            COUNT(DISTINCT o.customerid) AS customers
        FROM orderdetail o
        JOIN product p ON p.productid = o.productid;
        """
        df = run_query(summary_sql)
        if df is not None:
            col1, col2, col3 = st.columns(3)
            col1.metric("Revenue", f"${df['revenue'][0]:,.2f}")
            col2.metric("Orders", df['total_orders'][0])
            col3.metric("Customers", df['customers'][0])

        st.markdown("---")

        region_sql = """
        SELECT r.region,
               SUM(p.productunitprice * o.quantityordered) AS revenue
        FROM orderdetail o
        JOIN customer c ON c.customerid = o.customerid
        JOIN country co ON co.countryid = c.countryid
        JOIN region r ON r.regionid = co.regionid
        JOIN product p ON p.productid = o.productid
        GROUP BY r.region
        ORDER BY revenue DESC;
        """
        region_df = run_query(region_sql)
        if region_df is not None:
            st.write("#### Revenue by Region")
            st.bar_chart(region_df.set_index("region"))

    # ---------------------- Ask AI ----------------------
    if menu == "🤖 Ask AI":
        st.markdown("### 🤖 Ask anything about your Retail data")

        q = st.text_input("Your question:", value=st.session_state.get("pre_fill", ""))

        if st.button("Generate SQL"):
            sql = generate_sql(q)
            if sql:
                st.code(sql, language="sql")
                st.session_state.generated_sql = sql
                st.session_state.latest_question = q

        if st.session_state.get("generated_sql"):
            sq = st.text_area("Edit SQL before running:", st.session_state.generated_sql, height=200)

            if st.button("Run Query"):
                df = run_query(sq)
                if df is not None:

                    # Add to history
                    st.session_state.history.append({
                        "question": st.session_state.latest_question,
                        "sql": sq,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "rows": len(df)
                    })

                    st.success(f"Returned {len(df)} rows.")
                    st.dataframe(df)

                    # Auto chart
                    nums = df.select_dtypes(["float", "int"]).columns
                    if len(nums) > 0:
                        st.line_chart(df[nums])

                    # Insights
                    st.write("#### Insights")
                    st.info(generate_insights(df))

    # ---------------------- SQL Editor ----------------------
    if menu == "💻 SQL Editor":
        st.markdown("### ✏️ SQL Editor")
        sql_raw = st.text_area("Write your SQL:", height=200)
        if st.button("Run"):
            df = run_query(sql_raw)
            if df is not None:
                st.dataframe(df)

    # ---------------------- History ----------------------
    if menu == "📜 History":
        st.markdown("### 📜 Query History")
        if len(st.session_state.history) == 0:
            st.info("No history yet.")
        else:
            for item in reversed(st.session_state.history):
                st.markdown(f"""
                <div class='history-item'>
                    <b>{item['timestamp']}</b><br>
                    <b>Question:</b> {item['question']}<br>
                    <b>Rows:</b> {item['rows']}
                </div>
                """, unsafe_allow_html=True)

                if st.button(f"Re-run {item['timestamp']}", key=item['timestamp']):
                    df = run_query(item["sql"])
                    if df is not None:
                        st.dataframe(df)

    # ---------------------- Schema ----------------------
    if menu == "📘 Schema":
        st.markdown("### 📘 Retail Database Schema")
        st.code(DATABASE_SCHEMA)

    # ---------------------- Logout ----------------------
    if menu == "🚪 Logout":
        st.session_state.logged_in = False
        st.rerun()


if __name__ == "__main__":
    main()
