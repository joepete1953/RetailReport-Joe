import streamlit as st
import pandas as pd
import psycopg2
import re
import bcrypt
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime

# -------------------------- Load secrets / env --------------------------
# Just pulling in credentials from Streamlit's secrets + .env
load_dotenv()
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

# -------------------------- Streamlit setup --------------------------
st.set_page_config(
    page_title="RetailX Ultra Studio",
    page_icon="🛍️",
    layout="wide"
)

# -------------------------- Styles (custom CSS) --------------------------
# Added a bunch of styling tweaks to make the UI look much better.
# Tried to keep it clean and readable instead of overly complicated.
st.markdown("""
<style>

@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'Poppins', sans-serif;
}

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

.glass-card {
    background: rgba(255,255,255,0.14);
    padding: 24px;
    border-radius: 18px;
    backdrop-filter: blur(8px);
    margin-bottom: 20px;
}

.stButton > button {
    background: linear-gradient(90deg, #6d28d9, #db2777);
    color: #fff;
    border: none;
    padding: 10px 22px;
    border-radius: 10px;
    font-weight: 600;
}
.stButton > button:hover {
    opacity: 0.95;
    transform: scale(1.02);
}

.history-item {
    background: rgba(255,255,255,0.12);
    padding: 15px;
    border-radius: 12px;
    margin-bottom: 12px;
}

</style>
""", unsafe_allow_html=True)


# -------------------------- Retail DB Schema --------------------------
# Left this simple since it's only for reference in the schema tab.
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


# -------------------------- Login Logic --------------------------
def login_screen():
    # Fullscreen background with animated gradient
    st.markdown("""
    <style>
    
    body {
        background: linear-gradient(-45deg, #ff9a9e, #fad0c4, #fad0c4, #fbc2eb, #a18cd1, #fbc2eb);
        background-size: 600% 600%;
        animation: gradientBG 18s ease infinite;
    }
    @keyframes gradientBG {
        0% {background-position: 0% 50%;}
        50% {background-position: 100% 50%;}
        100% {background-position: 0% 50%;}
    }

    .login-container {
        margin-top: 10vh;
        display: flex;
        justify-content: center;
    }

    .welcome-card {
        width: 480px;
        padding: 32px;
        border-radius: 18px;
        background: rgba(255,255,255,0.22);
        box-shadow: 0 8px 30px rgba(0,0,0,0.18);
        backdrop-filter: blur(12px);
        animation: fadeIn 1.2s ease;
    }

    @keyframes fadeIn {
        from {opacity: 0; transform: translateY(12px);}
        to {opacity: 1; transform: translateY(0);}
    }

    .title {
        text-align: center;
        font-size: 32px;
        font-weight: 700;
        padding-bottom: 10px;
    }

    .subtitle {
        text-align: center;
        font-size: 16px;
        color: #2d2d2d;
        padding-bottom: 28px;
    }

    .logo-circle {
        width: 80px;
        height: 80px;
        background: linear-gradient(135deg, #a855f7, #ec4899);
        border-radius: 50%;
        margin: auto;
        display: flex;
        justify-content: center;
        align-items: center;
        color: white;
        font-size: 34px;
        box-shadow: 0 6px 20px rgba(0,0,0,0.2);
        margin-bottom: 20px;
    }

    .stButton > button {
        background: linear-gradient(90deg, #7c3aed, #d946ef);
        border-radius: 10px !important;
        border: none;
        padding: 10px 22px;
        color: white;
        font-weight: 600;
        width: 100% !important;
        box-shadow: 0 4px 14px rgba(124, 58, 237, 0.45);
        transition: 0.15s ease;
    }
    .stButton > button:hover {
        transform: scale(1.035);
    }

    </style>
    """, unsafe_allow_html=True)

    st.markdown("<div class='login-container'><div class='welcome-card'>", unsafe_allow_html=True)

    st.markdown("""
        <div class='logo-circle'>🛍️</div>
        <div class='title'>Welcome to RetailX</div>
        <div class='subtitle'>Your AI-powered retail analytics studio</div>
    """, unsafe_allow_html=True)

    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if password and bcrypt.checkpw(password.encode(), HASHED_PASSWORD):
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("Incorrect password, try again.")

    st.markdown("</div></div>", unsafe_allow_html=True)



def require_login():
    if not st.session_state.get("logged_in", False):
        login_screen()
        st.stop()


# -------------------------- DB helpers --------------------------
@st.cache_resource
def get_db_connection():
    """Return a Postgres connection. Cached so it isn't recreated constantly."""
    try:
        conn = psycopg2.connect(
            f"postgresql://{st.secrets['POSTGRES_USERNAME']}:{st.secrets['POSTGRES_PASSWORD']}@"
            f"{st.secrets['POSTGRES_SERVER']}/{st.secrets['POSTGRES_DATABASE']}"
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


def run_query(sql):
    """Run SQL and return a DataFrame."""
    conn = get_db_connection()
    try:
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        st.error(f"Error running query: {e}")
        return None


# -------------------------- OpenAI helpers --------------------------
@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)


def extract_sql(text):
    """Strip ```sql blocks if model adds them."""
    return re.sub(r"```sql|```", "", text).strip()


def generate_sql(question):
    """Convert English question into SQL based on retail schema."""
    prompt = f"""
You are helping with SQL generation for a retail dataset.

Schema:
{DATABASE_SCHEMA}

User question:
{question}

Rules:
- Return ONLY SQL.
- Use correct joins between Customer → Country → Region.
- Convert orderdate: TO_DATE(orderdate::text, 'YYYYMMDD').
- Add LIMIT 100 for large queries.
"""

    try:
        resp = get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        return extract_sql(resp.choices[0].message.content)
    except Exception as e:
        st.error(f"OpenAI error: {e}")
        return None


def generate_insights(df):
    """Simple helper to let AI summarize a query result."""
    prompt = f"""
Please summarize this retail data into a few useful business insights.
Keep it short and practical.

Sample data:
{df.head().to_string(index=False)}
"""

    try:
        resp = get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.25,
        )
        return resp.choices[0].message.content
    except:
        return "Could not generate insights."


# -------------------------- Main App --------------------------
def main():
    require_login()

    # Initialize history storage
    if "history" not in st.session_state:
        st.session_state.history = []

    # Header
    st.markdown("""
        <div class='ultra-header'>
            <h1 style='margin-bottom:6px;'>RetailX Ultra Studio</h1>
            <p style='margin-top:-6px;'>AI-assisted retail analytics for insights that matter.</p>
        </div>
    """, unsafe_allow_html=True)

    # Sidebar navigation
    menu = st.sidebar.radio(
        "Navigation",
        ["🏠 Dashboard", "🤖 Ask AI", "💻 SQL Editor", "📜 History", "📘 Schema", "🚪 Logout"]
    )

    # ------------------ Dashboard ------------------
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

        # Region revenue
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

    # ------------------ Ask AI ------------------
    if menu == "🤖 Ask AI":
        st.markdown("### 💬 Ask a question about your retail data")

        question = st.text_input("Your question here…")

        if st.button("Generate SQL"):
            sql = generate_sql(question)
            if sql:
                st.code(sql, language="sql")
                st.session_state.generated_sql = sql
                st.session_state.latest_question = question

        if st.session_state.get("generated_sql"):
            sql_text = st.text_area("Edit SQL before running:", st.session_state.generated_sql, height=200)

            if st.button("Run Query"):
                df = run_query(sql_text)
                if df is not None:

                    # Store in history
                    st.session_state.history.append({
                        "question": st.session_state.latest_question,
                        "sql": sql_text,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "rows": len(df)
                    })

                    st.success(f"Query returned {len(df)} rows.")
                    st.dataframe(df)

                    # Auto chart if numeric columns exist
                    num_cols = df.select_dtypes(["float", "int"]).columns
                    if len(num_cols) > 0:
                        st.write("#### Auto Chart")
                        st.line_chart(df[num_cols])

                    # Insights
                    st.write("#### AI Insights")
                    st.info(generate_insights(df))

    # ------------------ SQL Editor ------------------
    if menu == "💻 SQL Editor":
        st.markdown("### ✏️ SQL Editor")
        sql_text = st.text_area("Write your SQL here:", height=200)

        if st.button("Run"):
            df = run_query(sql_text)
            if df is not None:
                st.dataframe(df)

    # ------------------ History ------------------
    if menu == "📜 History":
        st.markdown("### 📜 Recent Queries")

        if len(st.session_state.history) == 0:
            st.info("No query history yet.")
        else:
            for item in reversed(st.session_state.history):
                st.markdown(f"""
                <div class='history-item'>
                    <b>{item['timestamp']}</b><br>
                    <b>Question:</b> {item['question']}<br>
                    <b>Rows:</b> {item['rows']}
                </div>
                """, unsafe_allow_html=True)

                if st.button(f"Re-run", key=item["timestamp"]):
                    df = run_query(item["sql"])
                    if df is not None:
                        st.dataframe(df)

    # ------------------ Schema ------------------
    if menu == "📘 Schema":
        st.markdown("### 📘 Retail Database Schema")
        st.code(DATABASE_SCHEMA)

    # ------------------ Logout ------------------
    if menu == "🚪 Logout":
        st.session_state.logged_in = False
        st.rerun()


if __name__ == "__main__":
    main()

