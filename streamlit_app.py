import streamlit as st
import pandas as pd
import psycopg2
import re
import bcrypt
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime


# ================================
# Load secrets / env
# ================================
load_dotenv()
OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"].encode("utf-8")

st.set_page_config(
    page_title="RetailX Ultra Studio",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# ================================
# Global Styles (fixes gradient + removes top empty bar)
# ================================
st.markdown("""
<style>

html, body, [class*="css"] {
    font-family: 'Poppins', sans-serif;
}

/* FORCE override Streamlit dark theme so gradient shows */
#root, .block-container, .main, html, body {
    background: transparent !important;
}

/* remove the ugly top padding Streamlit adds */
.block-container {
    padding-top: 0 !important;
}

/* Fullscreen animated gradient */
body {
    background: linear-gradient(-45deg, #ff9a9e, #fad0c4, #fbc2eb, #a18cd1);
    background-size: 600% 600% !important;
    animation: gradientBG 18s ease infinite;
}

@keyframes gradientBG {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

/* Style login card */
.login-wrapper {
    margin-top: 12vh;
    display: flex;
    justify-content: center;
}

.welcome-card {
    width: 480px;
    padding: 34px;
    border-radius: 22px;
    background: rgba(255,255,255,0.25);
    backdrop-filter: blur(14px);
    box-shadow: 0 8px 35px rgba(0,0,0,0.22);
    animation: fadeIn 1.0s ease;
}

@keyframes fadeIn {
    from { opacity: 0; transform: translateY(16px); }
    to { opacity: 1; transform: translateY(0); }
}

/* nice round logo */
.logo-circle {
    width: 90px;
    height: 90px;
    background: linear-gradient(135deg, #a855f7, #ec4899);
    border-radius: 50%;
    margin: auto;
    display: flex;
    justify-content: center;
    align-items: center;
    color: white;
    font-size: 36px;
    box-shadow: 0 6px 20px rgba(0,0,0,0.22);
    margin-bottom: 18px;
}

/* Login button */
.stButton > button {
    background: linear-gradient(90deg, #7c3aed, #d946ef);
    color: white;
    border-radius: 10px !important;
    padding: 10px 20px;
    width: 100%;
    font-weight: 600;
    border: none;
    box-shadow: 0 4px 14px rgba(124,58,237,0.45);
    transition: 0.18s ease;
}

.stButton > button:hover {
    transform: scale(1.04);
}

</style>
""", unsafe_allow_html=True)



# ================================
# Retail DB Schema
# ================================
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


# ================================
# LOGIN SCREEN  (Fully fixed)
# ================================
def login_screen():
    st.markdown("<div class='login-wrapper'><div class='welcome-card'>", unsafe_allow_html=True)

    st.markdown("""
        <div class='logo-circle'>🛍️</div>
        <h2 style='text-align:center; margin-bottom:6px;'>Welcome to RetailX</h2>
        <p style='text-align:center; margin-top:-4px; color:#333;'>Your AI-powered retail analytics studio</p>
    """, unsafe_allow_html=True)

    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if password and bcrypt.checkpw(password.encode(), HASHED_PASSWORD):
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("Incorrect password. Try again.")

    st.markdown("</div></div>", unsafe_allow_html=True)



def require_login():
    if not st.session_state.get("logged_in", False):
        login_screen()
        st.stop()



# ================================
# DB Helpers
# ================================
@st.cache_resource
def get_db_connection():
    try:
        return psycopg2.connect(
            f"postgresql://{st.secrets['POSTGRES_USERNAME']}:{st.secrets['POSTGRES_PASSWORD']}@"
            f"{st.secrets['POSTGRES_SERVER']}/{st.secrets['POSTGRES_DATABASE']}"
        )
    except Exception as e:
        st.error(f"Could not connect: {e}")
        return None


def run_query(sql):
    try:
        return pd.read_sql_query(sql, get_db_connection())
    except Exception as e:
        st.error(f"Query failed: {e}")
        return None


# ================================
# OpenAI logic
# ================================
@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)


def extract_sql(text):
    return re.sub(r"```sql|```", "", text).strip()


def generate_sql(question):
    prompt = f"""
Schema:
{DATABASE_SCHEMA}

Generate SQL only. 
Rules:
- Use proper joins
- Convert date with: TO_DATE(orderdate::text, 'YYYYMMDD')
- LIMIT 100 for large queries

Question:
{question}
"""

    try:
        r = get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        return extract_sql(r.choices[0].message.content)
    except:
        return None



def generate_insights(df):
    prompt = f"""
Summarize these retail results into simple insights.

Data:
{df.head().to_string(index=False)}
"""
    try:
        r = get_openai_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        return r.choices[0].message.content
    except:
        return "Could not generate insights."


# ================================
# MAIN APP
# ================================
def main():
    require_login()

    if "history" not in st.session_state:
        st.session_state.history = []

    st.markdown("""
        <div class='ultra-header'>
            <h1 style='margin-bottom:6px;'>RetailX Ultra Studio</h1>
            <p style='margin-top:-6px;'>AI-assisted retail analytics for insights that matter.</p>
        </div>
    """, unsafe_allow_html=True)

    menu = st.sidebar.radio(
        "Menu",
        ["🏠 Dashboard", "🤖 Ask AI", "💻 SQL Editor", "📜 History", "📘 Schema", "🚪 Logout"]
    )


    # ========== Dashboard ==========
    if menu == "🏠 Dashboard":
        st.write("### 📊 Overview")

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
            c1, c2, c3 = st.columns(3)
            c1.metric("Revenue", f"${df['revenue'][0]:,.2f}")
            c2.metric("Orders", df['total_orders'][0])
            c3.metric("Customers", df['customers'][0])


    # ========== AI ==========
    if menu == "🤖 Ask AI":
        st.write("### 💬 Ask a question about your retail data")
        question = st.text_input("Ask something like: *Which region makes the most revenue?*")

        if st.button("Generate SQL"):
            sql = generate_sql(question)
            if sql:
                st.code(sql, language="sql")
                st.session_state.generated_sql = sql
                st.session_state.latest_question = question

        if st.session_state.get("generated_sql"):
            sql_text = st.text_area("Edit SQL:", st.session_state.generated_sql, height=180)

            if st.button("Run Query"):
                df = run_query(sql_text)
                if df is not None:
                    # Save history
                    st.session_state.history.append({
                        "question": st.session_state.latest_question,
                        "sql": sql_text,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "rows": len(df)
                    })

                    st.success(f"Returned {len(df)} rows.")
                    st.dataframe(df)

                    num_cols = df.select_dtypes(["float", "int"]).columns
                    if len(num_cols) > 0:
                        st.write("#### Auto Chart")
                        st.line_chart(df[num_cols])

                    st.write("#### AI Insights")
                    st.info(generate_insights(df))


    # ========== SQL Editor ==========
    if menu == "💻 SQL Editor":
        st.write("### ✏️ SQL Editor")
        sql_text = st.text_area("Write SQL here:", height=200)
        if st.button("Run"):
            df = run_query(sql_text)
            if df is not None:
                st.dataframe(df)


    # ========== History ==========
    if menu == "📜 History":
        st.write("### 📜 Recent Queries")
        if len(st.session_state.history) == 0:
            st.info("No history yet.")
        else:
            for h in reversed(st.session_state.history):
                st.markdown(f"""
                <div class='history-item'>
                    <b>{h['timestamp']}</b><br>
                    <b>Question:</b> {h['question']}<br>
                    <b>Rows:</b> {h['rows']}
                </div>
                """, unsafe_allow_html=True)


    # ========== Schema ==========
    if menu == "📘 Schema":
        st.write("### 📘 Retail Database Schema")
        st.code(DATABASE_SCHEMA)


    # ========== Logout ==========
    if menu == "🚪 Logout":
        st.session_state.logged_in = False
        st.rerun()


if __name__ == "__main__":
    main()
