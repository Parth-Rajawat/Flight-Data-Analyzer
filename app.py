import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# ---------------- Sidebar + Input Styling ----------------
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f2027, #203a43, #2c5364);
        color: white;
    }
    [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, [data-testid="stSidebar"] h1 {
        color: #f8f9fa;
        font-family: 'Helvetica Neue', sans-serif;
        font-weight: 600;
    }
    /* Sidebar radio (Reports vs Prediction) */
    .stRadio > div {
        display: flex;
        flex-direction: column;
        gap: 0.6rem;
    }
    .stRadio label {
        background: rgba(255, 255, 255, 0.08);
        padding: 10px 18px;
        border-radius: 12px;
        transition: all 0.3s ease;
        color: #f8f9fa;
        font-weight: 500;
        font-size: 15px;
        border: 1px solid rgba(255,255,255,0.2);
    }
    .stRadio label:hover {
        background: rgba(255, 255, 255, 0.25);
        transform: translateX(4px);
        cursor: pointer;
    }
    /* Sexy selectboxes */
    div[data-baseweb="select"] > div {
        background-color: rgba(255,255,255,0.1);
        border-radius: 10px;
        border: 1px solid rgba(255,255,255,0.3);
    }
    div[data-baseweb="select"] span {
        color: #f8f9fa !important;
    }
    .stAlert {
        font-size: 14px;
        font-weight: 400;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ------------------------------------------------------
# 1. DB Connection
# ------------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "password",
    "database": "flight_db"
}

def load_data(table_name):
    conn = pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"]
    )
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

# ------------------------------------------------------
# 2. Streamlit Layout
# ------------------------------------------------------
st.set_page_config(page_title="Flight Analytics Dashboard", layout="wide")
st.title("✈️ Flight Reports Dashboard and Delay Predictor")

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/e/e0/Airplane_icon.svg", width=100)
    st.markdown("## ✈️ Flight Analytics Dashboard")
    st.markdown("---")

    section = st.radio(
        "select section",
        options = ["📊 Reports", "🤖 Flight Delay Prediction"],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("### ℹ️ About")
    st.info("Monitor flights, detect busiest airports, and predict delays using Spark ML 🚀")

# ------------------------------------------------------
# 3. Section Rendering
# ------------------------------------------------------

# ---------------- Reports ----------------
if section == "📊 Reports":
    st.subheader("📊 Reports Section")

    # ✅ Force horizontal report menu
    st.markdown("""
    <style>
    div[data-testid="stRadio"] > div {
        flex-direction: row;
        justify-content: center;
        gap: 1rem;
    }         
    </style>
    """, unsafe_allow_html=True)

    report_menu = st.radio(
        "Select a report",
        options = [
            "📊 Flight Status Counts",
            "📈 Flight Status Trend",
            "🏆 Busiest Airports (arrival + departure) in last 10 days",
            "🛬 Busiest Arrivals in last 10 days",
            "🛫 Busiest Departures in last 10 days"
        ],
        label_visibility="collapsed"
    )

    # ---- Flight Status Counts ----
    if report_menu == "📊 Flight Status Counts":
        df = load_data("flight_status_counts")
        st.dataframe(df.head(50))
        fig = px.pie(df, names="flight_status", values="count", title="Flight Status Distribution")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(
        """
        🟢 **Interpretation**: This chart shows the proportion of flights by their status.  
        - *Delayed*: The flights that have been delayed.   
        - *Cancelled*: Not operational.  
        - *Landed*: Successfully completed.  

        👉 A healthy system should have **most flights in 'landed'** categories.
        """
        )

    # ---- Flight Status Trend ----
    elif report_menu == "📈 Flight Status Trend":
        df = load_data("flight_status_trend")
        st.dataframe(df.head(50))
        fig = px.line(df, x="flight_date", y="count", color="flight_status",
                      markers=True, title="Flight Status Trend by Date")
        st.plotly_chart(fig, use_container_width=True)
        st.info("📈 This line chart highlights how each flight status has changed over time. "
                "Peaks in *delayed* flights may indicate operational disruptions such as weather events.")

    # ---- Top 3 Busiest Airports ----
    elif report_menu == "🏆 Busiest Airports (arrival + departure) in last 10 days":
        df = load_data("top_3_busiest_airports_daily")
        st.dataframe(df.head(50))
        fig = px.bar(df, x="date", y="total_traffic", color="airport_code",
                     title="Daily Top 3 Busiest Airports")
        st.plotly_chart(fig, use_container_width=True)
        max_row = df.loc[df["total_traffic"].idxmax()]
        st.markdown(
            """
            🏆 **Interpretation**: This chart highlights the busiest hubs by combining both **departures and arrivals** over the past 10 days.  
            - Airports with consistently high traffic volumes serve as **major national or international hubs**.  
            - Spikes in total traffic may indicate **special events, seasonal travel surges, or operational expansions**.  

            👉 Monitoring these hubs helps airlines optimize **crew allocation, gate management, and ground services**.
            """
        )
        st.success(f"🏆 **Busiest Airport Overall:** {max_row['airport_code']} "
                   f"with {max_row['total_traffic']:,} flights")

    # ---- Top 3 Busiest Arrival Airports ----
    elif report_menu == "🛬 Busiest Arrivals in last 10 days":
        df = load_data("top_3_busiest_arrival_airports_daily")
        st.dataframe(df.head(50))
        col1, col2 = st.columns(2)
        with col1:
            fig_bar = px.bar(df, x="date", y="arrival_count", color="airport_code",
                             title="Daily Top 3 Arrival Airports (Bar Chart)")
            st.plotly_chart(fig_bar, use_container_width=True)
        with col2:
            latest_date = df["date"].max()
            latest_df = df[df["date"] == latest_date]
            fig_pie = px.pie(latest_df, names="airport_code", values="arrival_count",
                             title=f"Arrival Share on {latest_date}")
            st.plotly_chart(fig_pie, use_container_width=True)
        max_row = df.loc[df["arrival_count"].idxmax()]
        st.markdown(
            """
            🛬 **Interpretation**: This view isolates **arrival traffic** across airports.  
            - A high arrival volume signals airports acting as **key destinations** — often tourist cities or major business centers.  
            - Comparing daily arrival volumes helps spot **travel demand trends**.  

            👉 Useful for understanding where **passenger demand is concentrated** and planning **airport terminal capacity**.
            """
        )

        st.success(f"🛬 **Top Arrival Hub:** {max_row['airport_code']} "
                   f"with {max_row['arrival_count']:,} arrivals")

    # ---- Top 3 Busiest Departure Airports ----
    elif report_menu == "🛫 Busiest Departures in last 10 days":
        df = load_data("top_3_busiest_departure_airports_daily")
        st.dataframe(df.head(50))
        col1, col2 = st.columns(2)
        with col1:
            fig_bar = px.bar(df, x="date", y="departure_count", color="airport_code",
                             title="Daily Top 3 Departure Airports (Bar Chart)")
            st.plotly_chart(fig_bar, use_container_width=True)
        with col2:
            latest_date = df["date"].max()
            latest_df = df[df["date"] == latest_date]
            fig_pie = px.pie(latest_df, names="airport_code", values="departure_count",
                             title=f"Departure Share on {latest_date}")
            st.plotly_chart(fig_pie, use_container_width=True)
        max_row = df.loc[df["departure_count"].idxmax()]
        st.markdown(
            """
            🛫 **Interpretation**: This chart highlights airports with the **highest outbound traffic**.  
            - These are often **originating hubs** for airlines, serving as bases of operation.  
            - Monitoring departure surges helps in **fleet utilization planning** and **slot scheduling**.  

            👉 Critical for tracking where airlines are focusing their **network strength**.
            """
        )

        st.success(f"🛫 **Top Departure Hub:** {max_row['airport_code']} "
                   f"with {max_row['departure_count']:,} departures")

# ---------------- Prediction ----------------
elif section == "🤖 Flight Delay Prediction":
    spark = SparkSession.builder.appName("FlightDelayApp").getOrCreate()
    model = PipelineModel.load("/Users/parthmac/Desktop/Projects/Flight Data Engineering/transformation for use cases/models/final_flight_delay_model")

    st.subheader("✈️ Flight Delay Prediction")
    st.markdown("Fill in flight details below to predict delay likelihood.")

    airline = st.selectbox("✈️ Airline IATA Code", ["AA", "UA", "DL", "SW", "BA"])
    airports = ["JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "MIA", "SEA", "BOS", "DEN", "PHX"]
    dep_airport = st.selectbox("🛫 Departure Airport", airports)
    arr_airport = st.selectbox("🛬 Arrival Airport", airports)
    aircraft = st.selectbox("🛩️ Aircraft Model", ["A320", "B737", "B777", "A380"])

    sched_hour = st.slider("⏰ Scheduled Hour (0-23)", 0, 23, 15)
    sched_dayofweek = st.slider("📅 Day of Week (1=Mon)", 1, 7, 3)
    sched_dayofmonth = st.slider("📅 Day of Month", 1, 31, 10)
    sched_month = st.slider("📅 Month", 1, 12, 9)
    sched_weekofyear = st.slider("📅 Week of Year", 1, 52, 36)

    if st.button("🚀 Predict Delay"):
        user_input = {
            "airline_iata": airline,
            "departure_airport": dep_airport,
            "arrival_airport": arr_airport,
            "aircraft_model": aircraft,
            "sched_hour": sched_hour,
            "sched_dayofweek": sched_dayofweek,
            "sched_dayofmonth": sched_dayofmonth,
            "sched_month": sched_month,
            "sched_weekofyear": sched_weekofyear
        }
        input_df = spark.createDataFrame([user_input])
        prediction = model.transform(input_df).select("prediction", "probability").collect()[0]
        predicted_label = "Delayed" if prediction["prediction"] == 1 else "On-time"
        delay_prob = float(prediction["probability"][1])

        st.subheader("Prediction Result")
        st.success(f"Prediction: **{predicted_label}**")
        st.metric("Probability of Delay", f"{delay_prob:.2%}")
