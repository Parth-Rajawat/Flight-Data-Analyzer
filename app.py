import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
# ---------------- Sidebar Styling ----------------
st.markdown(
    """
    <style>
    /* Sidebar background */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f2027, #203a43, #2c5364);
        color: white;
    }

    /* Sidebar title */
    [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, [data-testid="stSidebar"] h1 {
        color: #f8f9fa;
        font-family: 'Helvetica Neue', sans-serif;
        font-weight: 600;
    }

    /* Sidebar radio buttons */
    .stRadio > div {
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }

    .stRadio label {
        background: rgba(255, 255, 255, 0.1);
        padding: 8px 16px;
        border-radius: 8px;
        transition: all 0.3s ease;
        color: white;
        font-weight: 500;
    }

    .stRadio label:hover {
        background: rgba(255, 255, 255, 0.3);
        cursor: pointer;
    }

    /* Info box text */
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
    "user": "root",         # 🔹 your MySQL username
    "password": "password", # 🔹 your MySQL password
    "database": "flight_db"
}

def load_data(table_name):
    """Fetch a table from MySQL into a Pandas DataFrame."""
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
st.set_page_config(page_title="Flight Data Dashboard", layout="wide")
st.title("✈️ Flight Data Analytics Dashboard")

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/e/e0/Airplane_icon.svg", width=100)
    st.markdown("## ✈️ Flight Analytics Dashboard")
    st.markdown("---")

    # Reports section
    st.markdown("### 📊 Reports")
    report_menu = st.radio(
        "Select Report",
        [
            "Flight Status Counts",
            "Flight Status Trend",
            "Top 3 Busiest Airports (arrival + departure) in past 10 days",
            "Top 3 Busiest Arrival Airports in past 10 days",
            "Top 3 Busiest Departure Airports in past 10 days"
        ]
    )

    st.markdown("---")

    # Prediction section
    st.markdown("### 🤖 Prediction")
    pred_menu = st.radio(
        "Choose Task",
        ["Flight Delay Prediction"]
    )

    st.markdown("---")
    st.markdown("### ℹ️ About")
    st.info("Monitor flights, detect busiest airports, and predict delays using Spark ML 🚀")


# ------------------------------------------------------
# 3. Visualizations
# ------------------------------------------------------

# ---- Flight Status Counts ----
if report_menu == "Flight Status Counts":
    df = load_data("flight_status_counts")
    st.subheader("📊 Flight Status Counts")
    st.dataframe(df.head(50))

    fig = px.pie(df,
                 names="flight_status",
                 values="count",
                 title="Flight Status Distribution")
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
elif report_menu == "Flight Status Trend":
    df = load_data("flight_status_trend")
    st.subheader("📈 Flight Status Trend Over Time")
    st.dataframe(df.head(50))

    fig = px.line(df,
                  x="flight_date",
                  y="count",
                  color="flight_status",
                  markers=True,
                  title="Flight Status Trend by Date")
    st.plotly_chart(fig, use_container_width=True)
    st.info(
    "📈 This line chart highlights how each flight status has changed over time. "
    "Peaks in *delayed* flights may indicate operational disruptions such as weather events."
    )

# ---- Top 3 Busiest Airports Daily ----
elif report_menu == "Top 3 Busiest Airports (arrival + departure) in past 10 days":
    df = load_data("top_3_busiest_airports_daily")
    st.subheader("🏆 Top 3 Busiest Airports (arrival + departure) in past 10 days")
    st.dataframe(df.head(50))

    fig = px.bar(df, x="date", y="total_traffic", color="airport_code",
                 title="Daily Top 3 Busiest Airports")
    st.plotly_chart(fig, use_container_width=True)

    
    # Find busiest airport overall
    max_row = df.loc[df["total_traffic"].idxmax()]
    busiest_airport = max_row["airport_code"]
    busiest_value = max_row["total_traffic"]
    st.markdown(
    "🏆 **These are the busiest airports by traffic (departure + arrival) on each day.** "
    "busiest airport overall is highlighted below."
    )
    st.success(f"🏆 **Busiest Airport Overall:** {busiest_airport} with {busiest_value:,} flights")

# ---- Top 3 Busiest Arrival Airports Daily ----
elif report_menu == "Top 3 Busiest Arrival Airports in past 10 days":
    df = load_data("top_3_busiest_arrival_airports_daily")
    st.subheader("🛬 Top 3 Busiest Arrival Airports in past 10 days")
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
    
    st.success(
    "🛬 The bar chart shows daily arrival traffic for top airports. "
    "The pie chart highlights their share **on the most recent day**."
    )
    max_row = df.loc[df["arrival_count"].idxmax()]
    busiest_airport = max_row["airport_code"]
    busiest_value = max_row["arrival_count"]
    st.info(f"🛬 **Top Arrival Hub:** {busiest_airport} with {busiest_value:,} arrivals")


# ---- Top 3 Busiest Departure Airports Daily ----
elif report_menu == "Top 3 Busiest Departure Airports in past 10 days":
    df = load_data("top_3_busiest_departure_airports_daily")
    st.subheader("🛫 Top 3 Busiest Departure Airports in past 10 days")
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
    
    st.success(
    "🛫 Here’s the departure traffic split across top airports. "
    "Helps spot dominant hubs."
    )
    max_row = df.loc[df["departure_count"].idxmax()]
    busiest_airport = max_row["airport_code"]
    busiest_value = max_row["departure_count"]
    st.info(f"🛫 **Top Departure Hub:** {busiest_airport} with {busiest_value:,} departures")

# Prediction
if pred_menu == "Flight Delay Prediction":
    spark = SparkSession.builder.appName("FlightDelayApp").getOrCreate()
    st.title("✈️ Flight Delay Prediction")
    st.markdown("Fill in flight details below to predict delay likelihood.")


    # User inputs
    airline = st.selectbox("Airline IATA Code", ["AA", "UA", "DL", "SW", "BA"])
    airports = ["JFK", "LAX", "ORD", "ATL", "DFW", "SFO", "MIA", "SEA", "BOS", "DEN", "PHX"]
    dep_airport = st.selectbox("Departure Airport", airports)
    arr_airport = st.selectbox("Arrival Airport", airports)
    aircraft = st.selectbox("Aircraft Model", ["A320", "B737", "B777", "A380"])


    sched_hour = st.slider("Scheduled Hour (0-23)", 0, 23, 15)
    sched_dayofweek = st.slider("Day of Week (1=Mon)", 1, 7, 3)
    sched_dayofmonth = st.slider("Day of Month", 1, 31, 10)
    sched_month = st.slider("Month", 1, 12, 9)
    sched_weekofyear = st.slider("Week of Year", 1, 52, 36)


    if st.button("Predict Delay"):
    # Build input DataFrame
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

        # Run prediction
        prediction = model.transform(input_df).select("prediction", "probability").collect()[0]
        predicted_label = "Delayed" if prediction["prediction"] == 1 else "On-time"
        delay_prob = float(prediction["probability"][1])

        st.subheader("Prediction Result")
        st.success(f"Prediction: **{predicted_label}**")
        st.metric("Probability of Delay", f"{delay_prob:.2%}")
