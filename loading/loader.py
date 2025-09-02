from pyspark.sql import SparkSession, DataFrame

# ------------------------------------------------------
# 1. Start Spark Session (no need for .config if jar is in pyspark/jars)
# ------------------------------------------------------
spark = SparkSession.builder \
    .appName("PySpark-MySQL") \
    .getOrCreate()

# ------------------------------------------------------
# 2. Function to write PySpark DF to MySQL
# ------------------------------------------------------
def write_to_mysql(
    df: DataFrame,
    table_name: str,
    host: str = "localhost",
    port: int = 3306,
    database: str = "flight_db",    # 🔹 schema used in "flight database connection"
    user: str = "root",
    password: str = "Ancestors@1",     # 🔹 replace with your actual MySQL password
    mode: str = "overwrite"
):
    """
    Write a PySpark DataFrame to a MySQL database.
    
    Args:
        df (DataFrame): PySpark DataFrame to write.
        table_name (str): Destination table name in MySQL.
        host (str): MySQL host (default: localhost).
        port (int): MySQL port (default: 3306).
        database (str): MySQL schema/database.
        user (str): MySQL username.
        password (str): MySQL password.
        mode (str): "overwrite", "append", "ignore", or "error".
    """
    mysql_url = f"jdbc:mysql://{host}:{port}/{database}"
    mysql_properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    df.write.jdbc(
        url=mysql_url,
        table=table_name,
        mode=mode,
        properties=mysql_properties
    )
    
    print(f"✅ Data written to MySQL ({host}:{port}/{database}) → Table: {table_name}")


# ------------------------------------------------------
# 3. Example Usage
# ------------------------------------------------------

# Assume these are your transformed DataFrames:
# df1 = <PySpark DF for Live Airport Traffic>
# df2 = <PySpark DF for Flight Delay Explorer>
# df3 = <PySpark DF for Aircraft Utilization>
# df4 = <PySpark DF for Codeshare Insights>

# Example: write them into MySQL under "flight_db" schema
# (same as Workbench "flight database connection")

# write_to_mysql(df1, "live_airport_traffic", password="yourpass")
# write_to_mysql(df2, "flight_delay_explorer", password="yourpass")
# write_to_mysql(df3, "aircraft_utilization", password="yourpass")
# write_to_mysql(df4, "codeshare_insights", password="yourpass")
