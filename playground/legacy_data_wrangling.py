import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def extract_flight_records_to_ndjson(
    input_path: str,
    output_path: str
) -> None:
    """
    Extracts flight records from a nested JSON file and writes them to a newline-delimited JSON (NDJSON) file.
    
    This function is designed to handle flight data in the format:
    {
        "pagination": {...},
        "data": [ {...}, {...}, ... ]
    }
    
    It reads the file at `input_path`, extracts the list under the "data" key, and writes each dictionary
    (representing one flight) as a single line in the output NDJSON file at `output_path`. This format is suitable 
    for processing in distributed systems like Apache Spark.

    Parameters:
    -----------
    input_path : str or os.PathLike
        Path to the input JSON file containing the raw nested flight data.

    output_path : str or os.PathLike
        Path where the output NDJSON file will be written.

    Raises:
    -------
    FileNotFoundError:
        If the input file does not exist.

    KeyError:
        If the input JSON does not contain a "data" key.

    JSONDecodeError:
        If the input file is not a valid JSON.

    Examples:
    ---------
    >>> extract_flight_records_to_ndjson("raw_data/flights_raw_data.json", "raw_data/flattened_flight_data.json")
    """

    # Read the full JSON content
    with open(input_path, "r", encoding="utf-8") as f:
        full_data = json.load(f)

    # Ensure "data" key exists
    if "data" not in full_data or not isinstance(full_data["data"], list):
        raise KeyError("Input JSON does not contain a 'data' array.")

    flight_data = full_data["data"]

    # Write each flight record to the output file as a single line
    with open(output_path, "w", encoding="utf-8") as f:
        for record in flight_data:
            f.write(json.dumps(record) + "\n")

def ndjson_to_clean_flattened_parquet(
    json_path: str, 
    output_path: str,
    csv_output_path: str = "clean data/flattened_flight_data_csv", 
    app_name: str = "ndjson_to_clean_parquet"
) -> None:
    """
    Load a newline-delimited JSON file containing flight data into a PySpark DataFrame,
    flatten nested fields into top-level columns,
    and save the flattened DataFrame to a specified output path in Parquet format.

    Parameters:
    ----------
    json_path : str
        The file path to the newline-delimited JSON file.
    output_path : str
        The directory path where the flattened DataFrame will be saved as a parquet file.
    csv_output_path : str, optional
        Path to the directory where the CSV file will be saved (default is "clean data/flattened_flight_data_csv").
    app_name : str, optional
        The name of the Spark application (default is "ndjson_to_clean_parquet").

    Returns:
    -------
    None
        Saves the flattened DataFrame to the output path in parquet format and a 
        copy in CSV format is created, returns nothing.
    """
    # Initialize SparkSession
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    try:
        # Read JSON file into a DataFrame
        df = spark.read.json(json_path)

        # Flatten nested fields explicitly
        flat_df = df.select(
            "flight_date",
            "flight_status",
            "aircraft",
            "live",

            # Airline struct
            col("airline.name").alias("airline_name"),
            col("airline.iata").alias("airline_iata"),
            col("airline.icao").alias("airline_icao"),

            # Departure struct
            col("departure.airport").alias("departure_airport"),
            col("departure.timezone").alias("departure_timezone"),
            col("departure.iata").alias("departure_iata"),
            col("departure.icao").alias("departure_icao"),
            col("departure.terminal").alias("departure_terminal"),
            col("departure.gate").alias("departure_gate"),
            col("departure.delay").alias("departure_delay"),
            col("departure.scheduled").alias("departure_scheduled"),
            col("departure.estimated").alias("departure_estimated"),
            col("departure.actual").alias("departure_actual"),
            col("departure.estimated_runway").alias("departure_estimated_runway"),
            col("departure.actual_runway").alias("departure_actual_runway"),

            # Arrival struct
            col("arrival.airport").alias("arrival_airport"),
            col("arrival.timezone").alias("arrival_timezone"),
            col("arrival.iata").alias("arrival_iata"),
            col("arrival.icao").alias("arrival_icao"),
            col("arrival.terminal").alias("arrival_terminal"),
            col("arrival.gate").alias("arrival_gate"),
            col("arrival.baggage").alias("arrival_baggage"),
            col("arrival.delay").alias("arrival_delay"),
            col("arrival.scheduled").alias("arrival_scheduled"),
            col("arrival.estimated").alias("arrival_estimated"),
            col("arrival.actual").alias("arrival_actual"),
            col("arrival.estimated_runway").alias("arrival_estimated_runway"),
            col("arrival.actual_runway").alias("arrival_actual_runway"),

            # Flight struct
            col("flight.number").alias("flight_number"),
            col("flight.iata").alias("flight_iata"),
            col("flight.icao").alias("flight_icao"),

            # Codeshared sub-struct
            col("flight.codeshared.airline_name").alias("codeshared_airline_name"),
            col("flight.codeshared.airline_iata").alias("codeshared_airline_iata"),
            col("flight.codeshared.airline_icao").alias("codeshared_airline_icao"),
            col("flight.codeshared.flight_number").alias("codeshared_flight_number"),
            col("flight.codeshared.flight_iata").alias("codeshared_flight_iata"),
            col("flight.codeshared.flight_icao").alias("codeshared_flight_icao"),
        )

        # Save flattened DataFrame as Parquet
        flat_df.write.mode("overwrite").parquet(output_path)
        
        # Save flattened DataFrame as CSV
        flat_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(csv_output_path)

    except Exception as e:
        print(f"Error processing flight data from {json_path}: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    try:
        input_file = "raw data/flights_raw_data.json"
        output_file = "raw data/flattened_flight_data.json"
        extract_flight_records_to_ndjson(input_file, output_file)
        print(f"Extracted records from {input_file} and wrote to {output_file} after flattening.\n")
        json_path = "raw data/flattened_flight_data.json"
        output_path = "clean data/flattened_flight_data_parquet"
        ndjson_to_clean_flattened_parquet(json_path, output_path)
        print(f"Flattened data saved successfully at: {output_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
        