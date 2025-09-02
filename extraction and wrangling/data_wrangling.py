import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

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
    >>> extract_flight_records_to_ndjson("raw data/flights_raw_data.json", "raw data/flattened_flight_data.json")
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

def flatten_struct_columns(df: DataFrame, parent: str = "") -> DataFrame:
    """
    Recursively flattens all nested StructType fields in a PySpark DataFrame.
    Resulting column names use underscore-separated paths (e.g., 'flight_codeshared_airline_iata').

    Parameters:
    ----------
    df : DataFrame
        The PySpark DataFrame to flatten.
    
    parent : str, optional
        Internal use: prefix used during recursive flattening (default is "").

    Returns:
    -------
    DataFrame
        A flattened DataFrame with all StructType columns expanded.
    """
    flat_cols = []

    def _flatten(schema: StructType, path: str = ""):
        for field in schema.fields:
            name = field.name
            dtype = field.dataType
            full_path = f"{path}.{name}" if path else name
            alias = full_path.replace(".", "_")

            if isinstance(dtype, StructType):
                _flatten(dtype, full_path)
            else:
                flat_cols.append(col(full_path).alias(alias))

    _flatten(df.schema)
    return df.select(flat_cols)

def ndjson_to_clean_flattened_parquet(
    json_path: str,
    parquet_output_path: str,
    csv_output_path: str = "data/wrangled data/flattened_flight_data_csv",
    app_name: str = "Flatten_NDJSON_Processor"
) -> None:
    """
    Loads a newline-delimited JSON (NDJSON) file, flattens all nested StructType fields,
    and saves the result as Parquet, creating a copy CSV.

    Parameters:
    ----------
    json_path : str
        Path to the NDJSON input file.
    
    parquet_output_path : str
        Directory where the flattened data will be saved in Parquet format.
    
    csv_output_path : str, optional
        Directory for saving the data in CSV format (default is 'clean data/flattened_flight_data_csv').
    
    app_name : str, optional
        Name of the Spark application (default is 'Flatten_NDJSON_Processor').

    Returns:
    -------
    None
        Outputs are written to disk; nothing is returned.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    try:
        # Read NDJSON into a DataFrame
        df = spark.read.json(json_path)

        # Flatten all nested StructType columns
        flat_df = flatten_struct_columns(df)

        # Save to Parquet
        flat_df.write.mode("overwrite").parquet(parquet_output_path)

        # Save to CSV
        flat_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(csv_output_path)

    except Exception as e:
        print(f"❌ Error processing file '{json_path}': {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    try:
        input_file = "data/raw data/flights_raw_data.json"
        output_file = "data/raw data/flattened_flight_data.json"
        extract_flight_records_to_ndjson(input_file, output_file)
        print(f"Extracted records from {input_file} and wrote to {output_file} after flattening.\n")
        json_path = "data/raw data/flattened_flight_data.json"
        output_path = "data/wrangled data/flattened_flight_data_parquet"
        ndjson_to_clean_flattened_parquet(json_path, output_path)
        print("NDJSON successfully flattened and saved to Parquet")
    except Exception as e:
        print(f"An error occurred: {e}")