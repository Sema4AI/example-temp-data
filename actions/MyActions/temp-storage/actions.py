from sema4ai.actions import action, chat, Response, Request
import duckdb
import os
from pathlib import Path


DB_PATH = os.path.join("data", "customer_data.duckdb")


@action
def load_data(filename: str = "") -> Response[str]:
    """
    Loads customer classification data from CSV into DuckDB.

    Args:
        filename: name of the file to extract data from, can be absolute or just basename (from LLM), always get just basename

    Returns:
        A message indicating the data was loaded successfully, and the number of rows loaded.
        Also includes schema information and sample data.
    """

    # Get the basename of the filename
    orig_filename, filename = _access_file(filename)

    # Connect to a file-based DuckDB database
    con = duckdb.connect(database=DB_PATH)

    # Table name for the loaded data
    table_name = "customers"

    # Load the CSV file into a table called 'customers'
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{filename}')"
    )

    # Get the row count to confirm data was loaded
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    row_count = result[0]

    load_msg = f"Successfully loaded {row_count} rows from {orig_filename} into table '{table_name}' in DuckDB database at {DB_PATH}.\n\n"

    # Get schema information
    schema_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    columns = con.execute(schema_query).fetchall()

    # Build schema information string
    schema_info = "Schema Information:\n"
    schema_info += "=" * 50 + "\n"

    for col_name, data_type in columns:
        schema_info += f"Column: {col_name}, Type: {data_type}"

        # Check if column might be categorical (less than 10 unique values)
        distinct_values_query = f'SELECT COUNT(DISTINCT "{col_name}") FROM {table_name}'
        distinct_count = con.execute(distinct_values_query).fetchone()[0]

        if distinct_count < 10:
            # Get the possible values for categorical columns
            values_query = (
                f'SELECT DISTINCT "{col_name}" FROM {table_name} ORDER BY "{col_name}"'
            )
            values = con.execute(values_query).fetchall()
            values_list = [str(val[0]) for val in values]
            schema_info += f", Possible values: {', '.join(values_list)}"

        schema_info += "\n"

    # Get 5 sample rows
    sample_rows = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
    column_names = [col[0] for col in columns]

    # Format sample rows
    sample_info = "\nSample Data (5 rows):\n"
    sample_info += "=" * 50 + "\n"

    # Add column headers
    sample_info += " | ".join(column_names) + "\n"
    sample_info += "-" * 50 + "\n"

    # Add row data
    for row in sample_rows:
        sample_info += " | ".join(str(value) for value in row) + "\n"

    return Response(result=load_msg + schema_info + sample_info)


@action
def query(sql_query: str = "") -> Response[str]:
    """
    Execute a custom SQL query on the DuckDB database.

    Args:
        sql_query: The SQL query to execute. If empty, returns a list of all tables in the database.

    Returns:
        A string containing the query results or table list.
    """
    # Connect to the database
    con = duckdb.connect(database=DB_PATH)

    try:
        # If no query provided, list all tables in the database
        if not sql_query.strip():
            tables_query = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
            result = con.execute(tables_query).fetchall()

            output = "Database Tables:\n"
            output += "=" * 50 + "\n"

            if result:
                output += "TABLE_NAME | TABLE_TYPE\n"
                output += "-" * 50 + "\n"
                for row in result:
                    output += f"{row[0]} | {row[1]}\n"
            else:
                output += "No tables found in the database.\n"

            return Response(result=output)

        # Execute the provided query
        result = con.execute(sql_query).fetchall()

        # Get column names from the result
        column_names = [col[0] for col in con.description]

        # Format the result as a string
        output = "Query Results:\n"
        output += "=" * 50 + "\n"

        # Add query that was executed
        output += f"Executed query: {sql_query}\n\n"

        # Add row count
        output += f"Returned {len(result)} rows\n\n"

        if result:
            # Add column headers
            output += " | ".join(column_names) + "\n"
            output += "-" * 50 + "\n"

            # Add row data
            for row in result:
                output += " | ".join(str(value) for value in row) + "\n"
        else:
            output += "No rows returned.\n"

        return Response(result=output)
    except Exception as e:
        return Response(result=f"Error executing query: {str(e)}")


@action
def cleanup() -> Response[str]:
    """
    Completely removes the DuckDB database file from the filesystem.

    Returns:
        A message indicating the database file was removed successfully.
    """
    try:
        # First close any open connections
        con = duckdb.connect(database=DB_PATH)
        con.close()

        # Delete the database file if it exists
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            return Response(
                result=f"Database file at {DB_PATH} has been completely removed."
            )
        else:
            return Response(result=f"Database file at {DB_PATH} does not exist.")
    except Exception as e:
        return Response(result=f"Error removing database: {str(e)}")


@action
def return_my_thread_id(request: Request) -> Response[str]:
    """Returns the thread ID of the current chat thread.

    The `Request` object

    Args:
        request: The incoming request object provided by the framework.

    Returns:
        The thread ID of the current request.
    """
    thread_id = request.headers.get("X-INVOKED_FOR_THREAD_ID")
    return Response(result=f"Thread id = {thread_id}")


def _access_file(filename: str):
    """This is a helper function to access a file from the Sema4.ai File API (chat files).

    Args:
        filename: name of the file to extract data from, can be absolute or just basename (from LLM), always get just basename

    Returns:
        The original basename of the file and the temporary filename (stored in the Sema4.ai File API).
    """
    filepath = Path(filename)
    orig_basename = filepath.name
    try:
        # Get file from Sema4.ai File API, returns temporary filename
        temp_file: Path = chat.get_file(orig_basename)
        # Extract the directory path from temp_file
        temp_file_dir = temp_file.parent
        # Use the basename from filepath.name and append the suffix
        new_temp_file_name = f"{temp_file.stem}{filepath.suffix}"
        # Combine them to form the new path
        new_temp_file_path = temp_file_dir / new_temp_file_name
        # Rename temp_file
        temp_file = temp_file.rename(new_temp_file_path)
    except Exception as err:
        print(f"Error getting file from chat - using filename as is: {err}")
        temp_file = filename
    return orig_basename, temp_file
