"""Example usage of the DB-API implementation."""
import os

from py_graphql_sql import connect


def main() -> None:
    """Basic example of connecting to a database and executing queries."""
    # Connection parameters
    host = "http://localhost:3000/graphql"
    jdbc_args = {"role": "admin"}

    # Get paths to JAR directories
    current_dir = os.path.dirname(os.path.abspath(__file__))
    driver_paths = [
        os.path.abspath(
            os.path.join(current_dir, "../../jdbc/target")
        )  # Add additional paths as needed
    ]

    # Create connection using context manager
    with connect(host, jdbc_args, driver_paths) as conn:
        with conn.cursor() as cur:
            # Execute a query
            cur.execute("SELECT * FROM Albums", [])

            # Fetch all results
            rows = cur.fetchall()

            # Display all rows
            for row in rows:
                print(f"Result: {row}")


if __name__ == "__main__":
    main()
