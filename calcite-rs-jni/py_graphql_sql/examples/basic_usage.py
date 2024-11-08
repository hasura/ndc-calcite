"""Example usage of the DB-API implementation."""
import os

from py_graphql_sql import connect


def main() -> None:
    """Basic example of connecting to a database and executing queries."""
    # Connection parameters
    host = "http://localhost:3000/graphql"
    jdbc_args = {"role": "admin"}

    # Create connection using context manager
    with connect(host, jdbc_args) as conn:
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
