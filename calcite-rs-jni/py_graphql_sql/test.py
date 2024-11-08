# test.py
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects import registry
import logging
from pprint import pprint

# Enable logging
logging.basicConfig(level=logging.DEBUG)

print("\n=== Testing Connection ===")

# Ensure dialect is registered
from py_graphql_sql.sqlalchemy.hasura.ddnbase import HasuraDDNDialect

url = 'hasura+graphql:///?url=http://localhost:3000/graphql'

try:
    print(f"\nCreating engine with URL: {url}")
    engine = create_engine(url, echo=True)
    print("Engine created successfully")
    print(f"Dialect name: {engine.dialect.name}")
    print(f"Driver name: {engine.dialect.driver}")

    with engine.connect() as conn:
        print("\nConnection successful")

        # Test schema retrieval
        print("\n=== Testing Schema Names ===")
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        print("Available schemas:", schemas)

        # Test table retrieval for each schema
        for schema in schemas:
            print(f"\n=== Testing Tables in Schema: {schema} ===")
            tables = inspector.get_table_names(schema=schema)
            print(f"Tables in {schema}:", tables)

            # Test column retrieval for first table
            if tables:
                first_table = tables[0]
                print(f"\n=== Testing Columns for {schema}.{first_table} ===")
                columns = inspector.get_columns(first_table, schema=schema)
                print("Columns:")
                pprint(columns)

        # Test a simple query
        print("\n=== Testing Simple Query ===")
        query = "SELECT * FROM GRAPHQL.Albums LIMIT 1"
        result = conn.execute(query)
        print("Query result:")
        for row in result:
            pprint(dict(row))

except Exception as e:
    print(f"\nError: {type(e).__name__} - {str(e)}")
    import traceback

    traceback.print_exc()
