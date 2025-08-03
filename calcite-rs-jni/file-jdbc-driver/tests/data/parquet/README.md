# Parquet Test Data

This directory should contain sample.parquet file for testing.

Since creating Parquet files requires additional tools, this serves as a placeholder.
The comprehensive test will skip this format if the file doesn't exist.

To create test data:
```bash
# Using Python with pandas and pyarrow
python3 -c "
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Create sample data
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'], 
    'age': [25, 30, 35],
    'salary': [50000.0, 60000.0, 70000.0]
})

# Write to parquet
df.to_parquet('sample.parquet', index=False)
"
```