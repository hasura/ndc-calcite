# Arrow Test Data

This directory should contain sample.arrow file for testing.

Since creating Arrow files requires additional tools, this serves as a placeholder.
The comprehensive test will skip this format if the file doesn't exist.

To create test data:
```bash
# Using Python with pyarrow
python3 -c "
import pyarrow as pa
import pyarrow.feather as feather

# Create sample data
data = {
    'id': [1, 2, 3],
    'product': ['Widget', 'Gadget', 'Tool'],
    'price': [19.99, 29.99, 39.99],
    'in_stock': [True, False, True]
}

# Create Arrow table
table = pa.table(data)

# Write to feather/arrow format
feather.write_feather(table, 'sample.arrow')
"
```