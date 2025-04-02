#!/usr/bin/env python
# coding: utf-8

# # Flight Nodes Tutorial
# 
# This tutorial demonstrates using both:
# 1. A standard `FlightNode` from turnpike 
# 2. Our custom `DoubleValueNode` from nj-turnpike
# 
# Both perform SQL operations on a PyArrow table via a Flight server.

# ## Setup
# 
# First, let's import the necessary libraries and start a Flight server.

# In[ ]:


import pyarrow as pa
import pandas as pd
import numpy as np
import time
from datetime import datetime

# Import from the main Turnpike package
from turnpike.flight.server import run_flight_server
from turnpike.flight.client_node import FlightNode

# Import our custom flight node
from nj_turnpike.double_flight_node import DoubleValueNode

# Start the Flight server
print("Starting Flight server...")
run_flight_server()
time.sleep(1)  # Give server time to start
print("Flight server running!")


# ## Create Sample Data
# 
# Let's create a sample PyArrow table to use in our examples.

# In[ ]:


def create_sample_data(rows=100):
    """Create a sample PyArrow table with numeric data."""
    # Create random data
    np.random.seed(42)  # For reproducibility

    data = {
        "id": list(range(1, rows+1)),
        "value_a": np.random.normal(100, 25, rows).astype(int),
        "value_b": np.random.normal(200, 50, rows).astype(int),
        "category": np.random.choice(["A", "B", "C"], size=rows),
        "timestamp": [datetime.now() for _ in range(rows)]
    }

    # Create PyArrow table
    return pa.Table.from_pandas(pd.DataFrame(data))

# Create the data
data = create_sample_data(200)
print(f"Created sample data with {data.num_rows} rows")
print("Schema:")
print(data.schema)

# Display a preview of the data
data.to_pandas().head()


# ## Example 1: Using Standard FlightNode
# 
# First, let's use the standard `FlightNode` from turnpike to perform a SQL operation on our data. In this example, we'll calculate the sum of two columns.

# In[ ]:


# SQL to calculate the sum of two columns
sql = """
SELECT 
    id,
    value_a,
    value_b,
    (value_a + value_b) AS sum_values,
    category
FROM input_table
"""

# Create and execute the node
start_time = time.time()

standard_node = FlightNode(
    expression=sql,
    input_table=data,
    output_name="combined_values"
)

standard_result = standard_node.execute()
combined_data = standard_result["combined_values"]

standard_time = time.time() - start_time

print(f"Processing time: {standard_time:.4f} seconds")
print("Sample results:")
combined_data.to_pandas().head()


# ## Example 2: Using Custom DoubleValueNode
# 
# Now let's use our custom `DoubleValueNode` from nj-turnpike to double the values in a column.

# In[ ]:


# Create and execute the node
start_time = time.time()

double_node = DoubleValueNode(
    input_table=data,
    column_name="value_a",
    output_column_name="doubled_value_a",
    output_name="doubled_data"
)

double_result = double_node.execute()
doubled_data = double_result["doubled_data"]

custom_time = time.time() - start_time

print(f"Processing time: {custom_time:.4f} seconds")
print("Sample results:")
doubled_data.to_pandas().head()


# ## Example 3: Chaining Flight Nodes
# 
# Now let's demonstrate how to chain nodes together. We'll first use our custom `DoubleValueNode` to double a column, then use a standard `FlightNode` to combine the doubled column with another column.

# In[ ]:


start_time = time.time()

# First double the values
first_node = DoubleValueNode(
    input_table=data,
    column_name="value_a",
    output_column_name="doubled_value_a",
    output_name="intermediate"
)

intermediate_result = first_node.execute()
intermediate_data = intermediate_result["intermediate"]

# Let's display the intermediate data
print("Intermediate data:")
intermediate_data.to_pandas().head(3)


# In[ ]:


# Then combine with another column using standard FlightNode
sql = """
SELECT 
    id,
    value_a,
    doubled_value_a,
    value_b,
    (doubled_value_a + value_b) AS enhanced_sum
FROM input_table
"""

second_node = FlightNode(
    expression=sql,
    input_table=intermediate_data,
    output_name="final_result"
)

final_result = second_node.execute()
final_data = final_result["final_result"]

chain_time = time.time() - start_time

print(f"Total chaining processing time: {chain_time:.4f} seconds")
print("Final results:")
final_data.to_pandas().head()


# ## Results Validation
# 
# Let's verify that all our nodes produced the expected results.

# In[ ]:


# Get pandas dataframes for easier validation
df = data.to_pandas()
df_combined = combined_data.to_pandas()
df_doubled = doubled_data.to_pandas()
df_final = final_data.to_pandas()

# Validate standard node results
assert all(df_combined["sum_values"] == df["value_a"] + df["value_b"])
print("✓ Standard FlightNode correctly calculated sums")

# Validate custom node results
assert all(df_doubled["doubled_value_a"] == df["value_a"] * 2)
print("✓ DoubleValueNode correctly doubled values")

# Validate chained results
assert all(df_final["enhanced_sum"] == (df["value_a"] * 2) + df["value_b"])
print("✓ Chained nodes correctly processed data")


# ## Performance Comparison
# 
# Let's compare the performance of the different approaches.

# In[ ]:


print("Performance Summary:")
print(f"Standard FlightNode: {standard_time:.4f} seconds")
print(f"Custom DoubleValueNode: {custom_time:.4f} seconds")
print(f"Chained nodes: {chain_time:.4f} seconds")


# ## Conclusion
# 
# In this tutorial, we've demonstrated:
# 
# 1. How to use the standard `FlightNode` from turnpike to execute SQL on a PyArrow table
# 2. How to use a custom `DoubleValueNode` from nj-turnpike to perform specialized operations
# 3. How to chain multiple nodes together to build a data processing pipeline
# 
# The custom `DoubleValueNode` provides a more user-friendly interface for a specific operation (doubling values), while the standard `FlightNode` offers more flexibility with raw SQL. By combining them, you can create powerful data processing pipelines that are both flexible and easy to use.
