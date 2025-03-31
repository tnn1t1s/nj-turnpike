# NJ Turnpike: Custom Nodes for the Turnpike Framework

## Overview

**NJ Turnpike** is a collection of custom nodes and extensions for the [Turnpike](https://github.com/tnn1t1s/turnpike) framework. It demonstrates how to build specialized nodes that extend Turnpike's capabilities while maintaining compatibility with the core framework.

This package provides:

- **IdentityNode**: A specialized MCPNode that passes data through unchanged (useful for testing, column selection, and as a base class)
- **DoubleValueNode**: A FlightNode that doubles values in a numeric column, demonstrating remote execution

## What is Turnpike?

Turnpike is a high-performance hybrid DAG (Directed Acyclic Graph) execution framework designed for data processing pipelines that combines:

- **Local execution** via Ibis + DuckDB
- **Remote execution** via Apache Arrow Flight
- **AI model integration** via Model Control Protocol (MCP) and ONNX
- **Zero-copy data passing** using Apache Arrow Tables

Key features that make Turnpike powerful:

- **DAG-based architecture**: Unlike tree-based systems, Turnpike allows sharing intermediate results across branches
- **Hybrid execution**: Seamlessly mix local and remote processing nodes
- **Arrow native**: All data flows through the system as Arrow Tables, enabling efficient data transfer
- **AI integration**: Built-in support for model inference via ONNX and MCP
- **Extensible**: Designed to be extended with custom nodes (like the ones in this package)

## Why Use Turnpike?

Turnpike provides several advantages for data processing workflows:

1. **Efficiency**: Zero-copy data sharing between nodes running in the same process
2. **Flexibility**: Mix local and remote execution as needed
3. **Scalability**: Use Flight protocol to offload heavy processing
4. **Simple API**: Clean, composable abstractions for building DAGs
5. **AI Model Integration**: First-class support for ONNX models and MCP-compliant services
6. **Extensibility**: Easily create custom nodes for specific needs

## Installation

```bash
# Install Turnpike from GitHub
pip install git+https://github.com/tnn1t1s/turnpike.git

# Then install NJ Turnpike
pip install git+https://github.com/yourusername/nj-turnpike.git

# For development
git clone https://github.com/yourusername/nj-turnpike.git
cd nj-turnpike
pip install -e .
```

## NJ Turnpike Nodes

### IdentityNode

The `IdentityNode` is a specialized MCPNode that returns its input data unchanged. It's useful for:

- **Testing pipeline topology**
- **Data inspection** at specific points in a DAG
- **Column selection** to extract specific fields
- **Base class** for more specialized transformations

```python
from nj_turnpike import IdentityNode
import pyarrow as pa

# Create a sample table
data = pa.table({
    "id": [1, 2, 3],
    "value": [10, 20, 30],
    "category": ["A", "B", "C"]
})

# Create an identity node that only selects specific columns
identity_node = IdentityNode(
    input_table=data,
    input_keys=["id", "value"],  # Only include these columns
    output_name="filtered_data"
)

# Execute the node - returns the input table with only selected columns
result = identity_node.execute()["filtered_data"]
```

### DoubleValueNode

The `DoubleValueNode` is a specialized FlightNode that doubles the values in a numeric column. It demonstrates:

- **Remote execution** via Apache Arrow Flight
- **SQL transformation** of data
- **Custom validation** to ensure inputs meet requirements

```python
from nj_turnpike import DoubleValueNode
import pyarrow as pa

# Create a sample table
data = pa.table({
    "id": [1, 2, 3],
    "temperature": [22.5, 19.8, 27.1]
})

# Create a node that doubles the temperature values into a new column
double_node = DoubleValueNode(
    input_table=data,
    column_name="temperature",
    output_column_name="temperature_doubled",  # Create a new column
    output_name="transformed_data"
)

# Execute the node - processes via Flight server
result = double_node.execute()["transformed_data"]
```

## Demo Pipeline

NJ Turnpike includes a demonstration pipeline that showcases both custom nodes in an end-to-end workflow:

```bash
# Run the demo
python -m nj_turnpike.demo.custom_nodes_demo
# or from the repo root
python demo/custom_nodes_demo.py
```

This demo performs the following steps:

1. **Creates a sample sensor dataset** with temperature readings
2. **Uses IdentityNode** to extract specific columns (temperature readings)
3. **Uses DoubleValueNode** to convert temperature values (local to Fahrenheit)
4. **Calculates statistics** on the processed data with a local DuckDB node

The demo shows how to:
- Create a custom pipeline class
- Register nodes and data in the DAG
- Mix local and remote execution
- Chain multiple nodes together
- Measure performance

## Advanced Usage

### Creating a Custom Pipeline with NJ Turnpike Nodes

You can incorporate NJ Turnpike nodes into your own pipelines:

```python
from turnpike import Pipeline
from turnpike.node import Node
from turnpike.flight.server import run_flight_server
from nj_turnpike import IdentityNode, DoubleValueNode
import pyarrow as pa
import ibis

class MyCustomPipeline(Pipeline):
    def __init__(self):
        super().__init__()
        self.con = ibis.duckdb.connect()
        
    def build(self):
        # Create your input data
        self.input_data = pa.table({
            "id": [1, 2, 3, 4, 5],
            "temperature_c": [22.5, 19.8, 27.1, 24.3, 18.5],
            "humidity_pct": [48.2, 52.0, 38.7, 45.1, 61.3],
        })
        
        # Register data in the pipeline
        self.register_node("input_data", self.input_data)
        self._built = True
        
    def run(self):
        if not self._built:
            raise ValueError("Pipeline must be built before running")
            
        # Filter data using IdentityNode
        identity_node = IdentityNode(
            input_table=self.nodes["input_data"],
            input_keys=["id", "temperature_c"],
            output_name="temperature_data"
        )
        
        # Get temperature data
        temp_data = identity_node.execute()["temperature_data"]
        
        # Convert to Fahrenheit using DoubleValueNode
        double_node = DoubleValueNode(
            input_table=temp_data,
            column_name="temperature_c",
            output_column_name="temperature_f",
            output_name="converted_data"
        )
        
        # Get converted data
        converted_data = double_node.execute()["converted_data"]
        self.outputs["result"] = converted_data
        
        # Final analysis using a local Node
        self.con.create_table("temp_data", converted_data, overwrite=True)
        analysis_expr = self.con.table("temp_data").aggregate([
            self.con.table("temp_data").temperature_c.mean().name("avg_temp_c"),
            self.con.table("temp_data").temperature_f.mean().name("avg_temp_f")
        ])
        
        stats_node = Node(analysis_expr, self.con)
        stats = stats_node.execute()["default"]
        self.outputs["stats"] = stats
        
        self._ran = True
        return self.outputs

# Start the Flight server (required for DoubleValueNode)
run_flight_server()

# Create and run the pipeline
pipeline = MyCustomPipeline()
pipeline.build()
results = pipeline.run()

print("Converted Data:")
print(results["result"].to_pandas())
print("\nStatistics:")
print(results["stats"].to_pandas())
```

## Developing Your Own Custom Nodes

NJ Turnpike serves as a template for creating your own Turnpike extensions. To create additional custom nodes:

1. **Choose a base node type**:
   - `Node`: For local execution with Ibis
   - `FlightNode`: For remote execution via Arrow Flight
   - `MCPModelNode`: For AI model integration
   - `ONNXModelNode`: For ONNX model inference
   - `CheckpointNode`: For persistent intermediate results

2. **Create a subclass** that inherits from the chosen base:

```python
from turnpike.mcp import MCPModelNode
import pyarrow as pa
from typing import Dict, Optional

class MyCustomNode(MCPModelNode):
    """
    A specialized node that extends MCPModelNode with custom functionality.
    """
    
    def __init__(
        self,
        input_table: Optional[pa.Table] = None,
        custom_param: str = "default",
        output_name: str = "default"
    ):
        super().__init__(
            endpoint_url="http://your-service.com/v1/models/model",
            input_table=input_table,
            output_name=output_name
        )
        self.custom_param = custom_param
    
    def execute(self) -> Dict[str, pa.Table]:
        # Custom pre-processing logic
        
        # Call parent method for core functionality
        result = super().execute()
        
        # Custom post-processing logic
        
        return result
```

3. **Override methods** as needed for your specific functionality:
   - `execute()`: For synchronous execution
   - `stream()`: For streaming execution
   - Custom validation methods
   - Pre/post-processing methods

4. **Import your custom nodes** in your pipelines:

```python
from my_package.custom_nodes import MyCustomNode
from turnpike import Pipeline

class MyPipeline(Pipeline):
    def run(self):
        # Use your custom node
        node = MyCustomNode(input_table=data, custom_param="value")
        result = node.execute()
```

## Architecture of Turnpike

Understanding Turnpike's architecture helps when developing custom nodes:

- **Node**: Base class for all nodes, representing a computation unit
- **Pipeline**: Container for nodes, manages DAG execution
- **MCPModelNode**: Connects to AI model services via MCP protocol
- **FlightNode**: Executes operations on a remote Flight server
- **ONNXModelNode**: Runs inference with local ONNX models

Turnpike uses PyArrow Tables as the universal data format, enabling:
- Zero-copy data sharing between in-process nodes
- Efficient serialization for remote execution
- Compatibility with popular data science tools

## Requirements

- **Python 3.9+**
- **Turnpike framework**: For core DAG functionality
- **PyArrow**: For Arrow Table operations
- **Ibis**: For query operations
- **DuckDB**: For local execution engine
- **Pytest**: For testing

Optional dependencies:
- **ONNX Runtime**: For ONNX model inference
- **Requests**: For MCP node HTTP communication

## License

None. Proprietary Repo.