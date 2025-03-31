"""
NJ Turnpike Custom Nodes Demo

This demo showcases custom Turnpike nodes developed by the NJ Turnpike team.
It demonstrates how to extend the base Turnpike framework with specialized
functionality tailored to specific business needs.
"""

import pyarrow as pa
import ibis
import time

# Import from the main Turnpike package
from turnpike.pipeline import Pipeline
from turnpike.node import Node
from turnpike.flight.server import run_flight_server

# Import our custom nodes
try:
    # When installed as a package
    from nj_turnpike.identity_node import IdentityNode
    from nj_turnpike.double_flight_node import DoubleValueNode
except ImportError:
    # When running from the repository root
    from src.identity_node import IdentityNode
    from src.double_flight_node import DoubleValueNode


class NJTurnpikePipeline(Pipeline):
    """
    Example pipeline that demonstrates NJ Turnpike custom node extensions.
    
    This pipeline:
    1. Creates a dataset of sensor readings
    2. Extracts temperature readings with an IdentityNode (demonstrates column selection)
    3. Doubles temperature values with a DoubleValueNode (demonstrates value transformation)
    4. Calculates summary statistics locally
    """
    
    def __init__(self):
        super().__init__()
        self.con = ibis.duckdb.connect()
        
    def build(self):
        """Define the pipeline's DAG structure."""
        # Create sample sensor data
        print("🌡️ Creating sample sensor data...")
        self.sensor_data = pa.table({
            "sensor_id": [1001, 1002, 1003, 1004, 1005],
            "timestamp": [1646006400, 1646006460, 1646006520, 1646006580, 1646006640],
            "temperature_c": [22.5, 19.8, 27.1, 24.3, 18.5],
            "humidity_pct": [48.2, 52.0, 38.7, 45.1, 61.3],
            "battery_v": [3.2, 3.1, 3.0, 3.3, 2.9]
        })
        
        # Register the sensor data
        self.register_node("sensor_data", self.sensor_data)
        
        # Mark build phase complete
        self._built = True
        print("✅ Pipeline DAG built successfully")
        
    def run(self):
        """Execute the pipeline."""
        if not self._built:
            raise ValueError("Pipeline must be built before running")
            
        print("\n🚀 Executing pipeline...")
        
        # Step 1: Extract temperature data using IdentityNode
        print("\n🔍 Step 1: Extracting temperature data...")
        start_time = time.time()
        
        identity_node = IdentityNode(
            input_table=self.nodes["sensor_data"],
            input_keys=["sensor_id", "timestamp", "temperature_c"],  # Only select these columns
            output_name="temperature_data"
        )
        
        temp_data = identity_node.execute()["temperature_data"]
        identity_time = time.time() - start_time
        
        self.outputs["temperature_data"] = temp_data
        print(f"  Extracted {temp_data.num_rows} temperature readings")
        
        # Step 2: Double temperature values using DoubleValueNode
        print("\n🔄 Step 2: Doubling temperature values...")
        start_time = time.time()
        
        double_node = DoubleValueNode(
            input_table=temp_data,
            column_name="temperature_c",
            output_column_name="temperature_f",  # Create a new column
            output_name="doubled_temp"
        )
        
        doubled_data = double_node.execute()["doubled_temp"]
        double_time = time.time() - start_time
        
        self.outputs["doubled_temp"] = doubled_data
        print(f"  Doubled {doubled_data.num_rows} temperature readings")
        
        # Step 3: Calculate statistics locally
        print("\n📊 Step 3: Calculating statistics...")
        start_time = time.time()
        
        # Register the doubled data in DuckDB
        self.con.create_table("doubled_temp", doubled_data, overwrite=True)
        
        # Create a statistics expression
        stats_expr = self.con.table("doubled_temp").aggregate([
            self.con.table("doubled_temp").temperature_c.min().name("min_temp_c"),
            self.con.table("doubled_temp").temperature_c.max().name("max_temp_c"),
            self.con.table("doubled_temp").temperature_c.mean().name("avg_temp_c"),
            self.con.table("doubled_temp").temperature_f.min().name("min_temp_f"),
            self.con.table("doubled_temp").temperature_f.max().name("max_temp_f"),
            self.con.table("doubled_temp").temperature_f.mean().name("avg_temp_f")
        ])
        
        # Execute the statistics node
        stats_node = Node(stats_expr, self.con)
        stats_data = stats_node.execute()["default"]
        stats_time = time.time() - start_time
        
        self.outputs["stats"] = stats_data
        
        # Print performance information
        print("\n⏱️ Performance Summary:")
        print(f"  Identity operation: {identity_time:.4f} seconds")
        print(f"  Double operation: {double_time:.4f} seconds")
        print(f"  Statistics calculation: {stats_time:.4f} seconds")
        print(f"  Total execution time: {identity_time + double_time + stats_time:.4f} seconds")
        
        self._ran = True
        return self.outputs


def run_demo():
    """Run the NJ Turnpike nodes demo."""
    print("🚗 Starting NJ Turnpike Custom Nodes Demo")
    
    # Start the Flight server for remote execution
    print("\n💫 Starting Flight server...")
    run_flight_server()
    time.sleep(1)  # Give server time to start
    
    # Create and run the pipeline
    pipeline = NJTurnpikePipeline()
    
    # Build the pipeline
    pipeline.build()
    
    # Run the pipeline
    results = pipeline.run()
    
    # Display results
    print("\n📊 Results:")
    print("\nTemperature Data (Identity Node):")
    print(results["temperature_data"].to_pandas().head())
    
    print("\nTemperature Data with Fahrenheit (Double Node):")
    print(results["doubled_temp"].to_pandas().head())
    
    print("\nTemperature Statistics:")
    print(results["stats"].to_pandas())
    
    print("\n✅ Demo complete!")


if __name__ == "__main__":
    run_demo()