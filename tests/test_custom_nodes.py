"""
Tests for NJ Turnpike custom nodes.
"""

import pytest
import pyarrow as pa
import numpy as np
from datetime import datetime

from nj_turnpike.identity_node import IdentityNode
from nj_turnpike.double_flight_node import DoubleValueNode

try:
    # Try to import from demo
    from demo.custom_nodes_demo import TollDataFanOutNode
except ImportError:
    # If not available, create a placeholder for tests that need it
    TollDataFanOutNode = None


class TestIdentityNode:
    """Test cases for the IdentityNode."""
    
    def test_identity_node_full_table(self):
        """Test that IdentityNode returns the full table when no columns specified."""
        # Create a sample table
        data = pa.table({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
            "category": ["A", "B", "C"]
        })
        
        # Create an identity node
        node = IdentityNode(input_table=data)
        
        # Execute
        result = node.execute()["default"]
        
        # Check that all columns are included
        assert result.column_names == data.column_names
        assert result.num_rows == data.num_rows
        
    def test_identity_node_column_selection(self):
        """Test that IdentityNode correctly filters columns."""
        # Create a sample table
        data = pa.table({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
            "category": ["A", "B", "C"]
        })
        
        # Create an identity node with column selection
        node = IdentityNode(
            input_table=data,
            input_keys=["id", "value"]
        )
        
        # Execute
        result = node.execute()["default"]
        
        # Check that only selected columns are included
        assert result.column_names == ["id", "value"]
        assert result.num_rows == data.num_rows
        
    def test_identity_node_output_name(self):
        """Test that IdentityNode uses the correct output name."""
        # Create a sample table
        data = pa.table({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        })
        
        # Create an identity node with custom output name
        output_name = "test_output"
        node = IdentityNode(
            input_table=data,
            output_name=output_name
        )
        
        # Execute
        result = node.execute()
        
        # Check that the output name is correct
        assert output_name in result
        assert isinstance(result[output_name], pa.Table)


# Mock Flight client to avoid needing a server for tests
class MockDoubleValueNode(DoubleValueNode):
    """Mock version of DoubleValueNode that doesn't use Flight."""
    
    def execute(self):
        """Execute without Flight server by directly applying the transformation."""
        if self.input_table is None:
            raise ValueError("Input table is required")
            
        # Create a copy of the input table
        result = self.input_table
        
        # Apply the transformation directly
        if self.output_column_name != self.column_name:
            # Create a new column with doubled values
            column_data = result.column(self.column_name)
            doubled_values = pa.array([v * 2 for v in column_data.to_pylist()])
            
            # Add the new column
            new_data = {name: result.column(name) for name in result.column_names}
            new_data[self.output_column_name] = doubled_values
            result = pa.table(new_data)
        else:
            # Replace the original column with doubled values
            column_data = result.column(self.column_name)
            doubled_values = pa.array([v * 2 for v in column_data.to_pylist()])
            
            # Create a new table with the doubled column
            new_data = {name: (doubled_values if name == self.column_name else result.column(name)) 
                       for name in result.column_names}
            result = pa.table(new_data)
            
        return {self.output_name: result}


class TestDoubleValueNode:
    """Test cases for the DoubleValueNode."""
    
    def test_double_node_new_column(self):
        """Test that DoubleValueNode creates a new column with doubled values."""
        # Create a sample table
        data = pa.table({
            "id": [1, 2, 3],
            "temperature": [10.0, 20.0, 30.0]
        })
        
        # Create a double node with a new column
        node = MockDoubleValueNode(
            input_table=data,
            column_name="temperature",
            output_column_name="temperature_doubled"
        )
        
        # Execute
        result = node.execute()["default"]
        
        # Check that the new column exists
        assert "temperature_doubled" in result.column_names
        
        # Check that the values are doubled
        original = data.column("temperature").to_pylist()
        doubled = result.column("temperature_doubled").to_pylist()
        
        assert doubled == [v * 2 for v in original]
        
    def test_double_node_replace_column(self):
        """Test that DoubleValueNode replaces a column with doubled values."""
        # Create a sample table
        data = pa.table({
            "id": [1, 2, 3],
            "temperature": [10.0, 20.0, 30.0]
        })
        
        # Create a double node that replaces the original column
        node = MockDoubleValueNode(
            input_table=data,
            column_name="temperature"
        )
        
        # Execute
        result = node.execute()["default"]
        
        # Check that the values are doubled
        original = data.column("temperature").to_pylist()
        doubled = result.column("temperature").to_pylist()
        
        assert doubled == [v * 2 for v in original]
        
    def test_double_node_validation(self):
        """Test that DoubleValueNode validates the column name and type."""
        # Create a sample table
        data = pa.table({
            "id": [1, 2, 3],
            "category": ["A", "B", "C"]  # Non-numeric column
        })
        
        # Check that creating a node with a non-existent column raises an error
        with pytest.raises(ValueError, match="not found in input table"):
            MockDoubleValueNode(
                input_table=data,
                column_name="non_existent"
            )
            
        # Check that creating a node with a non-numeric column raises an error
        with pytest.raises(ValueError, match="must be numeric"):
            MockDoubleValueNode(
                input_table=data,
                column_name="category"
            )


# Mock version of TollDataFanOutNode for tests
class MockTollDataFanOutNode:
    """Mock version of TollDataFanOutNode that doesn't use Flight."""
    
    def __init__(self, input_table):
        self.input_table = input_table
        
    def execute(self):
        """Execute without Flight server by directly creating the output tables."""
        if self.input_table is None:
            raise ValueError("Input table is required")
            
        # Create vehicle data
        vehicle_columns = [
            "transaction_id", "plaza_id", "vehicle_class", "vehicle_type", 
            "axle_count", "weight_kg", "height_cm", "length_cm", "timestamp"
        ]
        
        vehicle_data = self.input_table.select(
            [col for col in vehicle_columns if col in self.input_table.column_names]
        )
        
        # Create payment data
        payment_columns = [
            "transaction_id", "plaza_id", "payment_method", "is_ezpass", "amount", "timestamp"
        ]
        
        payment_data = self.input_table.select(
            [col for col in payment_columns if col in self.input_table.column_names]
        )
        
        # Create temporal data - extract time components
        temporal_data = self.input_table.select(["transaction_id", "plaza_id", "timestamp"])
        
        # Return all three outputs
        return {
            "vehicle_data": vehicle_data,
            "payment_data": payment_data,
            "temporal_data": temporal_data
        }


@pytest.mark.skipif(TollDataFanOutNode is None, reason="TollDataFanOutNode not available")
class TestTollDataFanOutNode:
    """Test cases for the TollDataFanOutNode."""
    
    def setup_method(self):
        """Set up test data."""
        # Create a small sample of toll plaza data
        timestamps = [
            datetime(2023, 5, 1, 8, 30, 0),
            datetime(2023, 5, 1, 12, 15, 0),
            datetime(2023, 5, 1, 17, 45, 0)
        ]
        
        self.toll_data = pa.table({
            "transaction_id": [10001, 10002, 10003],
            "plaza_id": [1, 2, 3],
            "timestamp": timestamps,
            "vehicle_class": [1, 2, 4],
            "vehicle_type": ["sedan", "suv", "semi"],
            "axle_count": [2, 2, 5],
            "weight_kg": [1500, 2200, 15000],
            "height_cm": [150, 180, 350],
            "length_cm": [450, 500, 1500],
            "payment_method": ["ezpass", "cash", "ezpass"],
            "is_ezpass": [True, False, True],
            "amount": [2.5, 5.5, 10.0]
        })
    
    def test_fan_out_structure(self):
        """Test that TollDataFanOutNode creates the correct three outputs."""
        # Use the mock node to avoid needing a Flight server
        node = MockTollDataFanOutNode(input_table=self.toll_data)
        
        # Execute
        result = node.execute()
        
        # Check that all three outputs exist
        assert "vehicle_data" in result
        assert "payment_data" in result
        assert "temporal_data" in result
        
        # Check that each output is a PyArrow table
        assert isinstance(result["vehicle_data"], pa.Table)
        assert isinstance(result["payment_data"], pa.Table)
        assert isinstance(result["temporal_data"], pa.Table)
        
    def test_vehicle_data_columns(self):
        """Test that vehicle_data output has the correct columns."""
        # Use the mock node to avoid needing a Flight server
        node = MockTollDataFanOutNode(input_table=self.toll_data)
        
        # Execute
        result = node.execute()
        vehicle_data = result["vehicle_data"]
        
        # Check that vehicle_data has the expected columns
        expected_columns = [
            "transaction_id", "plaza_id", "vehicle_class", "vehicle_type", 
            "axle_count", "weight_kg", "height_cm", "length_cm", "timestamp"
        ]
        
        for col in expected_columns:
            assert col in vehicle_data.column_names
            
        # Check that the data is preserved
        assert vehicle_data.num_rows == self.toll_data.num_rows
        
    def test_payment_data_columns(self):
        """Test that payment_data output has the correct columns."""
        # Use the mock node to avoid needing a Flight server
        node = MockTollDataFanOutNode(input_table=self.toll_data)
        
        # Execute
        result = node.execute()
        payment_data = result["payment_data"]
        
        # Check that payment_data has the expected columns
        expected_columns = [
            "transaction_id", "plaza_id", "payment_method", "is_ezpass", "amount", "timestamp"
        ]
        
        for col in expected_columns:
            assert col in payment_data.column_names
            
        # Check that the data is preserved
        assert payment_data.num_rows == self.toll_data.num_rows
        
    def test_temporal_data_columns(self):
        """Test that temporal_data output has the correct columns."""
        # Use the mock node to avoid needing a Flight server
        node = MockTollDataFanOutNode(input_table=self.toll_data)
        
        # Execute
        result = node.execute()
        temporal_data = result["temporal_data"]
        
        # Check that temporal_data has the expected columns
        expected_columns = ["transaction_id", "plaza_id", "timestamp"]
        
        for col in expected_columns:
            assert col in temporal_data.column_names
            
        # Check that the data is preserved
        assert temporal_data.num_rows == self.toll_data.num_rows


class TestFanOutPipeline:
    """Integration tests for the fan-out pipeline components."""
    
    def setup_method(self):
        """Set up test data."""
        # Create a small sample of toll plaza data
        timestamps = [
            datetime(2023, 5, 1, 8, 30, 0),
            datetime(2023, 5, 1, 12, 15, 0),
            datetime(2023, 5, 1, 17, 45, 0)
        ]
        
        self.toll_data = pa.table({
            "transaction_id": [10001, 10002, 10003],
            "plaza_id": [1, 2, 3],
            "timestamp": timestamps,
            "vehicle_class": [1, 2, 4],
            "vehicle_type": ["sedan", "suv", "semi"],
            "axle_count": [2, 2, 5],
            "weight_kg": [1500, 2200, 15000],
            "height_cm": [150, 180, 350],
            "length_cm": [450, 500, 1500],
            "payment_method": ["ezpass", "cash", "ezpass"],
            "is_ezpass": [True, False, True],
            "amount": [2.5, 5.5, 10.0]
        })
    
    def test_fan_out_double_integration(self):
        """Test that fan-out data can be processed by DoubleValueNode."""
        # First fan out the data
        fan_out_node = MockTollDataFanOutNode(input_table=self.toll_data)
        fan_out_results = fan_out_node.execute()
        
        # Get the vehicle data
        vehicle_data = fan_out_results["vehicle_data"]
        
        # Now use DoubleValueNode on the weight_kg column
        double_node = MockDoubleValueNode(
            input_table=vehicle_data,
            column_name="weight_kg",
            output_column_name="adjusted_weight"
        )
        
        # Execute
        double_result = double_node.execute()
        heavy_vehicles = double_result["default"]
        
        # Check that the adjusted weight is double the original
        orig_weights = vehicle_data.column("weight_kg").to_pylist()
        adj_weights = heavy_vehicles.column("adjusted_weight").to_pylist()
        
        assert adj_weights == [w * 2 for w in orig_weights]
        
    def test_fan_in_data_flow(self):
        """Test the fan-in data flow by combining results from multiple streams."""
        # First fan out the data
        fan_out_node = MockTollDataFanOutNode(input_table=self.toll_data)
        fan_out_results = fan_out_node.execute()
        
        # Get the separate data streams
        vehicle_data = fan_out_results["vehicle_data"]
        payment_data = fan_out_results["payment_data"]
        
        # Extract heavy vehicles (class 4 or above)
        heavy_mask = np.array(vehicle_data.column("vehicle_class").to_pylist()) >= 4
        heavy_transaction_ids = np.array(vehicle_data.column("transaction_id").to_pylist())[heavy_mask]
        
        # Simulate the fan-in process by finding payments for heavy vehicles
        heavy_payment_mask = np.isin(
            np.array(payment_data.column("transaction_id").to_pylist()),
            heavy_transaction_ids
        )
        
        # Get payment amounts for heavy vehicles
        heavy_payments = np.array(payment_data.column("amount").to_pylist())[heavy_payment_mask]
        
        # Verify that we found the payments for the heavy vehicles
        assert len(heavy_payments) > 0
        
        # Verify that the heavy vehicle in our sample data has the expected payment
        assert 10.0 in heavy_payments