"""
Tests for NJ Turnpike custom nodes.
"""

import pytest
import pyarrow as pa
import numpy as np

try:
    # When installed as a package
    from nj_turnpike.identity_node import IdentityNode
    from nj_turnpike.double_flight_node import DoubleValueNode
except ImportError:
    # When running from the repository root
    from src.identity_node import IdentityNode
    from src.double_flight_node import DoubleValueNode


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