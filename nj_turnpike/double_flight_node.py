"""
Double Value Flight Node for Turnpike pipelines

This node is a specialized FlightNode that doubles the values in a specified column.
"""

import pyarrow as pa
from typing import Optional, Dict, Union, List

# Import from the main Turnpike package
from turnpike.flight.client_node import FlightNode


class DoubleValueNode(FlightNode):
    """
    A specialized FlightNode that doubles the values in a specified numeric column.
    
    This node demonstrates how to extend the FlightNode with custom SQL transformations
    that are performed on the Flight server.
    """
    
    def __init__(
        self,
        input_table: pa.Table,
        column_name: str,
        output_column_name: Optional[str] = None,
        output_name: str = "default"
    ):
        """
        Initialize a DoubleValueNode.
        
        Args:
            input_table: The PyArrow table to process
            column_name: The name of the column whose values should be doubled
            output_column_name: Optional name for the new column with doubled values.
                               If None, the original column will be replaced.
            output_name: Name for the output table in the result dictionary
        """
        # Validate column exists and is numeric
        if column_name not in input_table.column_names:
            raise ValueError(f"Column '{column_name}' not found in input table")
            
        # Check if the column is numeric
        column_type = input_table.schema.field(column_name).type
        if not (pa.types.is_integer(column_type) or pa.types.is_floating(column_type)):
            raise ValueError(f"Column '{column_name}' must be numeric, got {column_type}")
            
        # Store the column name
        self.column_name = column_name
        self.output_column_name = output_column_name or column_name
        
        # Generate SQL that doubles the specified column
        if output_column_name and output_column_name != column_name:
            # Create a new column with doubled values
            sql = f"""
                SELECT 
                    *, 
                    {column_name} * 2 AS {output_column_name}
                FROM input_table
            """
        else:
            # Replace the original column with doubled values
            columns = []
            for name in input_table.column_names:
                if name == column_name:
                    columns.append(f"{name} * 2 AS {name}")
                else:
                    columns.append(name)
                    
            sql = f"""
                SELECT 
                    {', '.join(columns)}
                FROM input_table
            """
        
        # Initialize the parent FlightNode with our custom SQL
        # Updated to match new FlightNode API which uses 'expression' instead of 'sql'
        super().__init__(
            expression=sql,
            input_table=input_table,
            output_name=output_name
        )