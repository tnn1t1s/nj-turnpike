"""
Identity Node for Turnpike pipelines

This node is a specialized MCPModelNode that simply returns its input unchanged.
Useful for testing, as a placeholder, or for topology definition.
"""

import pyarrow as pa
from typing import Optional, Dict, List

# Import from the main Turnpike package
from turnpike.mcp.mcp_node import MCPModelNode


class IdentityNode(MCPModelNode):
    """
    A specialized MCPNode that returns its input unchanged.
    
    This node can be used:
    - As a placeholder during pipeline development
    - For testing pipeline topology
    - For verification of data at specific points in the DAG
    - As a base class for more specialized transformations
    """
    
    def __init__(
        self,
        input_table: Optional[pa.Table] = None,
        input_keys: Optional[List[str]] = None,
        output_name: str = "default"
    ):
        """
        Initialize an IdentityNode.
        
        Args:
            input_table: The PyArrow table to process
            input_keys: Optional list of column names to include (if None, uses all columns)
            output_name: Name for the output table in the result dictionary
        """
        # Set up a mock MCP endpoint that our class will intercept
        # The URL doesn't matter as we'll override the execute method
        super().__init__(
            endpoint_url="http://identity.local/v1/models/identity",
            input_table=input_table,
            input_keys=input_keys,
            output_name=output_name
        )
    
    def execute(self) -> Dict[str, pa.Table]:
        """
        Execute the identity operation (return input unchanged).
        
        Returns:
            Dict[str, pa.Table]: Dictionary with a single entry mapping output_name to the input table
        """
        if self.input_table is None:
            raise ValueError("Input table is required")
            
        # Get the subset of columns if input_keys is specified
        if self.input_keys:
            result = self.input_table.select(self.input_keys)
        else:
            result = self.input_table
            
        return {self.output_name: result}