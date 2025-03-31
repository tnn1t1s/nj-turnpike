#!/usr/bin/env python
"""
NJ Turnpike Fan-Out Pipeline Demo

A standalone script that demonstrates the fan-out pipeline architecture
using simplified local execution. This demo does not require a Flight server.
"""

import pyarrow as pa
import numpy as np
import time
from datetime import datetime
import pandas as pd
from tabulate import tabulate

try:
    # When installed as a package
    from nj_turnpike.double_flight_node import DoubleValueNode
except ImportError:
    # When running from the repository root
    from src.double_flight_node import DoubleValueNode

try:
    from demo.custom_nodes_demo import TollDataFanOutNode
except ImportError:
    # If not available, continue without actual implementation
    TollDataFanOutNode = None


# Mock implementations for testing without a Flight server
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


class MockDoubleValueNode:
    """Mock version of DoubleValueNode that doesn't use Flight."""
    
    def __init__(self, input_table, column_name, output_column_name=None, output_name="default"):
        # Validate column exists and is numeric
        if column_name not in input_table.column_names:
            raise ValueError(f"Column '{column_name}' not found in input table")
        
        # Store properties
        self.input_table = input_table
        self.column_name = column_name
        self.output_column_name = output_column_name or column_name
        self.output_name = output_name
        
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


def create_demo_data(num_records=200):
    """Create sample toll plaza data."""
    print("🚗 Creating sample toll plaza data...")
    
    # Generate timestamps for a day's worth of transactions
    base_time = datetime(2023, 5, 1, 0, 0, 0)
    timestamps = []
    
    for i in range(num_records):
        # Create timestamps throughout the day, with more during rush hours
        # Create a list of all hours with peak hours appearing multiple times to increase their probability
        hour_options = [7, 8, 9, 12, 17, 18, 19] + list(range(24))
        # To weight certain hours more heavily, we'll just select randomly
        if np.random.random() < 0.5:  # 50% chance of peak hour
            hour = np.random.choice([7, 8, 9, 17, 18, 19])
        else:
            hour = np.random.choice(range(24))
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        transaction_time = base_time.replace(hour=hour, minute=minute, second=second)
        timestamps.append(transaction_time)
    
    # Generate random vehicle weights (kg) based on vehicle class
    vehicle_classes = np.random.choice([1, 2, 3, 4, 5], size=num_records, p=[0.4, 0.3, 0.15, 0.1, 0.05])
    
    # Generate vehicle weights based on class
    weights = []
    for vc in vehicle_classes:
        if vc == 1:  # Cars
            weights.append(np.random.normal(1500, 300))
        elif vc == 2:  # SUVs/Light trucks
            weights.append(np.random.normal(2200, 400))
        elif vc == 3:  # Medium trucks
            weights.append(np.random.normal(5000, 1000))
        elif vc == 4:  # Heavy trucks
            weights.append(np.random.normal(15000, 3000))
        else:  # Special vehicles
            weights.append(np.random.normal(20000, 5000))
    
    # Generate vehicle types based on class
    vehicle_types = []
    for vc in vehicle_classes:
        if vc == 1:
            vehicle_types.append(np.random.choice(["sedan", "compact", "coupe"]))
        elif vc == 2:
            vehicle_types.append(np.random.choice(["suv", "pickup", "van"]))
        elif vc == 3:
            vehicle_types.append(np.random.choice(["delivery", "box_truck"]))
        elif vc == 4:
            vehicle_types.append(np.random.choice(["semi", "tanker"]))
        else:
            vehicle_types.append(np.random.choice(["oversize", "special"]))
    
    # Generate axle counts based on vehicle class
    axle_counts = []
    for vc in vehicle_classes:
        if vc == 1:
            axle_counts.append(2)
        elif vc == 2:
            axle_counts.append(np.random.choice([2, 3], p=[0.8, 0.2]))
        elif vc == 3:
            axle_counts.append(np.random.choice([2, 3], p=[0.4, 0.6]))
        elif vc == 4:
            axle_counts.append(np.random.choice([3, 4, 5], p=[0.2, 0.5, 0.3]))
        else:
            axle_counts.append(np.random.choice([5, 6, 7], p=[0.4, 0.4, 0.2]))
    
    # Generate payment methods
    is_ezpass = np.random.choice([True, False], size=num_records, p=[0.7, 0.3])
    payment_methods = []
    for ez in is_ezpass:
        if ez:
            payment_methods.append("ezpass")
        else:
            payment_methods.append(np.random.choice(["cash", "credit", "debit"], p=[0.6, 0.3, 0.1]))
    
    # Generate toll amounts based on vehicle class and payment method
    amounts = []
    for i in range(num_records):
        base_amount = vehicle_classes[i] * 2.5
        if payment_methods[i] != "ezpass":
            # Cash payments are slightly higher
            base_amount *= 1.1
        amounts.append(round(base_amount, 2))
    
    # Generate heights and lengths based on vehicle class
    heights_cm = []
    lengths_cm = []
    for vc in vehicle_classes:
        if vc == 1:  # Cars
            heights_cm.append(int(np.random.normal(150, 20)))
            lengths_cm.append(int(np.random.normal(450, 50)))
        elif vc == 2:  # SUVs/Light trucks
            heights_cm.append(int(np.random.normal(180, 20)))
            lengths_cm.append(int(np.random.normal(500, 50)))
        elif vc == 3:  # Medium trucks
            heights_cm.append(int(np.random.normal(250, 30)))
            lengths_cm.append(int(np.random.normal(700, 100)))
        elif vc == 4:  # Heavy trucks
            heights_cm.append(int(np.random.normal(350, 50)))
            lengths_cm.append(int(np.random.normal(1500, 200)))
        else:  # Special vehicles
            heights_cm.append(int(np.random.normal(400, 70)))
            lengths_cm.append(int(np.random.normal(2000, 300)))

    # Create the PyArrow table with toll plaza data
    toll_data = pa.table({
        "transaction_id": list(range(10001, 10001 + num_records)),
        "plaza_id": np.random.choice([1, 2, 3, 4, 5], size=num_records),
        "timestamp": timestamps,
        "vehicle_class": vehicle_classes,
        "vehicle_type": vehicle_types,
        "axle_count": axle_counts,
        "weight_kg": [max(500, int(w)) for w in weights],  # Ensure minimum weight
        "height_cm": heights_cm,
        "length_cm": lengths_cm,
        "payment_method": payment_methods,
        "is_ezpass": is_ezpass,
        "amount": amounts
    })
    
    return toll_data


def run_fan_out_pipeline_demo():
    """Run the toll plaza fan-out pipeline demo without Flight server."""
    print("🚗 Starting NJ Turnpike Fan-Out Pipeline Demo (Standalone Mode)")
    
    # Step 0: Create sample data
    toll_data = create_demo_data(500)
    print(f"  Created {toll_data.num_rows} sample toll transactions")
    
    # Step 1: Fan out the toll data into three streams
    print("\n🔀 Step 1: Fanning out toll data into three streams...")
    start_time = time.time()
    
    fan_out_node = MockTollDataFanOutNode(input_table=toll_data)
    fan_out_results = fan_out_node.execute()
    
    vehicle_data = fan_out_results["vehicle_data"]
    payment_data = fan_out_results["payment_data"]
    temporal_data = fan_out_results["temporal_data"]
    
    fan_out_time = time.time() - start_time
    
    print(f"  Vehicle stream: {vehicle_data.num_rows} records")
    print(f"  Payment stream: {payment_data.num_rows} records")
    print(f"  Temporal stream: {temporal_data.num_rows} records")
    
    # Step 2a: Process vehicle data - Double toll rates for heavy vehicles
    print("\n🚛 Step 2a: Processing vehicle stream (Heavy vehicle toll adjustment)...")
    start_time = time.time()
    
    # Find heavy vehicles (class 4-5)
    heavy_mask = np.array(vehicle_data.column("vehicle_class").to_pylist()) >= 4
    heavy_transaction_ids = np.array(vehicle_data.column("transaction_id").to_pylist())[heavy_mask]
    
    # Filter to get only heavy vehicles
    heavy_vehicles_indices = [i for i, mask in enumerate(heavy_mask) if mask]
    heavy_vehicles_data = {name: vehicle_data.column(name).take(heavy_vehicles_indices)
                         for name in vehicle_data.column_names}
    heavy_vehicles = pa.table(heavy_vehicles_data)
    
    # Double the toll rates for heavy vehicles using MockDoubleValueNode
    if heavy_vehicles.num_rows > 0:
        double_node = MockDoubleValueNode(
            input_table=heavy_vehicles,
            column_name="weight_kg",  # Double the weight for toll calculation
            output_column_name="adjusted_weight",
            output_name="heavy_vehicles"
        )
        
        heavy_vehicle_results = double_node.execute()
        heavy_vehicles_adjusted = heavy_vehicle_results["heavy_vehicles"]
        print(f"  Adjusted toll rates for {heavy_vehicles_adjusted.num_rows} heavy vehicles")
    else:
        print("  No heavy vehicles found for toll adjustment")
        heavy_vehicles_adjusted = None
        
    vehicle_time = time.time() - start_time
    
    # Step 2b: Process payment data - E-ZPass efficiency analysis
    print("\n💰 Step 2b: Processing payment stream (E-ZPass efficiency)...")
    start_time = time.time()
    
    # Convert to pandas for easier analysis in standalone demo
    payment_df = payment_data.to_pandas()
    
    # Calculate transaction metrics by payment method
    payment_metrics = payment_df.groupby("payment_method").agg(
        transaction_count=("transaction_id", "count"),
        total_revenue=("amount", "sum"),
        avg_transaction=("amount", "mean")
    ).reset_index().sort_values("transaction_count", ascending=False)
    
    # Payment processing time efficiency
    processing_times = pd.DataFrame({
        "payment_method": ["ezpass", "cash", "credit", "debit"],
        "avg_seconds": [2.1, 15.3, 11.2, 10.8]
    })
    
    # Merge with payment data to calculate total processing times
    efficiency_metrics = payment_df.groupby("payment_method").size().reset_index(name="transaction_count")
    efficiency_metrics = efficiency_metrics.merge(processing_times, on="payment_method")
    efficiency_metrics["total_processing_seconds"] = efficiency_metrics["transaction_count"] * efficiency_metrics["avg_seconds"]
    
    payment_time = time.time() - start_time
    
    # Step 2c: Process temporal data - Traffic patterns by time
    print("\n🕒 Step 2c: Processing temporal stream (Traffic patterns)...")
    start_time = time.time()
    
    # Convert to pandas for easier analysis in standalone demo
    temporal_df = temporal_data.to_pandas()
    
    # Extract hour of day for analysis
    temporal_df["hour_of_day"] = temporal_df["timestamp"].dt.hour
    temporal_df["day_of_week"] = temporal_df["timestamp"].dt.dayofweek
    
    # Calculate hourly patterns
    hourly_patterns = temporal_df.groupby("hour_of_day").size().reset_index(name="transaction_count")
    
    # Calculate day of week patterns
    daily_patterns = temporal_df.groupby("day_of_week").size().reset_index(name="transaction_count")
    
    temporal_time = time.time() - start_time
    
    # Step 3: Fan in the results for final analysis
    print("\n🔄 Step 3: Fanning in results for staffing and revenue optimization...")
    start_time = time.time()
    
    # Staffing recommendations based on hourly patterns
    staffing_recommendations = hourly_patterns.copy()
    staffing_recommendations["min_staff_needed"] = np.ceil(staffing_recommendations["transaction_count"] / 40).astype(int)
    staffing_recommendations["is_peak_hour"] = staffing_recommendations["hour_of_day"].isin([7, 8, 9, 17, 18, 19])
    
    # Revenue projection by combining heavy vehicle and payment data
    if heavy_vehicles_adjusted is not None:
        # Convert to pandas
        heavy_df = heavy_vehicles_adjusted.to_pandas()
        payment_df = payment_data.to_pandas()
        
        # Get transaction IDs for heavy vehicles
        heavy_transaction_ids = heavy_df["transaction_id"].tolist()
        
        # Mark heavy vehicle transactions in payment data
        payment_df["is_heavy"] = payment_df["transaction_id"].isin(heavy_transaction_ids)
        
        # Calculate adjusted revenue
        payment_df["projected_amount"] = payment_df["amount"]
        payment_df.loc[payment_df["is_heavy"], "projected_amount"] = payment_df.loc[payment_df["is_heavy"], "amount"] * 1.5
        
        # Aggregate
        current_revenue = payment_df["amount"].sum()
        projected_revenue = payment_df["projected_amount"].sum()
        percent_increase = (projected_revenue / current_revenue - 1) * 100
        
        revenue_projection = {
            "current_revenue": current_revenue,
            "projected_revenue": projected_revenue,
            "percent_increase": percent_increase
        }
    else:
        revenue_projection = None
    
    fan_in_time = time.time() - start_time
    
    # Display results
    print("\n📊 Results:")
    
    print("\nVehicle Stream Sample (first 3 records):")
    print(vehicle_data.to_pandas().head(3).to_string())
    
    print("\nPayment Stream Sample (first 3 records):")
    print(payment_data.to_pandas().head(3).to_string())
    
    print("\nTemporal Stream Sample (first 3 records):")
    print(temporal_data.to_pandas().head(3).to_string())
    
    if heavy_vehicles_adjusted is not None:
        print("\nHeavy Vehicles Sample (with adjusted weights, first 3 records):")
        print(heavy_vehicles_adjusted.to_pandas().head(3).to_string())
    
    print("\nPayment Method Analysis:")
    print(tabulate(payment_metrics, headers='keys', tablefmt='psql', showindex=False))
    
    print("\nE-ZPass Efficiency Metrics:")
    print(tabulate(efficiency_metrics, headers='keys', tablefmt='psql', showindex=False))
    
    print("\nHourly Traffic Patterns (first 5 hours):")
    print(tabulate(hourly_patterns.head(5), headers='keys', tablefmt='psql', showindex=False))
    
    print("\nStaffing Recommendations (first 5 hours):")
    print(tabulate(staffing_recommendations.head(5), headers='keys', tablefmt='psql', showindex=False))
    
    if revenue_projection:
        print("\nRevenue Projection with Heavy Vehicle Adjustments:")
        print(f"Current Revenue: ${revenue_projection['current_revenue']:.2f}")
        print(f"Projected Revenue: ${revenue_projection['projected_revenue']:.2f}")
        print(f"Percent Increase: {revenue_projection['percent_increase']:.2f}%")
    
    # Print performance information
    print("\n⏱️ Performance Summary:")
    print(f"  Fan-out operation: {fan_out_time:.4f} seconds")
    print(f"  Vehicle stream processing: {vehicle_time:.4f} seconds")
    print(f"  Payment stream processing: {payment_time:.4f} seconds")
    print(f"  Temporal stream processing: {temporal_time:.4f} seconds")
    print(f"  Fan-in operation: {fan_in_time:.4f} seconds")
    print(f"  Total execution time: {fan_out_time + vehicle_time + payment_time + temporal_time + fan_in_time:.4f} seconds")
    
    print("\n✅ Demo complete!")
    
    return {
        "vehicle_data": vehicle_data,
        "payment_data": payment_data,
        "temporal_data": temporal_data,
        "heavy_vehicles": heavy_vehicles_adjusted,
        "payment_metrics": payment_metrics,
        "efficiency_metrics": efficiency_metrics,
        "hourly_patterns": hourly_patterns,
        "staffing_recommendations": staffing_recommendations,
        "revenue_projection": revenue_projection
    }


if __name__ == "__main__":
    run_fan_out_pipeline_demo()