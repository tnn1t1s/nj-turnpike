"""
NJ Turnpike Fan-Out Pipeline Demo

This demo showcases a data fan-out architecture for toll plaza operations,
splitting traffic data into three parallel processing streams and then
recombining the results for operational insights.
"""

import pyarrow as pa
import ibis
import time
import numpy as np
from datetime import datetime, timedelta

# Import from the main Turnpike package
from turnpike.pipeline import Pipeline
from turnpike.node import Node
from turnpike.flight.server import run_flight_server
from turnpike.flight.client_node import FlightNode

# Import our custom nodes
from nj_turnpike.identity_node import IdentityNode
from nj_turnpike.double_flight_node import DoubleValueNode


class TollDataFanOutNode:
    """
    A specialized node that fans out toll plaza data into three distinct streams.
    
    Since the FlightNode no longer supports register_output, this implementation
    now creates three separate FlightNodes for each of the outputs.
    
    Outputs three tables:
    - vehicle_data: Information about vehicle types and classifications
    - payment_data: Information about payment methods and transactions
    - temporal_data: Time-based patterns and metrics
    """
    
    def __init__(self, input_table: pa.Table):
        """
        Initialize a TollDataFanOutNode.
        
        Args:
            input_table: The PyArrow table with toll plaza data
        """
        self.input_table = input_table
        
        # SQL for vehicle data
        vehicle_sql = """
        SELECT 
            transaction_id,
            plaza_id,
            vehicle_class,
            vehicle_type,
            axle_count,
            weight_kg,
            height_cm,
            length_cm,
            timestamp
        FROM input_table
        """
        
        # SQL for payment data
        payment_sql = """
        SELECT 
            transaction_id,
            plaza_id,
            payment_method,
            is_ezpass,
            amount,
            timestamp
        FROM input_table
        """
        
        # SQL for temporal data
        temporal_sql = """
        SELECT 
            transaction_id,
            plaza_id,
            timestamp,
            EXTRACT(HOUR FROM timestamp) AS hour_of_day,
            EXTRACT(DOW FROM timestamp) AS day_of_week,
            EXTRACT(MONTH FROM timestamp) AS month
        FROM input_table
        """
        
        # Create the vehicle data node
        self.vehicle_node = FlightNode(
            expression=vehicle_sql,
            input_table=input_table,
            output_name="vehicle_data"
        )
        
        # Create the payment data node
        self.payment_node = FlightNode(
            expression=payment_sql,
            input_table=input_table,
            output_name="payment_data"
        )
        
        # Create the temporal data node
        self.temporal_node = FlightNode(
            expression=temporal_sql,
            input_table=input_table,
            output_name="temporal_data"
        )
    
    def execute(self) -> dict[str, pa.Table]:
        """
        Execute all three nodes and combine their outputs.
        
        Returns:
            Dict[str, pa.Table]: Dictionary with all three outputs
        """
        # Execute all three nodes
        vehicle_result = self.vehicle_node.execute()
        payment_result = self.payment_node.execute()
        temporal_result = self.temporal_node.execute()
        
        # Combine the results
        return {
            "vehicle_data": vehicle_result["vehicle_data"],
            "payment_data": payment_result["payment_data"],
            "temporal_data": temporal_result["temporal_data"]
        }


class TollPlazaFanOutPipeline(Pipeline):
    """
    Example pipeline that demonstrates fan-out architecture with NJ Turnpike data.
    
    This pipeline:
    1. Creates a dataset of toll plaza transactions
    2. Fans out the data into vehicle, payment, and temporal streams
    3. Processes each stream separately:
       - Doubles toll rates for heavy vehicles
       - Analyzes E-ZPass vs cash payment efficiency
       - Examines traffic patterns by time periods
    4. Fans in the results to optimize staffing and predict revenue
    """
    
    def __init__(self):
        super().__init__()
        self.con = ibis.duckdb.connect()
        
    def build(self):
        """Define the pipeline's DAG structure."""
        # Create sample toll plaza data
        print("🚗 Creating sample toll plaza data...")
        
        # Generate timestamps for a day's worth of transactions
        base_time = datetime(2023, 5, 1, 0, 0, 0)
        timestamps = []
        
        for i in range(200):
            # Create timestamps throughout the day, with more during rush hours
            # Generate a list of all hours, then calculate probabilities that sum to 1
            all_hours = [7, 8, 9, 12, 17, 18, 19] + list(range(24))
            
            # Assign more weight to peak hours
            weights = []
            for h in all_hours:
                if h in [7, 8, 9, 17, 18, 19]:
                    weights.append(0.07)  # Higher probability for peak hours
                elif h == 12:
                    weights.append(0.04)  # Medium probability for lunch hour
                else:
                    weights.append(0.015)  # Lower probability for other hours
                    
            # Normalize weights to ensure they sum to 1
            total_weight = sum(weights)
            probabilities = [w/total_weight for w in weights]
            
            hour = np.random.choice(all_hours, p=probabilities)
            minute = np.random.randint(0, 60)
            second = np.random.randint(0, 60)
            transaction_time = base_time.replace(hour=hour, minute=minute, second=second)
            timestamps.append(transaction_time)
        
        # Generate random vehicle weights (kg) based on vehicle class
        vehicle_classes = np.random.choice([1, 2, 3, 4, 5], size=200, p=[0.4, 0.3, 0.15, 0.1, 0.05])
        
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
        is_ezpass = np.random.choice([True, False], size=200, p=[0.7, 0.3])
        payment_methods = []
        for ez in is_ezpass:
            if ez:
                payment_methods.append("ezpass")
            else:
                payment_methods.append(np.random.choice(["cash", "credit", "debit"], p=[0.6, 0.3, 0.1]))
        
        # Generate toll amounts based on vehicle class and payment method
        amounts = []
        for i in range(200):
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
        self.toll_data = pa.table({
            "transaction_id": list(range(10001, 10201)),
            "plaza_id": np.random.choice([1, 2, 3, 4, 5], size=200),
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
        
        # Register the toll data
        self.register_node("toll_data", self.toll_data)
        
        # Mark build phase complete
        self._built = True
        print("✅ Pipeline DAG built successfully")
        
    def run(self):
        """Execute the pipeline."""
        if not self._built:
            raise ValueError("Pipeline must be built before running")
            
        print("\n🚀 Executing pipeline...")
        
        # Step 1: Fan out the toll data into three streams
        print("\n🔀 Step 1: Fanning out toll data into three streams...")
        start_time = time.time()
        
        fan_out_node = TollDataFanOutNode(
            input_table=self.nodes["toll_data"]
        )
        
        fan_out_results = fan_out_node.execute()
        vehicle_data = fan_out_results["vehicle_data"]
        payment_data = fan_out_results["payment_data"]
        temporal_data = fan_out_results["temporal_data"]
        
        fan_out_time = time.time() - start_time
        
        self.outputs["vehicle_data"] = vehicle_data
        self.outputs["payment_data"] = payment_data
        self.outputs["temporal_data"] = temporal_data
        
        print(f"  Vehicle stream: {vehicle_data.num_rows} records")
        print(f"  Payment stream: {payment_data.num_rows} records")
        print(f"  Temporal stream: {temporal_data.num_rows} records")
        
        # Step 2a: Process vehicle data - Double toll rates for heavy vehicles
        print("\n🚛 Step 2a: Processing vehicle stream (Heavy vehicle toll adjustment)...")
        start_time = time.time()
        
        # Register vehicle data in DuckDB
        self.con.create_table("vehicle_data", vehicle_data, overwrite=True)
        
        # Find heavy vehicles (class 4-5)
        heavy_vehicle_expr = self.con.table("vehicle_data").filter(
            (self.con.table("vehicle_data").vehicle_class >= 4)
        )
        
        heavy_node = Node(heavy_vehicle_expr, self.con)
        heavy_vehicles = heavy_node.execute()["default"]
        
        # Double the toll rates for heavy vehicles using DoubleValueNode
        if heavy_vehicles.num_rows > 0:
            double_node = DoubleValueNode(
                input_table=heavy_vehicles,
                column_name="weight_kg",  # Double the weight for toll calculation
                output_column_name="adjusted_weight",
                output_name="heavy_vehicles"
            )
            
            heavy_vehicle_results = double_node.execute()
            self.outputs["heavy_vehicles"] = heavy_vehicle_results["heavy_vehicles"]
            print(f"  Adjusted toll rates for {heavy_vehicle_results['heavy_vehicles'].num_rows} heavy vehicles")
        else:
            print("  No heavy vehicles found for toll adjustment")
            
        vehicle_time = time.time() - start_time
        
        # Step 2b: Process payment data - E-ZPass efficiency analysis
        print("\n💰 Step 2b: Processing payment stream (E-ZPass efficiency)...")
        start_time = time.time()
        
        # Register payment data in DuckDB
        self.con.create_table("payment_data", payment_data, overwrite=True)
        
        # Calculate transaction metrics by payment method
        payment_table = self.con.table("payment_data")
        payment_expr = (
            payment_table.group_by("payment_method")
            .aggregate({
                "transaction_count": payment_table.count(),
                "total_revenue": payment_table.amount.sum(),
                "avg_transaction": payment_table.amount.mean()
            })
            .order_by(payment_table.count().desc())
        )
        
        payment_node = Node(payment_expr, self.con)
        payment_metrics = payment_node.execute()["default"]
        self.outputs["payment_metrics"] = payment_metrics
        
        # E-ZPass vs cash processing time analysis
        # First create a table with processing times based on payment method
        self.con.create_table("processing_times", pa.table({
            "payment_method": ["ezpass", "cash", "credit", "debit"],
            "avg_seconds": [2.1, 15.3, 11.2, 10.8]
        }), overwrite=True)
        
        # Use direct SQL for efficiency metrics rather than ibis 
        # since the Ibis API has changed significantly
        direct_sql = """
        SELECT 
            pm.payment_method,
            pm.transaction_count,
            pm.transaction_count * pt.avg_seconds as total_processing_seconds
        FROM (
            SELECT 
                payment_method,
                COUNT(*) as transaction_count
            FROM payment_data
            GROUP BY payment_method
        ) pm
        JOIN processing_times pt ON pm.payment_method = pt.payment_method
        """
        
        efficiency_metrics = self.con.raw_sql(direct_sql).fetch_arrow_table()
        self.outputs["efficiency_metrics"] = efficiency_metrics
        
        payment_time = time.time() - start_time
        
        # Step 2c: Process temporal data - Traffic patterns by time
        print("\n🕒 Step 2c: Processing temporal stream (Traffic patterns)...")
        start_time = time.time()
        
        # Register temporal data in DuckDB
        self.con.create_table("temporal_data", temporal_data, overwrite=True)
        
        # Calculate traffic patterns by hour
        temporal_table = self.con.table("temporal_data")
        hourly_pattern_expr = (
            temporal_table
            .group_by("hour_of_day")
            .aggregate({
                "transaction_count": temporal_table.count()
            })
            .order_by("hour_of_day")
        )
        
        hourly_pattern_node = Node(hourly_pattern_expr, self.con)
        hourly_patterns = hourly_pattern_node.execute()["default"]
        self.outputs["hourly_patterns"] = hourly_patterns
        
        # Calculate traffic patterns by day of week
        daily_pattern_expr = (
            temporal_table
            .group_by("day_of_week")
            .aggregate({
                "transaction_count": temporal_table.count()
            })
            .order_by("day_of_week")
        )
        
        daily_pattern_node = Node(daily_pattern_expr, self.con)
        daily_patterns = daily_pattern_node.execute()["default"]
        self.outputs["daily_patterns"] = daily_patterns
        
        temporal_time = time.time() - start_time
        
        # Step 3: Fan in the results for final analysis
        print("\n🔄 Step 3: Fanning in results for staffing and revenue optimization...")
        start_time = time.time()
        
        # Join temporal patterns with payment efficiency to optimize staffing
        # Create a staffing recommendation table
        self.con.create_table("hourly_patterns", hourly_patterns, overwrite=True)
        self.con.create_table("efficiency_metrics", efficiency_metrics, overwrite=True)
        
        # Staffing recommendations - simplified formula for demo purposes
        hourly_table = self.con.table("hourly_patterns")
        staffing_expr = (
            hourly_table
            .mutate({
                # Calculate base staffing needed (simplistic formula for demo)
                "min_staff_needed": (hourly_table.transaction_count / 40).ceil().cast("int"),
                # Classify as peak or off-peak hours
                "is_peak_hour": hourly_table.hour_of_day.isin([7, 8, 9, 17, 18, 19])
            })
        )
        
        staffing_node = Node(staffing_expr, self.con)
        staffing_recommendations = staffing_node.execute()["default"]
        self.outputs["staffing_recommendations"] = staffing_recommendations
        
        # Combine vehicle and payment data for revenue projection
        # This demonstrates the culmination of the fan-out/fan-in pattern
        if "heavy_vehicles" in self.outputs:
            # Register heavy vehicle data
            self.con.create_table("heavy_vehicles", self.outputs["heavy_vehicles"], overwrite=True)
            
            # Use direct SQL for revenue projection
            revenue_sql = """
            WITH projected AS (
                SELECT 
                    p.transaction_id,
                    p.amount,
                    CASE 
                        WHEN h.adjusted_weight IS NOT NULL THEN p.amount * 1.5
                        ELSE p.amount
                    END AS projected_amount
                FROM payment_data p
                LEFT JOIN heavy_vehicles h ON p.transaction_id = h.transaction_id
            )
            SELECT 
                SUM(amount) AS current_revenue,
                SUM(projected_amount) AS projected_revenue,
                (SUM(projected_amount) / SUM(amount) - 1) * 100 AS percent_increase
            FROM projected
            """
            
            revenue_projection = self.con.raw_sql(revenue_sql).fetch_arrow_table()
            self.outputs["revenue_projection"] = revenue_projection
        
        fan_in_time = time.time() - start_time
        
        # Print performance information
        print("\n⏱️ Performance Summary:")
        print(f"  Fan-out operation: {fan_out_time:.4f} seconds")
        print(f"  Vehicle stream processing: {vehicle_time:.4f} seconds")
        print(f"  Payment stream processing: {payment_time:.4f} seconds")
        print(f"  Temporal stream processing: {temporal_time:.4f} seconds")
        print(f"  Fan-in operation: {fan_in_time:.4f} seconds")
        print(f"  Total execution time: {fan_out_time + vehicle_time + payment_time + temporal_time + fan_in_time:.4f} seconds")
        
        self._ran = True
        return self.outputs


def run_demo():
    """Run the NJ Turnpike fan-out pipeline demo."""
    print("🚗 Starting NJ Turnpike Fan-Out Pipeline Demo")
    
    # Start the Flight server for remote execution
    print("\n💫 Starting Flight server...")
    run_flight_server()
    time.sleep(1)  # Give server time to start
    
    # Create and run the pipeline
    pipeline = TollPlazaFanOutPipeline()
    
    # Build the pipeline
    pipeline.build()
    
    # Run the pipeline
    results = pipeline.run()
    
    # Display results
    print("\n📊 Results:")
    
    print("\nVehicle Stream Sample:")
    print(results["vehicle_data"].to_pandas().head(3))
    
    print("\nPayment Stream Sample:")
    print(results["payment_data"].to_pandas().head(3))
    
    print("\nTemporal Stream Sample:")
    print(results["temporal_data"].to_pandas().head(3))
    
    if "heavy_vehicles" in results:
        print("\nHeavy Vehicles Sample (with adjusted weights):")
        print(results["heavy_vehicles"].to_pandas().head(3))
    
    print("\nPayment Method Analysis:")
    print(results["payment_metrics"].to_pandas())
    
    print("\nE-ZPass Efficiency Metrics:")
    print(results["efficiency_metrics"].to_pandas())
    
    print("\nHourly Traffic Patterns:")
    print(results["hourly_patterns"].to_pandas().head(5))
    
    print("\nStaffing Recommendations:")
    print(results["staffing_recommendations"].to_pandas().head(5))
    
    if "revenue_projection" in results:
        print("\nRevenue Projection with Heavy Vehicle Adjustments:")
        print(results["revenue_projection"].to_pandas())
    
    print("\n✅ Demo complete!")


if __name__ == "__main__":
    run_demo()