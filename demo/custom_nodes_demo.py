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
try:
    # When installed as a package
    from nj_turnpike.identity_node import IdentityNode
    from nj_turnpike.double_flight_node import DoubleValueNode
except ImportError:
    # When running from the repository root
    from src.identity_node import IdentityNode
    from src.double_flight_node import DoubleValueNode


class TollDataFanOutNode(FlightNode):
    """
    A specialized FlightNode that fans out toll plaza data into three distinct streams.
    
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
        # SQL that splits the data into three different aspects
        sql = """
        -- Vehicle data (first output)
        WITH vehicle_data AS (
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
        ),
        
        -- Payment data (second output)
        payment_data AS (
            SELECT 
                transaction_id,
                plaza_id,
                payment_method,
                is_ezpass,
                amount,
                timestamp
            FROM input_table
        ),
        
        -- Temporal data (third output)
        temporal_data AS (
            SELECT 
                transaction_id,
                plaza_id,
                timestamp,
                EXTRACT(HOUR FROM timestamp) AS hour_of_day,
                EXTRACT(DOW FROM timestamp) AS day_of_week,
                EXTRACT(MONTH FROM timestamp) AS month
            FROM input_table
        )
        
        -- Return vehicle_data as main output
        SELECT * FROM vehicle_data
        """
        
        # Initialize the parent FlightNode with our custom SQL
        super().__init__(
            expression=sql,
            input_table=input_table,
            output_name="vehicle_data"
        )
        
        # Create additional outputs
        self.register_output("payment_data", """
            SELECT 
                transaction_id,
                plaza_id,
                payment_method,
                is_ezpass,
                amount,
                timestamp
            FROM input_table
        """)
        
        self.register_output("temporal_data", """
            SELECT 
                transaction_id,
                plaza_id,
                timestamp,
                EXTRACT(HOUR FROM timestamp) AS hour_of_day,
                EXTRACT(DOW FROM timestamp) AS day_of_week,
                EXTRACT(MONTH FROM timestamp) AS month
            FROM input_table
        """)


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
            hour = np.random.choice(
                [7, 8, 9, 12, 17, 18, 19] + list(range(24)), 
                p=[0.1, 0.1, 0.1, 0.05, 0.1, 0.1, 0.1, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016]
            )
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
        payment_expr = self.con.table("payment_data").group_by("payment_method").aggregate([
            self.con.table("payment_data").count().name("transaction_count"),
            self.con.table("payment_data").amount.sum().name("total_revenue"),
            self.con.table("payment_data").amount.mean().name("avg_transaction")
        ]).order_by(self.con.table("payment_data").count().desc())
        
        payment_node = Node(payment_expr, self.con)
        payment_metrics = payment_node.execute()["default"]
        self.outputs["payment_metrics"] = payment_metrics
        
        # E-ZPass vs cash processing time analysis
        # First create a table with processing times based on payment method
        self.con.create_table("processing_times", pa.table({
            "payment_method": ["ezpass", "cash", "credit", "debit"],
            "avg_seconds": [2.1, 15.3, 11.2, 10.8]
        }), overwrite=True)
        
        # Join payment data with processing times
        efficiency_expr = (
            self.con.table("payment_data")
            .join(
                self.con.table("processing_times"),
                self.con.table("payment_data").payment_method == self.con.table("processing_times").payment_method
            )
            .group_by([self.con.table("payment_data").payment_method])
            .aggregate([
                self.con.table("payment_data").count().name("transaction_count"),
                (self.con.table("payment_data").count() * self.con.table("processing_times").avg_seconds).name("total_processing_seconds")
            ])
        )
        
        efficiency_node = Node(efficiency_expr, self.con)
        efficiency_metrics = efficiency_node.execute()["default"]
        self.outputs["efficiency_metrics"] = efficiency_metrics
        
        payment_time = time.time() - start_time
        
        # Step 2c: Process temporal data - Traffic patterns by time
        print("\n🕒 Step 2c: Processing temporal stream (Traffic patterns)...")
        start_time = time.time()
        
        # Register temporal data in DuckDB
        self.con.create_table("temporal_data", temporal_data, overwrite=True)
        
        # Calculate traffic patterns by hour
        hourly_pattern_expr = (
            self.con.table("temporal_data")
            .group_by("hour_of_day")
            .aggregate([
                self.con.table("temporal_data").count().name("transaction_count")
            ])
            .order_by("hour_of_day")
        )
        
        hourly_pattern_node = Node(hourly_pattern_expr, self.con)
        hourly_patterns = hourly_pattern_node.execute()["default"]
        self.outputs["hourly_patterns"] = hourly_patterns
        
        # Calculate traffic patterns by day of week
        daily_pattern_expr = (
            self.con.table("temporal_data")
            .group_by("day_of_week")
            .aggregate([
                self.con.table("temporal_data").count().name("transaction_count")
            ])
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
        staffing_expr = (
            self.con.table("hourly_patterns")
            .mutate([
                # Calculate base staffing needed (simplistic formula for demo)
                (self.con.table("hourly_patterns").transaction_count / 40).ceil().cast("int").name("min_staff_needed"),
                # Classify as peak or off-peak hours
                self.con.table("hourly_patterns").hour_of_day.isin([7, 8, 9, 17, 18, 19]).name("is_peak_hour")
            ])
        )
        
        staffing_node = Node(staffing_expr, self.con)
        staffing_recommendations = staffing_node.execute()["default"]
        self.outputs["staffing_recommendations"] = staffing_recommendations
        
        # Combine vehicle and payment data for revenue projection
        # This demonstrates the culmination of the fan-out/fan-in pattern
        if "heavy_vehicles" in self.outputs:
            # Register heavy vehicle data
            self.con.create_table("heavy_vehicles", self.outputs["heavy_vehicles"], overwrite=True)
            
            # Calculate projected revenue
            revenue_expr = (
                self.con.table("payment_data")
                .join(
                    self.con.table("heavy_vehicles"),
                    self.con.table("payment_data").transaction_id == self.con.table("heavy_vehicles").transaction_id,
                    how="left"
                )
                .mutate([
                    # If it's a heavy vehicle with adjusted weight, increase the rate
                    self.con.case()
                    .when(self.con.table("heavy_vehicles").adjusted_weight.notnull(), 
                          self.con.table("payment_data").amount * 1.5)
                    .else_(self.con.table("payment_data").amount)
                    .end().name("projected_amount")
                ])
                .group_by([])
                .aggregate([
                    self.con.table("payment_data").amount.sum().name("current_revenue"),
                    self.con.expr("sum(projected_amount)").name("projected_revenue"),
                    (self.con.expr("sum(projected_amount)") / self.con.table("payment_data").amount.sum() - 1).name("percent_increase")
                ])
            )
            
            revenue_node = Node(revenue_expr, self.con)
            revenue_projection = revenue_node.execute()["default"]
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