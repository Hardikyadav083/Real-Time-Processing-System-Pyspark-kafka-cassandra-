import findspark
findspark.init()
from flask import Flask, render_template, request, redirect, session, jsonify,send_file,Response, make_response
from apscheduler.schedulers.background import BackgroundScheduler
import mysql.connector
import os
import json
import random
import time
from pyspark.sql import SparkSession
from threading import Thread
import time
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from confluent_kafka import Producer
from confluent_kafka import Consumer
import spark_kafka_integration
import fake_data
import pandas as pd
from io import BytesIO
import csv
from kafka import KafkaProducer

app = Flask(__name__)
app.secret_key = os.urandom(24)
cluster = Cluster(['127.0.0.1'])  
cassandra_session = cluster.connect('ecom_data')
kafka_bootstrap_servers = ['localhost:9092']
kafka_topic = 'ecom'
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# Web routes
@app.route('/')
def first():
    return render_template('first.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/home')
def home():
    if 'user_id' in session:
        return render_template('home.html')
    else:
        return redirect('/login')

@app.route('/Analysis')
def analysis():
    return render_template('Analysis.html')

@app.route('/login_validation', methods=['POST', 'GET'])
def login_validation():
    error_message = None
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        cursor.execute("SELECT * FROM `register` WHERE `email` = %s AND `password` = %s", (email, password))
        user = cursor.fetchall()
        if len(user) > 0:
            session['user_id'] = user[0][0]

            # Set a cookie with the user's name
            response = make_response(redirect('/home'))
            response.set_cookie('user_name', user[0][1])  # Assuming the user's name is in the second column of the table
            return response
        else:
            error_message = "Invalid email or password"
    return render_template('login.html', error_message=error_message)

@app.route('/register', methods=['POST', 'GET'])
def register():
    error_message = None
    if request.method == 'POST':
        fullName = request.form.get('ufname')
        email = request.form.get('uemail')
        password = request.form.get('upassword')
        if not fullName or not email or not password:
            error_message = "Please fill in all fields."
        else:
            select_query = "SELECT * FROM `register` WHERE `email` = %s"
            cursor.execute(select_query, (email,))
            existing_user = cursor.fetchone()
            if existing_user:
                error_message = "User already exists with this email."
            else:
                insert_query = "INSERT INTO `register` (`fullName`, `email`, `password`) VALUES (%s, %s, %s)"
                values = (fullName, email, password)
                cursor.execute(insert_query, values)
                conn.commit()
                select_query = "SELECT * FROM `register` WHERE `email` = %s"
                cursor.execute(select_query, (email,))
                myuser = cursor.fetchone()
                session['user_id'] = myuser[0]

                # Set a cookie with the user's name
                response = make_response(redirect('/home'))
                response.set_cookie('user_name', myuser[1])  # Assuming the user's name is in the second column of the table
                return response
    return render_template('signup.html', error_message=error_message)

@app.route('/logout')
def logout():
    if 'user_id' in session:
        session.pop('user_id')
    return redirect('/login')


@app.route('/second_analysis')
def second_analysis():
    return render_template('SecondAnalysis.html')

@app.route('/load_dashboard', methods=['POST'])
def load_dashboard():
    if 'user_id' in session:
        return render_template('loading_dashboard.html')
    else:
        return redirect('/login')

@app.route('/loadingdata', methods=['POST'])
def load_data():
    if 'user_id' in session:
        def generate_fake_ecom_events():
            while True:
                fake_data.simulate_ecommerce_events()
                time.sleep(random.uniform(0.1, 2.0))

        def process_kafka_stream():
            for batch_id in spark_kafka_integration.process_kafka_stream():
                print(f"Processed batch ID: {batch_id}")
                # You can log or print the batch ID as needed

        # Start the threads
        event_generation_thread = Thread(target=generate_fake_ecom_events)
        spark_processing_thread = Thread(target=process_kafka_stream)

        event_generation_thread.start()
        spark_processing_thread.start()

        return render_template('loading_dashboard.html')
    else:
        return redirect('/login')

@app.route('/get_gender_data')
def get_gender_data():
    query = "SELECT gender, SUM(customer_count) as total_count FROM gender_counts GROUP BY gender"
    result = cassandra_session.execute(query)

    gender_data = {'maleCount': 0, 'femaleCount': 0}

    for row in result:
        gender = row.gender.lower()
        count = row.total_count
        if gender == 'male':
            gender_data['maleCount'] = count
        elif gender == 'female':
            gender_data['femaleCount'] = count

    return jsonify(gender_data)

@app.route('/get_product_performance_data')
def get_product_performance_data():
    query = "SELECT transaction_date_readable, quantity_sold, total_sales_amount FROM product_performance LIMIT 15"
    result = cassandra_session.execute(query)

    product_performance_data = {'dates': [], 'quantitySold': [], 'totalSalesAmount': []}

    for row in result:
        product_performance_data['dates'].append(row.transaction_date_readable)
        product_performance_data['quantitySold'].append(row.quantity_sold)
        product_performance_data['totalSalesAmount'].append(row.total_sales_amount)

    return jsonify(product_performance_data)

@app.route('/get_product_profitability')
def get_rfm_data():
    query = "SELECT total_sales_amount,total_discount_amount FROM product_profitability LIMIT 15"
    result = cassandra_session.execute(query)

    product_profitability_data = []

    for row in result:
        product_profitability_data.append({
            'total_sales_amount': row.total_sales_amount,
            'total_discount_amount': row.total_discount_amount,
        })

    return jsonify(product_profitability_data)

@app.route('/get_payment_method_data')
def get_payment_method_data():
    # Fetch data from Cassandra

    query = "SELECT * FROM payment_method_counts"
    rows = cassandra_session.execute(query)

    # Convert data to a Pandas DataFrame
    data = [(row.payment_method,row.customer_count) for row in rows]
    df = pd.DataFrame(data, columns=['payment_method', 'customer_count'])

    # Return data as JSON
    return jsonify(df.to_dict(orient='records'))

@app.route('/get_location_performance_data')
def get_combined_data():
    # Fetch data from Cassandra for location performance
    location_query = "SELECT location_store, total_sales_amount, total_quantity_sold FROM location_performance;"
    location_rows = cassandra_session.execute(location_query)
    location_data = [(row.location_store, row.total_sales_amount, row.total_quantity_sold) for row in location_rows]

    # Convert data to Pandas DataFrame
    location_df = pd.DataFrame(location_data, columns=['location_store', 'total_sales_amount', 'total_quantity_sold'])

    # Combine the data into a single JSON response
    combined_data = {
        'location_data': location_df.to_dict(orient='records'),
    } 

    return jsonify(combined_data)

@app.route('/get_monetary_data')
def get_monetary_data_route():
    try:
        # Execute the CQL query
        query = "SELECT monetary FROM rfm"
        result = cassandra_session.execute(query)

        # Fetch the results and format them into a list of dictionaries
        monetary_data = [{'monetary': row.monetary} for row in result]

        return jsonify(monetary_data)

    except Exception as e:
        # Handle exceptions, log errors, etc.
        print(f"Error fetching monetary data: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500
dashboard_metrics = {}
    
@app.route('/get_dashboard_data', methods=['GET'])
def get_dashboard_data():
    # Fetch most recent data from Cassandra
    query = "SELECT * FROM revenue_total_sales"
    result = cassandra_session.execute(query)

    # Check if there is any result
    if result:
        # Use ResultSet.one() to get a single row
        row = result.one()

        if row:
            # Convert Cassandra Row to Python dictionary
            data = dict(row._asdict())

            # Update the global dashboard metrics variable
            global dashboard_metrics
            dashboard_metrics = {
                'total_sales': data.get('total_sales', 0),
                'revenue': data.get('revenue', 0),
                'avg_sales': data.get('avg_sales', 0),
                'total_inventory_value': data.get('total_inventory_value', 0),
            }

            return jsonify(data)
        else:
            # Handle the case where there is no data
            return jsonify({})
    else:
        # Handle the case where there is no result
        return jsonify({})

@app.route('/get_inventory_turnover_data', methods=['GET'])
def get_inventory_turnover_data():
    # Fetch data from Cassandra table (replace with your logic)
    query = "SELECT product_name, inventory_turnover FROM inventory_turnover LIMIT 12"
    result = cassandra_session.execute(query)

    inventory_data = {'product_name': [], 'inventory_turnover': []}

    for row in result:
        inventory_data['product_name'].append(row.product_name)
        inventory_data['inventory_turnover'].append(row.inventory_turnover)

    return jsonify(inventory_data)

def get_payment_method_data():
    # Fetch data from Cassandra for payment method counts
    query = "SELECT * FROM payment_method_counts"
    rows = cassandra_session.execute(query)
    data = [(row.payment_method, row.customer_count) for row in rows]
    return data

def get_product_performance_data():
    # Fetch data from Cassandra for product performance
    query = "SELECT transaction_date_readable, quantity_sold, total_sales_amount FROM product_performance LIMIT 15"
    result = cassandra_session.execute(query)
    data = [(row.transaction_date_readable, row.quantity_sold, row.total_sales_amount) for row in result]
    return data

def get_gender_data():
    # Fetch data from Cassandra for gender counts
    query = "SELECT gender, SUM(customer_count) as total_count FROM gender_counts GROUP BY gender"
    result = cassandra_session.execute(query)
    gender_data = {'maleCount': 0, 'femaleCount': 0}
    for row in result:
        gender = row.gender.lower()
        count = row.total_count
        if gender == 'male':
            gender_data['maleCount'] = count
        elif gender == 'female':
            gender_data['femaleCount'] = count
    return gender_data

def get_monetary_data_route():
    # Fetch data from Cassandra for monetary data
    query = "SELECT monetary FROM rfm"
    result = cassandra_session.execute(query)
    monetary_data = [{'monetary': row.monetary} for row in result]
    return monetary_data

def get_product_profitability():
    # Fetch data from Cassandra for product profitability
    query = "SELECT total_sales_amount, total_discount_amount FROM product_profitability LIMIT 15"
    result = cassandra_session.execute(query)
    product_profitability_data = []
    for row in result:
        product_profitability_data.append({
            'total_sales_amount': row.total_sales_amount,
            'total_discount_amount': row.total_discount_amount,
        })
    return product_profitability_data

def get_combined_data():
    # Fetch data from Cassandra for location performance
    location_query = "SELECT location_store, total_sales_amount, total_quantity_sold FROM location_performance;"
    location_rows = cassandra_session.execute(location_query)
    location_data = [(row.location_store, row.total_sales_amount, row.total_quantity_sold) for row in location_rows]
    location_df = pd.DataFrame(location_data, columns=['location_store', 'total_sales_amount', 'total_quantity_sold'])
    combined_data = {'location_data': location_df.to_dict(orient='records')}
    return combined_data

def get_inventory_turnover_data():
    # Fetch data from Cassandra table (replace with your logic)
    query = "SELECT product_name, inventory_turnover FROM inventory_turnover LIMIT 12"
    result = cassandra_session.execute(query)

    inventory_data = {'product_name': [], 'inventory_turnover': []}

    for row in result:
        inventory_data['product_name'].append(row.product_name)
        inventory_data['inventory_turnover'].append(row.inventory_turnover)

    return inventory_data

def update_data():
    print("Updating data...")
    # Implement logic to fetch new data and update existing data variables
    global payment_method_data, product_performance_data
    payment_method_data = get_payment_method_data()
    product_performance_data = get_product_performance_data()
    gender_data = get_gender_data()
    monetary_data = get_monetary_data_route()
    product_profitability_data = get_product_profitability()
    location_performance_data = get_combined_data()
    inventory_turnover_data = get_inventory_turnover_data()

scheduler = BackgroundScheduler()
scheduler.add_job(update_data, trigger='interval', seconds=5)
scheduler.start()

# Initial data (replace this with your actual initial data)
payment_method_data = get_payment_method_data()
product_performance_data = get_product_performance_data()
gender_data = get_gender_data()
monetary_data = get_monetary_data_route()
product_profitability_data = get_product_profitability()
location_performance_data = get_combined_data()
inventory_turnover_data = get_inventory_turnover_data()

def generate_report_content():
    try:
        # Fetch data for each chart
        payment_method_data = get_payment_method_data()
        product_performance_data = get_product_performance_data()
        gender_data = get_gender_data()
        monetary_data = get_monetary_data_route()
        product_profitability_data = get_product_profitability()
        location_performance_data = get_combined_data()
        inventory_turnover_data = get_inventory_turnover_data()

        # Generate the report content
        report_content = f"**E-commerce Dashboard Insights Report**\n\n"

        # Chart 1: Bar Chart (Payment Method Counts)
        report_content += "1. **Payment Method Distribution Overview:**\n"
        report_content += "- Purpose: Illustrates the distribution of customers based on different payment methods.\n"
        report_content += "- Insights: Understanding the preferred payment methods helps optimize checkout processes and enhance user experience in the e-commerce platform.\n"
        report_content += f"- Payment Method Counts: {', '.join([f'{method}: {count} customers' for method, count in payment_method_data])}\n\n"

        # Chart 2: Line Chart (Product Performance)
        report_content += "2. **Product Performance Trends Analysis:**\n"
        report_content += "- Purpose: Tracks the quantity sold and total sales amount of products over time.\n"
        report_content += "- Insights: Analyzing product performance trends aids in identifying popular products, optimizing inventory, and planning marketing strategies.\n"
        report_content += f"- Product Performance Data: {', '.join([f'{date}: Quantity Sold - {quantity}, Total Sales - ${sales:,.2f}' for date, quantity, sales in product_performance_data])}\n\n"

        # Chart 3: Doughnut Chart (Gender Distribution)
        report_content += "3. **Customer Gender Distribution Overview:**\n"
        report_content += "- Purpose: Represents the distribution of customers based on gender.\n"
        report_content += "- Insights: Understanding the gender distribution assists in targeted marketing and product recommendations.\n"
        report_content += f"- Gender Distribution: Male Count - {gender_data['maleCount']} customers, Female Count - {gender_data['femaleCount']} customers\n\n"

        # Chart 4: Histogram Chart
        report_content += "4. **Monetary Value Distribution Analysis:**\n"
        report_content += "- Purpose: Displays the distribution of monetary values in the dataset.\n"
        report_content += "- Insights: Analyzing monetary value distribution helps in understanding customer spending patterns and tailoring pricing strategies.\n"
        report_content += "- Monetary Data: {}\n\n".format(', '.join([f'Monetary - ${item["monetary"]:,.2f}' for item in monetary_data]))

        # Chart 5: Scatter Chart (Product Profitability)
        report_content += "5. **Product Profitability Relationship Analysis:**\n"
        report_content += "- Purpose: Displays the relationship between total sales value and total discount value for different products.\n"
        report_content += "- Insights: Analyzing product profitability helps in optimizing pricing strategies and identifying products with higher margins.\n"
        report_content += "- Product Profitability Data: {}\n\n".format(', '.join([f'Total Sales - ${item["total_sales_amount"]:,.2f}, Total Discount - ${item["total_discount_amount"]:,.2f}' for item in product_profitability_data]))

        # Chart 6: Grouped Bar Chart (Location Performance)
        report_content += "6. **Location Performance Comparison:**\n"
        report_content += "- Purpose: Compares total sales amount and total quantity sold for different locations.\n"
        report_content += "- Insights: Analyzing location performance aids in optimizing inventory distribution and identifying regions with higher demand.\n"
        report_content += "- Location Performance Data: {}\n\n".format(', '.join([f'Location - {item["location_store"]}, Total Sales - ${item["total_sales_amount"]:,.2f}, Total Quantity Sold - {item["total_quantity_sold"]} units' for item in location_performance_data['location_data']]))

        report_content += "\n\n**Key E-commerce Metrics:**\n"
        report_content += f"- Total Sales: ${dashboard_metrics.get('total_sales', 0):,.2f}\n"
        report_content += f"- Revenue: ${dashboard_metrics.get('revenue', 0):,.2f}\n"
        report_content += f"- Average Sales: ${dashboard_metrics.get('avg_sales', 0):,.2f}\n"
        report_content += f"- Total Inventory Value: ${dashboard_metrics.get('total_inventory_value', 0):,.2f}\n\n"

        report_content += "**Inventory Turnover Data:**\n"
        report_content += "| Product Name | Inventory Turnover |\n"
        report_content += "|--------------|---------------------|\n"

        for i in range(len(inventory_turnover_data['product_name'])):
            product_name = inventory_turnover_data['product_name'][i]
            inventory_turnover = inventory_turnover_data['inventory_turnover'][i]
            report_content += f"| {product_name} | {inventory_turnover} |\n"

        return report_content

    except Exception as e:
        # Handle exceptions, log errors, etc.
        print(f"Error generating e-commerce dashboard insights report content: {e}")
        return "Error generating e-commerce dashboard insights report content"

    
@app.route('/download_report', methods=['GET'])
def download_report():
    try:
        # Generate the report content with the latest data
        report_content = generate_report_content()

        # Convert the string content to bytes
        report_bytes = report_content.encode('utf-8')

        # Create a BytesIO object to send the file in-memory
        buffer = BytesIO()

        # Write the bytes to the buffer
        buffer.write(report_bytes)

        # Seek to the beginning of the buffer
        buffer.seek(0)

        # Send the file as an attachment with a specific filename
        return send_file(buffer, download_name='chart_summaries_report.txt', as_attachment=True)

    except Exception as e:
        # Handle exceptions, log errors, etc.
        print(f"Error generating chart summaries report: {e}")
        return Response("Error generating chart summaries report", status=500, content_type='text/plain')
    

def analyze_csv_file(file_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("CSVProcessor").getOrCreate()

    # Read the CSV file using PySpark
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Initialize a variable to store the report text
    report_text = ""

    # Header
    report_text += "Sales Analysis Report\n"
    report_text += "----------------------\n"

    # Sales Overview
    report_text += "\nSales Overview\n"
    report_text += "--------------\n"
    total_sales_col = find_column(df, ['Amount', 'Sales', 'Revenue'])
    if total_sales_col:
        total_sales = df.agg({total_sales_col: 'sum'}).collect()[0]['sum(' + total_sales_col + ')']
        average_order_value = total_sales / df.count()
        report_text += f"Total Sales: ${total_sales:,.2f}\n"
        report_text += f"Average Order Value: ${average_order_value:,.2f}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += f"- Total Sales: ${total_sales:,.2f} (Overall revenue)\n"
        report_text += f"- Average Order Value: ${average_order_value:,.2f} (Average value of each order)\n"
        report_text += "\nSummary:\n"
        report_text += "The Sales Overview provides insights into the total sales and average order value, giving a snapshot of the overall revenue and order value.\n"
    else:
        report_text += "Total Sales column not found, skipping Sales Overview calculation\n"

    # Time-based Analysis
    report_text += "\nTime-based Analysis\n"
    report_text += "--------------------\n"
    time_col = find_column(df, ['Date', 'Timestamp'])
    if time_col:
        daily_sales = df.groupBy(time_col).agg({total_sales_col: 'sum'}).toPandas()
        report_text += f"Daily Sales:\n{daily_sales}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += "- Daily Sales: Review the trends and identify peak sales days.\n"
        report_text += "\nSummary:\n"
        report_text += "The Time-based Analysis explores daily sales trends, helping to identify patterns and peak sales days for strategic planning.\n"
    else:
        report_text += "Time column not found, skipping Time-based Analysis calculation\n"

    # Product Performance
    report_text += "\nProduct Performance\n"
    report_text += "---------------------\n"
    qty_col = find_column(df, ['Qty', 'Quantity'])
    if qty_col:
        top_selling_products = df.groupBy('SKU').agg({qty_col: 'sum', total_sales_col: 'sum'}).orderBy('sum(' + qty_col + ')', ascending=False).limit(5)
        report_text += f"Top Selling Products:\n{top_selling_products.toPandas()}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += "- Top Selling Products: Identify and prioritize high-performing products.\n"
        report_text += "\nSummary:\n"
        report_text += "The Product Performance analysis showcases the top-selling products, aiding in product prioritization and inventory management.\n"
    else:
        report_text += "Quantity column not found, skipping Product Performance calculation\n"

    # Order Fulfillment
    report_text += "\nOrder Fulfillment\n"
    report_text += "------------------\n"
    fulfillment_col = find_column(df, ['fulfilled-by'])
    if fulfillment_col:
        fulfillment_distribution = df.groupBy(fulfillment_col).count().toPandas()
        report_text += f"Fulfillment Distribution:\n{fulfillment_distribution}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += "- Fulfillment Distribution: Understand how orders are fulfilled.\n"
        report_text += "\nSummary:\n"
        report_text += "The Order Fulfillment analysis provides insights into how orders are distributed among fulfillment methods, crucial for operational efficiency.\n"
    else:
        report_text += "Fulfillment column not found, skipping Order Fulfillment calculation\n"

    # Courier and Order Status
    report_text += "\nCourier and Order Status\n"
    report_text += "------------------------\n"
    courier_status_col = find_column(df, ['Courier Status'])
    order_status_col = find_column(df, ['Status'])
    if courier_status_col and order_status_col:
        courier_status_distribution = df.groupBy(courier_status_col).count().toPandas()
        order_status_distribution = df.groupBy(order_status_col).count().toPandas()
        report_text += f"Courier Status Distribution:\n{courier_status_distribution}\n"
        report_text += f"Order Status Distribution:\n{order_status_distribution}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += "- Courier Status Distribution: Track the status of orders in the courier system.\n"
        report_text += "- Order Status Distribution: Monitor the overall status distribution of orders.\n"
        report_text += "\nSummary:\n"
        report_text += "The Courier and Order Status analysis provides insights into the distribution of orders among courier services and order statuses, aiding in monitoring delivery and order processing.\n"
    else:
        report_text += "Courier Status or Order Status column not found, skipping Courier and Order Status calculation\n"

    # Customer Insights
    report_text += "\nCustomer Insights\n"
    report_text += "------------------\n"

    # Quantity Analysis
    report_text += "\nQuantity Analysis\n"
    report_text += "-------------------\n"
    total_quantity_col = find_column(df, ['Qty', 'Quantity'])
    if total_quantity_col:
        total_quantity_sold = df.agg({total_quantity_col: 'sum'}).collect()[0]['sum(' + total_quantity_col + ')']
        average_quantity_per_order = total_quantity_sold / df.count()
        report_text += f"Total Quantity Sold: {total_quantity_sold}\n"
        report_text += f"Average Quantity Per Order: {average_quantity_per_order}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += "- Total Quantity Sold: Monitor overall product demand.\n"
        report_text += "- Average Quantity Per Order: Understand customer purchasing patterns.\n"
        report_text += "\nSummary:\n"
        report_text += "The Quantity Analysis provides insights into the total quantity sold and average quantity per order, helping to understand overall product demand and customer purchasing patterns.\n"
    else:
        report_text += "Quantity column not found, skipping Quantity Analysis calculation\n"

    # Financial Metrics
    report_text += "\nFinancial Metrics\n"
    report_text += "------------------\n"
    fixed_cost_per_unit = 50  # Replace with your actual cost per unit
    gross_revenue_col = find_column(df, ['Amount', 'Sales', 'Revenue'])
    if gross_revenue_col:
        gross_revenue = df.agg({gross_revenue_col: 'sum'}).collect()[0]['sum(' + gross_revenue_col + ')']
        total_quantity_col = find_column(df, ['Qty', 'Quantity'])
        if total_quantity_col:
            total_quantity_sold = df.agg({total_quantity_col: 'sum'}).collect()[0]['sum(' + total_quantity_col + ')']
            estimated_cost = total_quantity_sold * fixed_cost_per_unit
            profit_margin = (gross_revenue - estimated_cost) / gross_revenue
            report_text += f"Gross Revenue: ${gross_revenue:,.2f}\n"
            report_text += f"Estimated Cost: ${estimated_cost:,.2f}\n"
            report_text += f"Profit Margin: {profit_margin * 100:.2f}%\n"
            report_text += "\nKey Performance Indicators (KPIs):\n"
            report_text += "- Gross Revenue: Measure overall sales performance.\n"
            report_text += "- Profit Margin: Evaluate profitability.\n"
            report_text += "\nSummary:\n"
            report_text += "The Financial Metrics analysis provides insights into gross revenue, estimated cost, and profit margin, aiding in evaluating overall sales performance and profitability.\n"
        else:
            report_text += "Quantity column not found, skipping Financial Metrics calculation\n"
    else:
        report_text += "Gross Revenue column not found, skipping Financial Metrics calculation\n"

    # Currency Analysis
    report_text += "\nCurrency Analysis\n"
    report_text += "------------------\n"
    currency_col = find_column(df, ['currency'])
    if currency_col:
        currency_distribution = df.groupBy(currency_col).agg({total_sales_col: 'sum'}).toPandas()
        report_text += f"Currency Distribution:\n{currency_distribution}\n"
        report_text += "\nKey Performance Indicators (KPIs):\n"
        report_text += "- Currency Distribution: Analyze sales performance across different currencies.\n"
        report_text += "\nSummary:\n"
        report_text += "The Currency Analysis provides insights into sales performance across different currencies, aiding in understanding the global reach of your business.\n"
    else:
        report_text += "Currency column not found, skipping Currency Analysis calculation\n"

    # Summary
    report_text += "\nGeneral Summary\n"
    report_text += "---------------\n"
    report_text += "The analysis provides key insights into sales, time-based trends, product performance, order fulfillment, courier and order status distribution, customer insights, quantity analysis, financial metrics, and currency distribution. The report aims to help you make informed decisions based on your sales data, optimize strategies, and enhance overall business performance.\n"

    # Key Performance Indicators (KPIs) Summary
    report_text += "\nKPIs Summary\n"
    report_text += "-------------\n"
    report_text += "- Total Sales: Monitor overall revenue.\n"
    report_text += "- Average Order Value: Evaluate the average value of each order.\n"
    report_text += "- Daily Sales: Identify trends and peak sales days.\n"
    report_text += "- Top Selling Products: Prioritize high-performing products.\n"
    report_text += "- Fulfillment Distribution: Understand order fulfillment methods.\n"
    report_text += "- Courier Status Distribution: Track order statuses in the courier system.\n"
    report_text += "- Order Status Distribution: Monitor the overall status distribution of orders.\n"
    report_text += "- Total Quantity Sold: Monitor overall product demand.\n"
    report_text += "- Average Quantity Per Order: Understand customer purchasing patterns.\n"
    report_text += "- Gross Revenue: Measure overall sales performance.\n"
    report_text += "- Profit Margin: Evaluate profitability.\n"
    report_text += "- Currency Distribution: Analyze sales performance across different currencies.\n"
    report_text +=" --Report does not conatin the whole data as output .It only contain the top20 and summary for your understanding . \n"

    # Clean up: Delete the temporary file
    os.remove(file_path)

    # Response message with the report
    response_message = {"message": "Processing complete. Check the terminal for outputs.", "report": report_text}
    return jsonify(response_message)

def find_column(df, possible_column_names):
    # Find the first matching column name in the DataFrame
    for column_name in possible_column_names:
        if column_name.lower() in map(lambda x: x.lower(), df.columns):
            return column_name

    # If no matching column is found, return None
    return None

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        # Get the uploaded CSV file
        uploaded_file = request.files['csvFile']

        # Save the uploaded file temporarily
        uploaded_file_path = 'temp.csv'
        uploaded_file.save(uploaded_file_path)

        # Analyze the CSV file
        result = analyze_csv_file(uploaded_file_path)

        return result

    except Exception as e:
        return str(e)

       

if __name__ == "__main__":
    conn = mysql.connector.connect(host="localhost", user="hardik", password="Onedirection@01", database="user_detailed")
    cursor = conn.cursor()

    app.run(debug=True)