import json
import random
import time
from confluent_kafka import Producer, Consumer
from datetime  import datetime

def read_product_names(filename):
    with open(filename, 'r') as file:
        product_names = [line.strip() for line in file if line.strip()]
    return product_names

def generate_fake_ecom_event(real_product_names, all_sales_reps, indian_states_and_ut, max_sessions=100):
    sales_id = random.randint(1000, 9999)
    order_id = random.randint(10000, 99999)
    age = random.randint(18, 60)
    order_total = round(random.uniform(10.0, 1000.0), 2)
    promo_code_or_discount = random.choice(["PROMO123", "DISCOUNT50", None])
    product_id = random.randint(100, 999)
    product_name = random.choice(real_product_names)
    quantity_sold = random.randint(1, 10)
    unit_price = round(random.uniform(10.0, 100.0), 2)
    total_sales_amount = round(quantity_sold * unit_price, 2)
    discount_amount = round(random.uniform(0.0, 100.0), 2)
    tax_amount = round(random.uniform(0.0, 50.0), 2)
    shipping_fee = round(random.uniform(5.0, 20.0), 2)
    transaction_date_timestamp = int(time.time())
    transaction_date_readable = datetime.utcfromtimestamp(transaction_date_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    customer_id = random.randint(1000, 9999)
    gender = random.choice(["male", "female"])
    payment_method = random.choice(["credit card", "debit Card", "Cash on Delivery (COD)", "Gift Card"])
    payment_status = random.choice(["Successful", "Unsuccessful"])
    transaction_status = random.choice(["Abandoned", "Successful"])
    transaction_type = random.choice(["purchase", "return"])
    location_store = random.choice(indian_states_and_ut)
    sales_representative = random.choice(all_sales_reps)
    refund_return_status = random.choice(["refunded", "returned", "not applicable"])
    payment_confirmation_number = f"PAY-{random.randint(1000, 9999)}"
    session_id = random.randint(1, max_sessions)
    feature_accessed = random.choice(["Home", "Product Page", "Cart", "Checkout", "Profile"])
    session_end_timestamp = transaction_date_timestamp + random.randint(300, 1800)  # 5 to 30 minutes
    session_end_readable = datetime.utcfromtimestamp(session_end_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    session_duration = session_end_timestamp - transaction_date_timestamp

    event = {
        "Sales ID": sales_id,
        "Order ID": order_id,
        "Age": age,
        "Order Total": order_total,
        "Promo Code or Discount": promo_code_or_discount,
        "Product ID": product_id,
        "Product Name": product_name,
        "Quantity Sold": quantity_sold,
        "Unit Price": unit_price,
        "Total Sales Amount": total_sales_amount,
        "Discount Amount": discount_amount,
        "Tax Amount": tax_amount,
        "Shipping Fee": shipping_fee,
        "Transaction Date": transaction_date_timestamp,
        "Transaction Date Readable": transaction_date_readable,
        "Customer ID": customer_id,
        "Gender": gender,
        "Payment Method": payment_method,
        "Payment Status": payment_status,
        "Transaction Status": transaction_status,
        "Transaction Type": transaction_type,
        "Location/Store": location_store,
        "Sales Representative": sales_representative,
        "Refund/Return Status": refund_return_status,
        "Payment Confirmation Number": payment_confirmation_number,
        "Session ID": session_id,
        "Feature Accessed": feature_accessed,
        "Session Duration": session_duration,
        "Session End": session_end_readable
    }
    return event

def send_event_to_kafka(producer, kafka_topic, event):
    # Send the event to the Kafka topic as a JSON string
    producer.produce(kafka_topic, key=str(event["Sales ID"]).encode('utf-8'), value=json.dumps(event).encode('utf-8'))

    # Flush the producer buffer
    producer.flush()

def simulate_ecommerce_events(kafka_broker="localhost:9092", kafka_topic="ecom", product_names_file='D:\HARDIK YADAV\product_name.txt'):
    # Create a Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': kafka_broker,
        'group.id': 'ecom-consumer',
        'auto.offset.reset': 'earliest'
    }

    # Create a Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to the Kafka topic
    consumer.subscribe([kafka_topic])

    # Create a Kafka producer configuration
    producer_config = {
        'bootstrap.servers': kafka_broker,
        'client.id': 'ecom-producer'
    }

    # Create a Kafka producer
    producer = Producer(producer_config)

    # Load real product names from the file
    real_product_names = read_product_names(product_names_file)

    # Define Indian states and sales representatives
    indian_states_and_ut = [
        "Andaman and Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", "Assam",
        "Bihar", "Chandigarh", "Chhattisgarh", "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
        "Kerala", "Ladakh", "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya",
        "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
        "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal"
    ]

    all_sales_reps = [
        "Aarav", "Aisha", "Arjun", "Diya", "Rohan", "Sofia", "Vivek", "Zara", "Kiran", "Meera",
        "Aryan", "Anaya", "Dev", "Isha", "Aditya", "Avni", "Amit", "Neha", "Yuvan", "Tara","Emma", "Liam", "Olivia", "Noah", "Ava", "Sophia", "Jackson", "Isabella", "Lucas", "Mia",
        "Alexander", "Amelia", "Ethan", "Harper", "Henry", "Ella", "Sebastian", "Aria", "James", "Grace"
    ]

    while True:
        event = generate_fake_ecom_event(real_product_names, all_sales_reps, indian_states_and_ut)
        send_event_to_kafka(producer, kafka_topic, event)

        # Consume and print Kafka messages
        msg = consumer.poll(1.0)

        time.sleep(random.uniform(0.1, 2.0))
