EcomStream System

EcomStream System is a real-time big data processing platform designed to handle and analyze eCommerce data efficiently. The system uses Kafka for real-time data ingestion, PySpark for data processing, Cassandra for storage, and Flask for the web interface. The dashboard is created using JavaScript and Google Charts, allowing users to visualize data in real-time and download reports


## Acknowledgements

 - [Spark documentation](https://spark.apache.org/docs/latest/)
 - [Kafka documentation](https://kafka.apache.org/documentation/)
 - [Cassandra documentation](https://cassandra.apache.org/doc/latest/)


## Installation

Install my-project with npm

```bash
https://github.com/Hardikyadav083/Real-Time-Processing-System-Pyspark-kafka-cassandra-.git
cd Real-Time-Processing-System-Pyspark-kafka-cassandra-

```
Install dependencies:

```bash
pip install -r requirements.txt
```

Set up Apache Kafka:

https://kafka.apache.org/quickstart

Set up Apache Cassandra:

https://cassandra.apache.org/_/quickstart.html
    
## Configuration

- Configure Kafka:

```bash

Edit config/kafka_config.py to include your Kafka broker details.
```

- Configure Cassandra:
```bash
Edit config/cassandra_config.py to include your Cassandra cluster details.
```




## Deployment


Start Kafka:
```bash
kafka-server-start.sh config/server.properties
```
Start Cassandra:
```bash
cassandra -f
```
Run Flask Application:
```bash
python main.py
```
Access the Dashboard:
```bash
Open your browser and go to http://localhost:5000.
```






## Features

- Real-time data ingestion using Kafka
- Data processing with PySpark
- Scalable storage with Cassandra
- User authentication with Flask and MySQL
- Real-time dashboard using JavaScript and Google Charts
- Downloadable real-time reports
## Detailed Workflow
- User Login
Login Page: Users are presented with a login page where they can enter their credentials.
Authentication: Credentials are verified against the MySQL database.
Session Management: On successful login, a session is created for the user.

- Real-time Data Ingestion
Kafka Producer: Captures real-time eCommerce data and sends it to Kafka topics.
Kafka Broker: Manages the data streams and distributes data to Kafka consumers.

- Data Processing
Spark Streaming: Consumes data from Kafka topics using PySpark.
Data Transformation: Performs necessary transformations and aggregations on the data.
Data Output: Writes the processed data to Cassandra.

- Data Storage
Cassandra Database: Stores the processed data, ensuring low-latency read and write operations.
Schema Design: Optimized for quick data retrieval to support real-time dashboard updates.

- Dashboard Creation
Flask Endpoints: Flask provides RESTful APIs to fetch processed data from Cassandra.
JavaScript and Google Charts: The front-end uses JavaScript to make AJAX calls to Flask endpoints and Google Charts to visualize the data.

- Real-time Updates: The dashboard updates automatically with the latest data.

- Report Download
Download Feature: Users can download real-time reports directly from the dashboard.
Report Generation: Reports are generated on-demand, reflecting the most recent data.
## Tech Stack

**Client:** JavaScript, Google Charts,HTML/CSS

**Server:** Apache Kafka, Apache Spark (PySpark),Apache Cassandra,Flask,MySQL,Python

