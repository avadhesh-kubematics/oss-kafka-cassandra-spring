# oss-kafka-cassandra-spring

An example application that demonstrates how to use Spring Kafka to create a producer and consumer where the consumer is able to consume messages and simultaneously write the consumed records to a data source.  This application uses Cassandra as a data source, but the code can be modified to write data to any number of data sources such as MySQL or Postgres.  The plain Java version can be found [here](https://github.com/shoreviewanalytics/oss-kafka-cassandra).  

### Prerequisites:

In order to run this example it is necessary to have a Kafka single or multi node cluster as well as Cassandra single node or multi node cluster both with SSL enabled.  It is possible to run without SSL, but it will be necessary to comment out the SSL configuration throughout the application.  

The source code uses a .pem file to access a Cassandra cluster using SSL.  It also uses a client.truststore and a client.keystore to when accessing an SSL enabled Kafka cluster.  It will be necessary to recompile the  code adding your specific environment values.  For example, the  dbConnector class has a connect() method where you pass in values specific to your environment such as the IP address of your data source. 

Please note many of the instructions below relate specifically to running this application using Kafka and Cassandra as services from [Aiven.io]().  However, as I developed this example I pointed the code to a local deploy of a single node Kafka cluster and a local deploy of a three node Cassandra cluster.  

## Setting up Kafka SSL

Use the following commands to create the client.truststore and the client.keystore that allow for a connection using SSL. 

	openssl pkcs12 -export -inkey service.key -in service.cert -out client.keystore.p12 -name service_key
	
	keytool -import -file ca.pem -alias CA -keystore client.truststore.jks

Use these newly created files in the ReceiverConfig and SenderConfig classes in the application. 

## Setup Cassandra

### Step 1

Download the CA Certificate which is found on the services home page within the connection information.   Use the downloaded certificate within the application.  For this example, the certificate is located in the /src/main/resources folder and has been renamed to cassandra.pem.   

### Step 2

	Login to your Aiven.io Cassandra service using the connection information on the console page. For example,
	
	SSL_CERTFILE=CA Certificate cqlsh --ssl -u avnadmin -p your_password your_host  your_port

### Step 3

	After login create a KEYSPACE to use when inserting data. 
	
	CREATE KEYSPACE kafka_examples WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'aiven': 3};

### Compile and Package

```
mvn compile package
```

## Running the Application

### Step 1  - Commands

```
mvn spring-boot:run or java -jar target/oss-kafka-cassandra-spring-0.0.1.jar
```

### Step 2 - Send, Consume, Write Messages

```
curl localhost:8080/media
```

### Step 3 - Check Messages

```
kafka-console-consumer.sh --bootstrap-server kafka-3153b09-shoreviewanalytics-d9c3.aivencloud.com:12643 --topic media --consumer.config console.properties --from-beginning
```

You should see output similar to the following and pressing CTRL,C

```
{"title":"Grumpy Cat Outside Playing","added_year":"2015","added_date":"2015-03-01 08:00:00+0000","description":"Grumpy Cat was chewing on the drip irrigation line. New Tshirts at: http://www.tshirtoutlet.com/nsearch.html?query=grumpy+cat.","userid":"c26c353b-a076-457a-a450-e7029eeb0eb6","videoid":"15633b12-af9d-1eb3-a4bd-18a64d16704b"}
{"title":"Grumpy Cat: Slow Motion","added_year":"2015","added_date":"2015-02-01 16:00:00+0000","description":"Grumpy Cat - The internet''s grumpiest cat! Grumpy Cat Book: http://amzn.to/10Yd47R Grumpy Cat 2014 Calendar: http://amzn.to/YGK6MB New T-Shirts by ...","userid":"a15379d4-6d88-41bf-a46d-b4cac7707fc7","videoid":"e619d225-59a9-1ed8-b43f-dcf2905788c4"}
^CProcessed a total of 430 messages
```

### Step 4 - Check Records

From a server or workstation running Cassandra use the following command replacing the parameters with the values from your services.  

```
SSL_CERTFILE=ca.pem cqlsh --ssl -u avnadmin -p pqyk78fykq2ojox4 cassandra-23daba12-shoreviewanalytics-d9c3.aivencloud.com 12641
```

Next use the following commands to verify that records have been inserted. 

```
use kafka_examples;
expand on;
select * from videos_by_title_year ;
```

You should see output similar to the following.

```
@ Row 100

 title       | Marcus Johns Oscar Red Carpet Recap (2015) - Celebs & Selfies HD
 added_year  | 2015
 added_date  | 2015-02-24 00:00:01.000000+0000
 description | Subscribe to TRAILERS: http://bit.ly/sxaw6h Subscribe to COMING SOON: http://bit.ly/H2vZUn Like us on FACEBOOK: http://goo.gl/dHs73 Follow us on ...
 user_id     | c96ddac8-ad86-4989-b58e-6303df8c018f
 video_id    | 0a29f63f-2d6c-10c5-b385-8c55d3dcc2fc
```

If you have a question regarding this example, please let me know.  