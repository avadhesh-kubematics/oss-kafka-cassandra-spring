# oss-kafka-cassandra-spring

This application demonstrates how to use Spring Kafka to create a producer and consumer of JSON messages.  The consumer is able to consume messages and simultaneously write them to a data source.   Cassandra is the data source, but the code could be modified to write data to any number of data sources such as MySQL or Postgres.  Some of the instructions below relate specifically to running this application using Kafka and Cassandra as services from [Aiven.io](https://aiven.io/). However, as I developed this example application, I pointed to a local single-node Kafka cluster and a local three node Cassandra cluster.  

### Prerequisites:

In order to run this application, it will be necessary to have a Kafka single or multi-node cluster as well as Cassandra single node or multi-node cluster with SSL enabled. It is possible to run without SSL, but it will be necessary to remove or comment out, the SSL configuration throughout the application. The source code uses a .pem file to access a Cassandra cluster using SSL. It also uses a client.truststore and a client.keystore to when accessing an SSL enabled Kafka cluster. 

It will be necessary to recompile the project adding your specific environment values. For example, the dbConnector class has a connect() method where you pass in values specific to your environment such as the IP address of your data source.

## Setting up Kafka SSL

You can use the following commands to create the client.truststore and the client.keystore that allow for a connection to Kafka using SSL. You will be asked to create a password that can be used within the application and later to check messages.  

	openssl pkcs12 -export -inkey service.key -in service.cert -out client.keystore.p12 -name service_key
	
	keytool -import -file ca.pem -alias CA -keystore client.truststore.jks

Use these newly created files by defining a path to them in the ReceiverConfig and SenderConfig classes in the application. 

## Setting up Cassandra

### Step 1

Download the CA Certificate which is found on the services home page within the connection information.   Use the downloaded certificate within the application.  For this example, the certificate is located in the /src/main/resources folder and has been renamed to cassandra.pem.   

### Step 2

Login to your Aiven.io Cassandra service using the connection information on the console page. For example, It will be necessary to run this command from a directory that contains the SSL certificate or to provide a path to the certificate.  

	SSL_CERTFILE=CA Certificate cqlsh --ssl -u avnadmin -p your_password your_host  your_port

### Step 3

After login in step 1, create a KEYSPACE to use when inserting data. 

	CREATE KEYSPACE kafka_examples WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'aiven': 3};



## Kafka Setup

### Step 1

Prior to running the application it will be necessary to create a topic called media.  To do this login to the Aiven.io console, click on the topics tab and add a topic with the name media. If running this code against a non-service deployment of Kafka you can run the code as is without creating a topic first.  

# Compile and Package

After completing the above steps you can compile and package the application.  Be sure to review the code and make all the necessary adjustments such as the IP address or hostname of the node you want to connect to for Cassandra. See the cqlSession method in the MediaWriter class.  

```
mvn compile package
```

# Testing

There are three tests included.  The first two are standard embedded Kafka tests. The third test sends a test message to an embedded Kafka instance using the Media class and is called MediaTest.  The MediaTest can be run with the following command. 

```
mvn -Dtest=MediaTest test
```

Please note that the MediaTest will fail if you are pointing the application at an environment that is not available because it is using classes from the application, which means the test will ensure the overall application is working as expected.  For example, because the application expects Cassandra it fails if it is unable to reach a running cluster that is accessible based on the current configuration of the application.     

# Running the Application

### Step 1  - Commands

```
mvn spring-boot:run or java -jar target/oss-kafka-cassandra-spring-0.0.1.jar
```

### Step 2 - Send, Consume, Write Messages

```
curl localhost:8080/media
```

### Step 3 - Check Messages

Create a console.properties file with content that contains SSL configuration. For example, you will need the path to client.keystore and client.truststore created earlier as well as passwords. 

```
security.protocol=SSL
ssl.endpoint.identification.algorithm=
ssl.protocol=TLS
ssl.key.password=audiovox1
ssl.keystore.location=/home/kafka/Downloads/kafka.service/client.keystore.p12
ssl.keystore.password=
ssl.keystore.type=PKCS12
ssl.truststore.location=/home/kafka/Downloads/kafka.service/client.truststore.jks
ssl.truststore.password=
ssl.truststore.type=JKS
```

Use the console.properties file along with following command from a server or workstation where you have Kafka installed.  

```
kafka-console-consumer.sh --bootstrap-server kafka-3153b09-shoreviewanalytics-d9c3.aivencloud.com:12643 --topic media --consumer.config console.properties --from-beginning
```

You should see output similar to the following after running the above command. 

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

 

# Summary

As I completed this application a few ideas came to mind.  One is creating a connector or listener for the source data that is used to produce messages.  So, rather than using a .csv file to produce messages there would be a listener on a source data system that automatically sends or produces messages to Kafka as new rows are inserted.  Depending on the application as records are inserted into a source system new messages are created to send to Kafka which in turn are consumed and then written to Cassandra or some other downstream data source.  



 

