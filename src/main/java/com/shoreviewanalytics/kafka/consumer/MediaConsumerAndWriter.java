package com.shoreviewanalytics.kafka.consumer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.shoreviewanalytics.cassandra.MediaWriter;
import com.shoreviewanalytics.kafka.domain.Media;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
/**
 * Description: This class uses standard Java, NOT SPRING KAFKA and is NOT being used in the current implementation which uses Spring Kafka.
 * See the Controller class @KafkaListener. The @KafkaListener annotation uses ConcurrentKafkaListenerContainerFactory defined in
 * OssKafkaCassandraSpringApplication to consume and insert messages into Cassandra.
* */
public class MediaConsumerAndWriter {

    private static final Logger logger = LoggerFactory.getLogger(MediaConsumerAndWriter.class);
    private static final Vector<Media> vectorOfVideos = new Vector<>();
    private static CqlSession session;

    public static void consume(String brokers, String groupId, String topicName) {
        new MediaConsumerAndWriter().run(brokers,groupId, topicName);

    }

    private MediaConsumerAndWriter() {
    }

    private void run(String brokers, String groupId, String topicName) {

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        MediaWriter mediaWriter = new MediaWriter();


        // create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerThreadRunnable ConsumerRunnable = new ConsumerThreadRunnable(
                brokers,
                groupId,
                topicName,
                latch

        );

        // start the thread
        Thread sThread = new Thread(ConsumerRunnable);
        sThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerThreadRunnable implements Runnable {

        //private CountDownLatch latch;
        private KafkaConsumer<String, JsonNode> consumer;


        CountDownLatch latch = new CountDownLatch(1);

        ConsumerThreadRunnable(String brokers, String groupId, String topicName, CountDownLatch latch ) {
            this.latch = latch;
            Properties props = new Properties();

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put("security.protocol", "SSL");
            props.put("ssl.endpoint.identification.algorithm", "");
            props.put("ssl.truststore.location", "/home/kafka/Downloads/kafka.service/client.truststore.jks");
            props.put("ssl.truststore.password", "audiovox1");
            props.put("ssl.keystore.type", "PKCS12");
            props.put("ssl.keystore.location", "/home/kafka/Downloads/kafka.service/client.keystore.p12");
            props.put("ssl.keystore.password", "audiovox1");
            props.put("ssl.key.password", "audiovox1");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            consumer = new KafkaConsumer<>(props);

            //consumer.subscribe(Collections.singleton(topicName));
            //or multiple topics comma delimited
            consumer.subscribe(Arrays.asList(topicName));
            // get data


        }

        @Override
        public void run() {

            MediaWriter mediaWriter = new MediaWriter();


            try {
                session = mediaWriter.cqlSession();
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {

                 int numberOfMessagesToRead = 429;  // kafka stores messages from 0 to ...n
                 boolean keepOnReading = true;
                 int numberOfMessagesReadSoFar = 0;

                 // poll for new data, get data
                 while (keepOnReading) {

                    ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, JsonNode> record : records) {
                        logger.info("Value: " + record.value());
                        //logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                        JsonNode jsonNode = record.value();

                        //logger.info("the jsonNode value for Title is" + jsonNode.get("title").asText());
                        Media media_record = new Media();

                        media_record.setTitle(jsonNode.get("title").asText());
                        media_record.setAdded_year(jsonNode.get("added_year").asText());
                        media_record.setAdded_date(jsonNode.get("added_date").asText());
                        media_record.setDescription(jsonNode.get("description").asText());
                        media_record.setUserid(jsonNode.get("userid").asText());
                        media_record.setVideoid(jsonNode.get("videoid").asText());

                        vectorOfVideos.add(media_record);

                        logger.info("The number of messages read so far are " + numberOfMessagesReadSoFar);

                        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                            mediaWriter.WriteToCassandra(vectorOfVideos,session);
                            keepOnReading = false;
                            break;
                        }
                        numberOfMessagesReadSoFar = numberOfMessagesReadSoFar + 1;

                    }

                }

            } catch (WakeupException e) {
                logger.info("Received shutdown signal!"); // received shutdown signal so out of loop
            } catch (Exception e) {
                 e.printStackTrace();
             } finally {
                consumer.close();
                latch.countDown(); // allows main code to understand able to exit
            }

        }

        void shutdown() {
            consumer.wakeup(); // method to interrupt consumer.poll() will throw exception wakeup exception

        }
    }


}
