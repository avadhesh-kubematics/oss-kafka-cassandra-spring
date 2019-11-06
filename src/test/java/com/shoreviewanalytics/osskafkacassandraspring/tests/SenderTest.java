package com.shoreviewanalytics.osskafkacassandraspring.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/*
The test uses the Sender class to send a test message to an embedded kafka instance.  Info: https://codenotfound.com/spring-kafka-embedded-unit-test-example.html
to run use: mvn -Dtest=SenderTest test
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SenderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderTest.class);
    private static String SENDER_TOPIC = "sender.t";
    @Autowired
    private com.shoreviewanalytics.kafka.producer.Sender sender;
    private KafkaMessageListenerContainer<String, Object> container;
    private BlockingQueue<ConsumerRecord<String, Object>> records;

    @ClassRule
    /*
    spring-kafka-test includes an embedded Kafka broker that can be created via a JUnit @ClassRule annotation. The rule will start a
    ZooKeeper and Kafka server instance on a random port before all the test cases are run, and stops the instances once the
    test cases are finished.

    The EmbeddedKafkaRule constructor takes as parameters: the number of Kafka servers to start,
    whether a controlled shutdown is needed and the topics that need to be created on the server.
     */
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, SENDER_TOPIC);

    @Before
    public void setUp() throws Exception {
        // set up the Kafka consumer properties
        /*
        To create the needed consumer properties a static consumerProps() method provided by KafkaUtils is used.
         */
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("sender", "false",
                        embeddedKafka.getEmbeddedKafka());

        // create a Kafka consumer factory
        /*
        Next a DefaultKafkaConsumerFactory and ContainerProperties are created, the ContainerProperties contains runtime properties
        (in this case the topic name) for the listener container. Both are then passed to the KafkaMessageListenerContainer constructor.
         */
        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<String, Object>(
                        consumerProperties);

        // set the topic that needs to be consumed
        ContainerProperties containerProperties =
                new ContainerProperties(SENDER_TOPIC);

        // create a Kafka MessageListenerContainer

        container = new KafkaMessageListenerContainer<>(consumerFactory,containerProperties);

        // create a thread safe queue to store the received messages
         /*
        Received messages need to be stored somewhere. In this example, a thread-safe BlockingQueue is used.
        We create a new MessageListener and in the onMessage() method we add the received message to the BlockingQueue.
         */
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container
                .setupMessageListener(new MessageListener<String, Object>() {
                    @Override
                    public void onMessage(
                            ConsumerRecord<String, Object> record) {
                        LOGGER.debug("test-listener received message='{}'",record.toString());
                        records.add(record);
                    }
                });

        // start the container and underlying message listener
        container.start();

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container,
                embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        // stop the container
        container.stop();
    }

    @Test
    public void testSend() throws InterruptedException {
        // send the message
        String greeting = "Hello Spring Kafka Sender!";
        // use the send method from the Sender class
        sender.send(SENDER_TOPIC,greeting);
        // check that the message was received
        ConsumerRecord<String, Object> received = records.poll(10, TimeUnit.SECONDS);
        // check the value
        assertThat(received.value().equals(greeting));
        // AssertJ Condition to check the key
        assertThat(received).has(key(null));
    }
}
