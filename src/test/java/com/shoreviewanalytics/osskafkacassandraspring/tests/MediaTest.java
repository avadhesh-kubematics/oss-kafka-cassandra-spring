package com.shoreviewanalytics.osskafkacassandraspring.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shoreviewanalytics.kafka.domain.Media;
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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MediaTest
 * Description: A test class to test the ability to use the Media class to send and receive messages.
 * To Run: mvn -Dtest=MediaTest test
 * Note: A valid production configuration is necessary for this test class to run successfully.  For example,
 * because this test class contains a reference to the Sender class in the /src/main/.. when this test runs it ensures
 * that the overall application is configured as expected.  Therefore, when this test runs it ensures that Cassandra is
 * available as well as Kafka.
 */

/*
To have the correct broker address set on the Sender and Receiver beans during each test case,
we need to use the @DirtiesContext on all test classes. The reason for this is that each test case contains its own embedded Kafka broker
that will each be created on a new random port. By rebuilding the application context, the beans will always be set with the current broker address.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class MediaTest {

    private static final String TOPIC = "media-object-test";
    /*
    KafkaMessageListenerContainer - A single-threaded Message listener container using the Java Consumer
    supporting auto-partition assignment or user-configured assignment. With the latter, initial partition offsets can be provided.
     */
    private KafkaMessageListenerContainer<String, String> container;
    private BlockingQueue<ConsumerRecord<String, String>> records;
    private static final Logger LOGGER = LoggerFactory.getLogger(MediaTest.class);
    @Autowired
    /*
    Use the Sender class from the application code to send a test message using embedded Kafka.
     */
    private com.shoreviewanalytics.kafka.producer.Sender sender;

    @ClassRule
    /*
    https://stackoverflow.com/questions/41121778/junit-rule-and-classrule
    The distinction becomes clear when you have more than one test method in a class.
    A @ClassRule has its before() method run before any of the test methods. Then all the test methods are run,
    and finally the rule's after() method. So if you have five test methods in a class, before() and after() will only get run once each.

    @ClassRule applies to a static method, and so has all the limitations inherent in that. A @Rule causes tests to be run via the rule's apply() method,
    which can do things before and after the target method is run. If you have five test methods, the rule's apply() is called five times, as a wrapper around each method.

    Use @ClassRule to set up something that can be reused by all the test methods,
    if you can achieve that in a static method.

    Use @Rule to set up something that needs to be created new, or reset, for each test method.
     */
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TOPIC);


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
        Next a DefaultKafkaConsumerFactory is created using the consumerProperties from above.
         */
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<String, String>(
                        consumerProperties);

        // set the topic that needs to be consumed
        /*
        Next a new ContainerProperties is created that contains runtime properties
        (in this case the topic name) for the listener container. Both are then passed to the KafkaMessageListenerContainer constructor.
         */
        ContainerProperties containerProperties =
                new ContainerProperties(TOPIC);

        // create a Kafka MessageListenerContainer
        /*
        Next a new KafakMessageListenerContainer is created using the consumerFactory and the containerProperties.
         */

        container = new KafkaMessageListenerContainer<>(consumerFactory,
                containerProperties);

        // create a thread safe queue to store the received message
         /*
        Received messages need to be stored somewhere. In this example, a thread-safe BlockingQueue is used.
        We create a new MessageListener and in the onMessage() method we add the received message to the BlockingQueue.
         */
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container
                .setupMessageListener(new MessageListener<String, String>() {
                    @Override
                    public void onMessage(
                            ConsumerRecord<String, String> record) {
                        LOGGER.info("test-listener received message='{}'",
                                record.toString());
                        records.add(record);
                    }
                });

        // start the container and underlying message listener
        container.start();

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container,
                embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());

        // producer is setup in Sender so no need to setup once again unless you want to send a message
        // that contains keys, partitions, etc.


    }

    @After
    public void tearDown() {
        // stop the container
        container.stop();
    }

    @Test
    public void kafkaSetup_withTopic_ensureSendWithMedia() throws InterruptedException {

        ObjectMapper objectMapper = new ObjectMapper();

        Media media = new Media();
        media.setTitle("test title");
        media.setAdded_year("2019");
        media.setAdded_date("05-11-2019");
        media.setDescription("test description");
        media.setUserid("12345");
        media.setVideoid("12345");

        System.out.println("The object mapper value is " + objectMapper.valueToTree(media).toString());

        // Act
        sender.sendJson(TOPIC,objectMapper.valueToTree(media));


        // Assert
        ConsumerRecord<String, String> testMediaRecord = records.poll(10, TimeUnit.SECONDS);

        assertThat(testMediaRecord).isNotNull();

        assertThat(testMediaRecord.value().toString()).isEqualTo(objectMapper.valueToTree(media).toString());

    }

}

