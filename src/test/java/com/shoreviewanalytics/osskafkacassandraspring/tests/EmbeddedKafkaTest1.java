package com.shoreviewanalytics.osskafkacassandraspring.tests;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.*;
/*
to run use: mvn -Dtest=EmbeddedKafkaTest1 test
 */

/**
 * class EmbeddedKafkaTests:
 * This is an example from the Spring Kafka documentation at the time located at (https://docs.spring.io/spring-kafka/docs/2.3.0.RELEASE/reference/html/#example)
 * and uses an embedded instance of kafka, creates a consumer DefaultKafkaConsumerFactory as a listener KafkaMessageListenerContainer,
 * it creates a properties containerProperties that holds additional properties for the consumer and then starts it up.  After the consumer
 * is setup a producer is setup using Map<String, Object> senderProps for the configuration.  First a ProducerFactory is created as pf which is then
 * passed into KafkaTemplate kafkaTemplate.  kafkaTemplate is used to send test messages that are in turn consumed by ConsumeRecord
 * received.
 */
public class EmbeddedKafkaTest1 {
    private static final String TEMPLATE_TOPIC = "templateTopic";

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
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TEMPLATE_TOPIC);

    @Test
    /*
    http://junit.sourceforge.net/javadoc/org/junit/Test.html
    The Test annotation tells JUnit that the public void method to which it is attached can be run as a test case. To run the method,
    JUnit first constructs a fresh instance of the class then invokes the annotated method. Any exceptions thrown by the test will be
    reported by JUnit as a failure. If no exceptions are thrown, the test is assumed to have succeeded.
    */
    public void testTemplate() throws Exception {
        /*
        setup the consumer
         */
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false",
                embeddedKafka.getEmbeddedKafka());

        /*
        The ConsumerFactory implementation to produce new Consumer instances for provided Map configs and optional Deserializers
        on each ConsumerFactory.createConsumer() invocation. If you are using Deserializers that have no-arg constructors and require no setup,
        then simplest to specify Deserializer classes against ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG and ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
        keys in the configs passed to the DefaultKafkaConsumerFactory constructor.  If that is not possible, but you are using Deserializers that may be
        shared between all Consumer instances (and specifically that their close() method is a no-op), then you can pass in Deserializer instances for
        one or both of the key and value deserializers. If neither of the above is true then you may provide a Supplier for one or both Deserializers
         which will be used to obtain Deserializer(s) each time a Consumer is created by the factory.
         */
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(consumerProps);

        /*
        Contains runtime properties for a listener container.
         */
        ContainerProperties containerProperties = new ContainerProperties(TEMPLATE_TOPIC);
        /*
        public class KafkaMessageListenerContainer<K,V>
        extends AbstractMessageListenerContainer<K,V>
        Single-threaded Message listener container using the Java Consumer supporting auto-partition assignment or user-configured assignment.
        With the latter, initial partition offsets can be provided. Takes consumer factor and container properties as args.
         */
        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProperties);

        /*
        https://www.baeldung.com/java-blocking-queue

        Creating unbounded queues is simple:
        BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>();
        The Capacity of blockingQueue will be set to Integer.MAX_VALUE. All operations that add an element to the unbounded queue will never block,
        thus it could grow to a very large size. The most important thing when designing a producer-consumer program using
        unbounded BlockingQueue is that consumers should be able to consume messages as quickly as producers are adding messages to the queue.
        Otherwise, the memory could fill up and we would get an OutOfMemory exception.
         */
        final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();

        container.setupMessageListener(new MessageListener<Integer, String>() {

            @Override
            public void onMessage(ConsumerRecord<Integer, String> record) {
                System.out.println("The record added to the BlockingQueue records is " + record);
                records.add(record);
            }

        });

        /*
        The container is the KafkaMessageListenerContainer above
         */
        container.setBeanName("templateTests");
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());

        /*
        setup the producer / sender
         */
        Map<String, Object> senderProps = KafkaTestUtils.senderProps(embeddedKafka.getEmbeddedKafka().getBrokersAsString());

        ProducerFactory<Integer, String> pf =
                new DefaultKafkaProducerFactory<Integer, String>(senderProps);
        /*
         A template for executing high-level operations.
        */
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic(TEMPLATE_TOPIC);
        template.sendDefault("foo");

        assertThat(records.poll(10, TimeUnit.SECONDS), hasValue("foo"));

        template.sendDefault(0, 2, "bar");

        ConsumerRecord<Integer, String> received = records.poll(10, TimeUnit.SECONDS);

        assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        assertThat(received, hasValue("bar"));

        template.send(TEMPLATE_TOPIC, 0, 2, "baz");
        received = records.poll(10, TimeUnit.SECONDS);

        assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        assertThat(received, hasValue("baz"));
    }

}
