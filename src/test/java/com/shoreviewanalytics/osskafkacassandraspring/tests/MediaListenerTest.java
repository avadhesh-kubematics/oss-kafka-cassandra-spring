package com.shoreviewanalytics.osskafkacassandraspring.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shoreviewanalytics.kafka.domain.Media;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * MediaListenerTest
 * Description: A test class to test the listener that listens for new media messages without a write to Cassandra.
 * To Run: mvn -Dtest=MediaListenerTest test
 */

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class MediaListenerTest {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka =
            new EmbeddedKafkaRule(1, true, "test_media");
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private KafkaTemplate<String, Object> template;

    @Before
    public void setup() {
        //System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
    }

    @Test
    public void test() throws Exception {

        final BlockingQueue<ConsumerRecord<String, Object>> records = new LinkedBlockingQueue<>();

        ConcurrentMessageListenerContainer<?, ?> container = (ConcurrentMessageListenerContainer<?, ?>) registry
                .getListenerContainer("media-01-test");

        container.stop();
        @SuppressWarnings("unchecked")

        AcknowledgingConsumerAwareMessageListener<String, Object> messageListener =
                (AcknowledgingConsumerAwareMessageListener<String, Object>) container
                .getContainerProperties().getMessageListener();

        CountDownLatch latch = new CountDownLatch(1);

        container.getContainerProperties()
                .setMessageListener(new AcknowledgingConsumerAwareMessageListener<String, Object>() {

                    @Override
                    public void onMessage(ConsumerRecord<String, Object> data, Acknowledgment acknowledgment,
                                          Consumer<?, ?> consumer) {

                        records.add(data);
                        messageListener.onMessage(data, acknowledgment, consumer);
                        latch.countDown();
                    }

                });
        container.start();

        ObjectMapper objectMapper = new ObjectMapper();

        Media media_message = new Media();

        media_message.setTitle("test title");
        media_message.setAdded_year("2019");
        media_message.setAdded_date("05-11-2019");
        media_message.setDescription("test description");
        media_message.setUserid("12345");
        media_message.setVideoid("12345");

        System.out.println("the media message content is " + media_message.toString());

        template.send("test_media", media_message);

        ConsumerRecord<String, Object> received = records.poll(10, TimeUnit.SECONDS);

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(received).isNotNull();

        System.out.println("The received value is " + received.value());

        assertThat(received.value().toString()).isEqualTo(media_message.toString());
    }

}
