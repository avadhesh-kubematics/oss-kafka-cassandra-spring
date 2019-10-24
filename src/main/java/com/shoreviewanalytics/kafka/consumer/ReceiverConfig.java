package com.shoreviewanalytics.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ReceiverConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
//        props.put("security.protocol", "SSL");
//        props.put("ssl.endpoint.identification.algorithm", "");
//        props.put("ssl.truststore.location", "/home/one/Downloads/kafka.service/client.truststore.jks");
//        props.put("ssl.truststore.password", "");
//        props.put("ssl.keystore.type", "PKCS12");
//        props.put("ssl.keystore.location", "/home/one/Downloads/kafka.service/client.keystore.p12");
//        props.put("ssl.keystore.password", "");
//        props.put("ssl.key.password", "");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    @Bean // deserialize the consumed json messages
    public ConsumerFactory<String, Object> consumerFactory() {
        final JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("*");
        return new DefaultKafkaConsumerFactory<>(
                //kafkaProperties.buildConsumerProperties(),
                consumerConfigs(),
                new StringDeserializer(),
                jsonDeserializer
        );
    }

    @Bean
	/*
	https://stackoverflow.com/questions/55023240/when-to-use-concurrentkafkalistenercontainerfactory

	The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application making the call.
	It is the responsibility of the user to ensure that multi-threaded access is properly synchronized. Un-synchronized access will result
	in ConcurrentModificationException.

    If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time,
    effectively giving these partitions the same priority for consumption. However in some cases consumers may want to
    first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions
    when these partitions have few or no data to consume.

	Spring-kafka

	ConcurrentKafkaListenerContainerFactory is used to create containers for annotated methods with @KafkaListener

	There are two MessageListenerContainer in spring kafka

	KafkaMessageListenerContainer
	ConcurrentMessageListenerContainer

    The KafkaMessageListenerContainer receives all messages from all topics or partitions on a single thread.
    The ConcurrentMessageListenerContainer delegates to one or more KafkaMessageListenerContainer instances to provide multi-threaded consumption.

	Using ConcurrentMessageListenerContainer

	@Bean
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
						kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
								new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(3);
		factory.getContainerProperties().setPollTimeout(3000);
		return factory;
	  }

      It has a concurrency property. For example, container.setConcurrency(3)
      creates three KafkaMessageListenerContainer instances.

      If you have six TopicPartition instances are provided and the concurrency is 3;
      each container gets two partitions. For five TopicPartition instances, two containers get two partitions,
      and the third gets one. If the concurrency is greater than the number of TopicPartitions,
      the concurrency is adjusted down such that each container gets one partition.

	 */
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
