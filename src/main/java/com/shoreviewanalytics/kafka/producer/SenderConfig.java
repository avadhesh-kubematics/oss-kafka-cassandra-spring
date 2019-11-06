package com.shoreviewanalytics.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
/*
@Configuration indicates that the class can
be used by the Spring IoC container as a source of
bean definitions.
*/
@Configuration
public class SenderConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
//        props.put("security.protocol", "SSL");
//        props.put("ssl.endpoint.identification.algorithm", "");
//        props.put("ssl.truststore.location", "/home/one/Downloads/kafka.service/client.truststore.jks");
//        props.put("ssl.truststore.password", "");
//        props.put("ssl.keystore.type", "PKCS12");
//        props.put("ssl.keystore.location", "/home/one/Downloads/kafka.service/client.keystore.p12");
//        props.put("ssl.keystore.password", "");
//        props.put("ssl.key.password", "");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


}
