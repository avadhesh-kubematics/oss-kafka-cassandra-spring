package com.shoreviewanalytics.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class Sender {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private final KafkaTemplate<String, Object> template;

    public Sender(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    public void send(String topic, Object data) {
        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        this.template.send(topic, data);
    }
    public void sendJson(String topic, JsonNode data) {
        LOGGER.info("sending data='{}' to topic='{}'", data, topic);
        this.template.send(topic, data);
    }


}

