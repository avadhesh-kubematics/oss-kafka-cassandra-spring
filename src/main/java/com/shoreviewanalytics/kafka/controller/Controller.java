package com.shoreviewanalytics.kafka.controller;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shoreviewanalytics.cassandra.MediaWriter;
import com.shoreviewanalytics.kafka.domain.Media;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

@RestController
/**
 * Description: This class allows the user to use a curl command to initiate a producer that writes messages to Kafka.
 * It then consumes the messages and writes them at the same time to Cassandra. The inspiration to use this approach was inspired by
 * https://thepracticaldeveloper.com/2018/11/24/spring-boot-kafka-config/#Spring_Boot_and_Kafka_Practical_Configuration_Examples.
 *
 */
public class Controller {

    private static final Logger logger =
            LoggerFactory.getLogger(Controller.class);

    private final KafkaTemplate<String, Object> template;
    private final String topicName;
    private CountDownLatch latch;
    private final ObjectMapper objectMapper;
    private final MediaWriter mediaWriter;
    private final CqlSession session;

    public Controller(
            final KafkaTemplate<String, Object> template,
            @Value("${tpd.topic-name}") final String topicName ) throws Exception {
        this.template = template;
        this.topicName = topicName;
        objectMapper = new ObjectMapper();
        mediaWriter = new MediaWriter();
        session = mediaWriter.cqlSession();

    }

    @GetMapping("/media")
    public String media() throws Exception {

        latch = new CountDownLatch(1);

        try (
                InputStream is = Controller.class.getResourceAsStream("/media_by_title_year.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));

                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter('$'))
        ) {
            for (CSVRecord csvRecord : csvParser) {
                // Accessing Values by Column Index

                String line = csvRecord.get(0) + "," + csvRecord.get(1) + "," + csvRecord.get(2) + "," + csvRecord.get(3) + "," + csvRecord.get(4) + "," + csvRecord.get(5);
                Media media = new Media();
                media.setTitle(csvRecord.get(0));
                media.setAdded_year(csvRecord.get(1));
                media.setAdded_date(csvRecord.get(2));
                media.setDescription(csvRecord.get(3));
                media.setUserid(csvRecord.get(4));
                media.setVideoid(csvRecord.get(5));

                this.template.send(new ProducerRecord<>(topicName, media));

                //logger.info(line);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        latch.await(60, TimeUnit.SECONDS);
        logger.info("Done producing and inserting media messages");
        return "Thanks for sending us your favorite media!";
    }

    @KafkaListener
            /*
            Annotation that marks a method to be the target of a Kafka message listener on the specified topics.
            The containerFactory() identifies a KafkaListenerContainerFactory to use to build the Kafka listener container.
            If not set, a default container factory is assumed to be available with a bean name of kafkaListenerContainerFactory
            unless an explicit default has been provided through configuration.

            Processing of @KafkaListener annotations is performed by registering a KafkaListenerAnnotationBeanPostProcessor. This can be done manually or, more conveniently, through EnableKafka annotation.

            Annotated methods are allowed to have flexible signatures similar to what MessageMapping provides, that is

                ConsumerRecord to access to the raw Kafka message
                Acknowledgment to manually ack
                @Payload-annotated method arguments including the support of validation
                @Header-annotated method arguments to extract a specific header value, defined by KafkaHeaders
                @Headers-annotated argument that must also be assignable to Map for getting access to all headers.
                MessageHeaders arguments for getting access to all headers.
                MessageHeaderAccessor for convenient access to all method arguments.

             */
            (topics = "media", clientIdPrefix = "media-json",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(ConsumerRecord<String, Object> cr,
                               @Payload Media payload) throws Exception {


        // Serialize each message as json to use previously written insert logic

        JsonNode serializeForInsert = objectMapper.valueToTree(cr.value());

        // Insert the json formatted message

        mediaWriter.WriteToCassandra(serializeForInsert,session);

        latch.countDown();
    }

    private static String typeIdHeader(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().equals("__TypeId__"))
                .findFirst().map(header -> new String(header.value())).orElse("N/A");
    }


}
