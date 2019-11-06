package com.shoreviewanalytics;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
/*
to run this application use command: mvn spring-boot:run or java -jar target/oss-kafka-cassandra-spring-0.0.1.jar

to consume / insert records use command: curl localhost:8080/media


 */
@SpringBootApplication
public class OssKafkaCassandraSpringApplication {

	public static void main(String[] args) {
		SpringApplication.run(OssKafkaCassandraSpringApplication.class, args);
	}

	@Value("${tpd.topic-name}")
	private String topicName;
	/*
	create a new topic called ... topicName, with 3 partitions and a replication factor of 1
	 */
	@Bean
	public NewTopic adviceTopic() {
		return new NewTopic(topicName, 3, (short) 1);
	}

}
