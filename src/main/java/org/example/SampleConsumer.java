package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SampleConsumer {
    private final static String BOOTSTRAP_SERVERS = "kbroker_1:9092,kbroker_2:9092,kbroker_3:9092";
    private final static String TOPIC_NAME = "first_topic";
    private final static Logger logger = LoggerFactory.getLogger(SampleConsumer.class);
    private final static String GROUP_ID = "sample-consumer-group-01";

    public static void main(String[] args) {
        // 1. 기본 접속 및 데이터 통신 세팅
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // 2. 컨슈머 생성 및 Topic 구독
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));
        // 3. polling 을 통한 데이터 수신 및 수신한 데이터 구체적인 처리
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                // 4. 읽어온 정보를 처리하는 본격적인 로직을 여기에 구현합니다.
                logger.info("record received from broker : {}", record);
            }
        }
    }
}
