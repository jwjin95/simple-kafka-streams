package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStreamApplication {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "54.152.20.232:9092";
    private static String STREAM_LOG = "raw_topic";
    private static String STREAM_LOG_COPY = "filtered_topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        // 현재 application의 name - 특별한 기능은 없음
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        // kafka broker의 주소, 포트
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 메시지 키를 직렬화, 역직렬화하는 포맷을 String으로 설정
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // 메시지 value를 직렬화, 역직렬화하는 포맷을 String으로 설정
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        KStream<String, String> filteredStream = streamLog.filter(
                (key, value) -> value.length() > 5
        );
    
//        KStream<String, String> filteredStream = streamLog.filter(
//                (key, value) -> value.contains("DE")
//        );

        filteredStream.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
