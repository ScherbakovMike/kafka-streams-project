package com.mikescherbakov.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        var config = getProperties();

        var builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        var wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(value-> Arrays.asList(value.split(" ")))
                .selectKey((ignoredKey, word)->word)
                .groupByKey()
                .count(Named.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        System.out.println(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProperties() {
        var config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return config;
    }
}
