package com.mikescherbakov.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BankBalanceApp {

    private static final ObjectMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private static final String inputTopicName = "transactions-input-stream2";
    private static final String  outputTopicName = "transactions-output-stream2";

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class CustomerData {
        private String name;
        private int amount;
        private String time;
    }

    public static void main(String[] args) {
        var config = getProperties();
        Runnable kafkaProducerRunnable = kafkaProducerRunnable(config);
        Runnable streamProcessingRunnable = streamProcessingRunnable(config);

        new Thread(kafkaProducerRunnable).start();
        new Thread(kafkaProducerRunnable).start();
        new Thread(kafkaProducerRunnable).start();
        new Thread(kafkaProducerRunnable).start();
        new Thread(kafkaProducerRunnable).start();
        new Thread(kafkaProducerRunnable).start();
        new Thread(streamProcessingRunnable).start();
    }

    private static Runnable kafkaProducerRunnable(Properties config) {
        return () -> {
            // create the producer
            try (var producer = new KafkaProducer<String, String>(config)) {
                for (var i = 0; i < 100; i++) {
                    // create a producer record
                    var key = UUID.randomUUID().toString();
                    var value = mapper.writeValueAsString(getRandomCustomerData());
                    var producerRecord = new ProducerRecord<>(inputTopicName, key, value);
                    // send data
                    producer.send(producerRecord, (metadata, e) -> {
                        // executes every time a record successfully sent or an exception is thrown
                        if (Objects.nonNull(e)) {
                            log.error("Error while producing", e);
                        }
                    });
                    TimeUnit.MILLISECONDS.sleep(500);
                }
                // tell the producer to send all data and block until done -- synchronous
                producer.flush();
            } catch (RuntimeException | JsonProcessingException | InterruptedException e) {
                log.error("Producer exception: " + e.getMessage(), e);
            }
        };
    }

    private static Runnable streamProcessingRunnable(Properties config) {

        var stringSerde = Serdes.String();
        // create Kafka Stream processing
        return () -> {
            var builder = new StreamsBuilder();
            var transactionsInputStream = builder.stream(inputTopicName, Consumed.with(stringSerde, stringSerde));
            transactionsInputStream
                    .groupBy((key, value) -> {
                        try {
                            var customerData = mapper.readValue(value, CustomerData.class);
                            return customerData.name;
                        } catch (JsonProcessingException e) {
                            log.error(e.getMessage(), e);
                            throw new RuntimeException(e);
                        }
                    })
                    .reduce((v1String, v2String) -> {
                        try {
                            var v1 = mapper.readValue(v1String, CustomerData.class);
                            var v2 = mapper.readValue(v2String, CustomerData.class);
                            var result = new CustomerData(v1.name, v1.amount, v1.time);
                            result.amount += v2.amount;
                            result.time = LocalDateTime.parse(v1.time).isAfter(LocalDateTime.parse(v2.time)) ? v1.time : v2.time;
                            return mapper.writeValueAsString(result);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toStream()
                    //.peek((key, value) -> System.out.println(value))
                    .to(outputTopicName);
            var streams = new KafkaStreams(builder.build(), config);
            streams.start();
            // print the topology
            streams.metadataForLocalThreads().forEach(System.out::println);
            // shutdown hook to correctly close the streams application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        };
    }

    private static CustomerData getRandomCustomerData() {
        var names = List.of("Jonh", "Don", "Von", "James", "David", "Ivan");
        return new CustomerData(
                names.get(ThreadLocalRandom.current().nextInt(names.size())),
                ThreadLocalRandom.current().nextInt(1000),
                LocalDateTime.now().toString()
        );
    }

    private static Properties getProperties() {
        var config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty("key.serializer", StringSerializer.class.getName());
        config.setProperty("value.serializer", StringSerializer.class.getName());
        config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return config;
    }
}
