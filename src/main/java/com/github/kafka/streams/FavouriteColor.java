package com.github.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColor {


    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> favoriteColorInput = builder.stream("favourite-colour-input");


        // writting to topic (user, color) where color can only be "red, green, blue"

        favoriteColorInput
                .filter((key,value) -> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, colour) -> Arrays.asList("green","red","blue").contains(colour))
                .to("user-keys-and-colours");


        KTable<String, String> user_color = builder.table("user-keys-and-colours");

        KTable<String,Long> color_count = user_color
                .groupBy((user,color) -> new KeyValue<>(color,color))
                .count("CountsByColor");

        color_count.to(Serdes.String(), Serdes.Long(), "favourite-colour-output");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        kafkaStreams.cleanUp();
        kafkaStreams.start();


        System.out.println(kafkaStreams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


    }
}
