package pl.com.sages.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static pl.com.sages.kafka.KafkaConfigurationFactory.*;

public class WordCountApplication {

    private static final Pattern PATTERN = Pattern.compile("\\W+");

    public static void main(String[] args) {

        Properties config = getStreamConfig();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(TOPIC);
        KStream counts = source
                .flatMapValues(value -> Arrays.asList(PATTERN.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .filter((key, value) -> (!value.equals("ma")))
                .groupByKey()
                .count("CountStore")
                .mapValues(value -> Long.toString(value))
                .toStream();

        counts.to(TOPIC_OUT);

        KafkaStreams streams = new KafkaStreams(builder, config);

        // Nie używać na produkcji, czyści stan strumieni
        // streams.cleanUp();

        // start Kafka Streams
        streams.start();

        // jeśli chcemy zamknać aplikację po jakimś czasie to najprościej dajemy sleep i close
        // Thread.sleep(5000L);
        // streams.close();

        // Eleganckie zamknięcie
        // Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}