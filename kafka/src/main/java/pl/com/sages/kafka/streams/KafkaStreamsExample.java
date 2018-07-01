package pl.com.sages.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

import static pl.com.sages.kafka.KafkaConfigurationFactory.*;

public class KafkaStreamsExample {

    public static void main(final String[] args) {

        Properties config = getStreamConfig();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(TOPIC);
        KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.as("counts-store"));

        wordCounts.toStream().to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Nie używać na produkcji, czyści stan strumieni
        // streams.cleanUp();

        // start Kafka Streams
        streams.start();

        // jeśli chcemy zamknać aplikację po jakimś czasie to najprościej dajemy sleep i close
        // Thread.sleep(5000L);
        // streams.close();

        // Eleganckie zamknięcie
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
