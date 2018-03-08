package pl.com.sages.hadoop.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static pl.com.sages.hadoop.kafka.KafkaConfigurationFactory.*;

public class WordCountApplication {

    public static void main(String[] args) throws Exception {

        Properties config = getStreamConfig();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream(TOPIC);

        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts = source.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
//                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count("CountStore")
                .mapValues(value -> Long.toString(value))
                .toStream();

        counts.to(TOPIC_OUT);

        KafkaStreams streams = new KafkaStreams(builder, config);
//        streams.cleanUp();

        // start
        streams.start();

        // run and close
//        Thread.sleep(5000L);
//        streams.close();

        // forever
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}