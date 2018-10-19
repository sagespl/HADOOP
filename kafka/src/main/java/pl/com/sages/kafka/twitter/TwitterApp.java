package pl.com.sages.kafka.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import pl.com.sages.kafka.callback.LoggerCallback;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static pl.com.sages.kafka.KafkaConfigurationFactory.TOPIC;
import static pl.com.sages.kafka.KafkaConfigurationFactory.createProducerConfig;

/**
 * Twitter App based on: https://github.com/twitter/hbc
 */
public class TwitterApp {

    public static final int NUMBER_OF_MESSAGES_TO_READ = 100;

    public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .name("sampleExampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(createProducerConfig());
        LoggerCallback callback = new LoggerCallback();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < NUMBER_OF_MESSAGES_TO_READ; msgRead++) {

            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            String msg = queue.poll(5, TimeUnit.SECONDS);

            if (msg == null) {
                System.out.println("Did not receive a message in 5 seconds");
            } else {
                System.out.println(msgRead);
                System.out.println(msg);

                if (msg.contains("created_at")) {

                    JSONObject obj = new JSONObject(msg);
                    String id = obj.get("id").toString();
                    String text = obj.get("text").toString();

                    ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, id, text);
                    producer.send(data, callback);
                }

            }

            // One message for each second!
            Thread.sleep(1000);

        }

        // Twitter stop
        client.stop();

        // Kafka stop
        producer.flush();
        producer.close();

        // Print some stats
        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }

    /**
     * Running Twitter consumer
     */
    public static void main(String[] args) {
        try {
            TwitterApp.run(args[0], args[1], args[2], args[3]);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

}
