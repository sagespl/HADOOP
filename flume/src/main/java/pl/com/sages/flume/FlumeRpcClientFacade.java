package pl.com.sages.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

class FlumeRpcClientFacade {

    private RpcClient client;
    private String hostname;
    private int port;

    public void init(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);
    }

    public void sendDataToFlume(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        event.getHeaders().put("timestamp", String.valueOf(System.currentTimeMillis()));

        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
        }
    }

    public void cleanUp() {
        client.close();
    }

}