package pl.com.sages.hadoop.mapreduce.hive;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

import javax.security.sasl.SaslException;
import java.util.List;

/**
 * Hive Thrift Client
 */
public class HiveThriftClient {

    public void runTest() throws TException, SaslException {

        final TSocket tSocket = new TSocket("sandbox", 10000);
        //final TSaslClientTransport transport = new TSaslClientTransport("NOSASL", null, null, "sandbox", null, null, tSocket);
        final TProtocol protocol = new TBinaryProtocol(tSocket);
        final HiveClient client = new HiveClient(protocol);
        tSocket.open();

        client.execute("show tables");
        final List<String> results = client.fetchAll();
        for (String result : results) {
            System.out.println(result);
        }
        tSocket.close();
    }

    public static void main(String[] args) throws TException, SaslException {
        HiveThriftClient client = new HiveThriftClient();
        client.runTest();
    }
}
