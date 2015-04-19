package pl.com.sages.hadoop.pig;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.IOException;

/**
 * Pig Server Example
 */
public class EmbededPigServer {
    private PigServer server;

    public EmbededPigServer(ExecType target) throws IOException {
        server = new PigServer(target);
    }

    public EmbededPigServer(String target) throws IOException {
        server = new PigServer(target);
    }

    public void runTest(String inputFile) throws IOException {
        server.registerQuery("A = load '" + inputFile + "' using PigStorage(',');");
        server.registerQuery("B = foreach A generate $1;");
        server.registerQuery("C = limit B 10;");
        server.store("C", "out-pig-server");
    }

    public static void main(String[] args) throws IOException {
        //EmbededPigServer server = new EmbededPigServer("local");
        //server.runTest("../data/rollingsales_bronx.csv");

        EmbededPigServer server = new EmbededPigServer(ExecType.MAPREDUCE);
        server.runTest("/user/root/rollingsales_bronx.csv");

        System.getProperty("user.name");
    }


}