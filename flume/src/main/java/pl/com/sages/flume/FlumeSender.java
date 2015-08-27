package pl.com.sages.flume;

import sun.awt.windows.ThemeReader;

public class FlumeSender {

    public static void main(String[] args) {

        FlumeRpcClientFacade client = new FlumeRpcClientFacade();
        client.init("localhost", 4444);

        String sampleData = "Hello Flume ";
        for (int i = 0; i < 100000000; i++) {
            String message = sampleData + i;
            client.sendDataToFlume(message);
            System.out.println(message);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        client.cleanUp();

    }

}
