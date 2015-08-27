package pl.com.sages.hadoop.mapreduce;

import java.util.Properties;

public class PropertyReader {

    public static String readProperty(String keyName) {
        Properties prop = new Properties();
        try {

            prop.load(PropertyReader.class.getClassLoader().getResourceAsStream("mapreduce.properties"));
            String property = prop.getProperty(keyName);

            return property != null ? property.trim() : "";

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
