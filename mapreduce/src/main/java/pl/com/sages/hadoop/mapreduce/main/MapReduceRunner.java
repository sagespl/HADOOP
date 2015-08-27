package pl.com.sages.hadoop.mapreduce.main;

import pl.com.sages.hadoop.mapreduce.invertedindex.InvertedIndexRunner;
import pl.com.sages.hadoop.mapreduce.wordcount.WordCountRunner;

public class MapReduceRunner {

    public static void main(String[] args) throws Exception {
        String operation = args[0];

        String[] result = new String[args.length - 1];
        System.arraycopy(args, 1, result, 0, result.length);

        if ("wordcount".equalsIgnoreCase(operation)) {
            WordCountRunner.main(result);
        } else if ("invertedindex".equalsIgnoreCase(operation)) {
            InvertedIndexRunner.main(result);
        }
    }

}
