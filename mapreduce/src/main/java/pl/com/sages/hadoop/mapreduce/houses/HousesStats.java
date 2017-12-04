package pl.com.sages.hadoop.mapreduce.houses;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  House Stats
 */
public class HousesStats extends Configured implements Tool {

    public static enum KEYS {
        MAPPED,
        REDUCED
    }

    public static class Map extends Mapper<LongWritable, Text, HouseKeyWritable, HouseValueWritable> {
        public static final int HOOD_COLUMN = 1;
        public static final int TYPE_COLUMN = 2;
        public static final int LAND_AREA_COLUMN = 14;
        public static final int GROSS_AREA_COLUMN = 15;
        public static final int YEAR_COLUMN = 16;
        public static final int PRICE_COLUMN = 19;

        public static final String NON_DIGIT_REGEX = "[^\\d]";
        public static final String EMPTY_STRING_REGEX = "";

        private HouseKeyWritable houseKey = new HouseKeyWritable();
        private HouseValueWritable houseValue = new HouseValueWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Counter counter = context.getCounter(KEYS.MAPPED);

            String[] dataLine = value.toString().split(",");

            houseKey.setHood(dataLine[HOOD_COLUMN]);
            houseKey.setType(dataLine[TYPE_COLUMN]);

            houseValue.setCount(1);

            houseValue.setLandArea(Integer.parseInt(dataLine[LAND_AREA_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX)));
            houseValue.setGrossArea(Integer.parseInt(dataLine[GROSS_AREA_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX)));
            houseValue.setYearBuilt(Integer.parseInt(dataLine[YEAR_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX)));
            houseValue.setSalePrice(Integer.parseInt(dataLine[PRICE_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX)));

            context.write(houseKey, houseValue);
            counter.increment(1);
        }
    }

    public static class Reduce extends Reducer<HouseKeyWritable, HouseValueWritable, Text, NullWritable> {
        private Text result = new Text();

        public void reduce (HouseKeyWritable key, Iterable<HouseValueWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String v = conf.get("some.value");

            long count = 0;
            float grossArea = 0;
            float landArea = 0;
            long year = 0;
            float price = 0;
            for (HouseValueWritable value : values) {
                count += value.getCount();
                grossArea += value.getGrossArea();
                landArea += value.getLandArea();
                year += value.getYearBuilt();
                price += value.getSalePrice();
            }
            result.set(String.format("%s\t%s\t%d\t%.2f\t%.2f\t%d\t%.2f",
                    key.getType(), key.getHood(),
                    count, grossArea / count, landArea / count, year / count, price / count));
            context.write(result, NullWritable.get());
            context.getCounter(KEYS.REDUCED).increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("some.key", "value");

        Job job = Job.getInstance(conf, "houses-stats");
        job.setJarByClass(HousesStats.class);
        job.setMapperClass(HousesStats.Map.class);
        job.setReducerClass(HousesStats.Reduce.class);

        // Specify key / value
        job.setMapOutputKeyClass(HouseKeyWritable.class);
        job.setMapOutputValueClass(HouseValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        String timeStamp = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        String outDir = String.format("%s/%s/%s", args[1], job.getJobName(), timeStamp);
        FileOutputFormat.setOutputPath(job, new Path(outDir));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        int ret = job.waitForCompletion(true) ? 0 : 1;

        Counters counters = job.getCounters();
        System.out.printf("KEYS.MAPPED=%d\n", counters.findCounter(KEYS.MAPPED).getValue());
        System.out.printf("KEYS.REDUCED=%d\n", counters.findCounter(KEYS.REDUCED).getValue());

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HousesStats(), args);
        System.exit(res);
    }
}
