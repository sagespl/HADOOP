package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.hbase.TableName;
import pl.com.sages.hbase.api.util.HBaseUtil;

public class MovieAverageRatingsConstants {

    public static final TableName TABLE_NAME = HBaseUtil.getUserTableName("ratings_average");
    public static final String FAMILY_NAME = "ratings_average";
    public static final String QUALIFIER_NAME = "average";

}
