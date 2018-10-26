package pl.com.sages.hbase.mapred.join;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.dao.MovieDao;
import pl.com.sages.hbase.api.dao.RatingDao;
import pl.com.sages.hbase.api.util.HBaseUtil;
import pl.com.sages.hbase.mapred.movies.MovieAverageRatingsConstants;

import java.io.IOException;

public class AllMovieDataMapper extends TableMapper<ImmutableBytesWritable, Put> {

    static final TableName TABLE_NAME = HBaseUtil.getUserTableName("movies_data");
    static final String FAMILY_NAME = "movies_data";

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        Put put = resultToPut(row, value);
        if (put != null) {
            context.write(row, put);
        }
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {

        System.out.println("------------------------------");

        String movieId = null;
        String movieTitle = null;
        String movieGenres = "";
        Double movieRating = null;

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {

            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            System.out.println(family + ":" + column);

            if (family.equals(Bytes.toString(MovieDao.CF))) {

                movieId = Bytes.toString(CellUtil.cloneValue(cell));

                if (column.equals(Bytes.toString(MovieDao.TITLE))) {

                    movieTitle = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(movieId + "::" + movieTitle);

                } else if (column.equals(Bytes.toString(MovieDao.GENRES))) {
                    movieGenres = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(movieId + "::" + movieGenres);
                }

            } else if (family.equals(MovieAverageRatingsConstants.FAMILY_NAME)) {

                movieId = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(movieId);
                if (column.equals(MovieAverageRatingsConstants.QUALIFIER_NAME)) {

                    movieRating = Bytes.toDouble(CellUtil.cloneValue(cell));
                    System.out.println(movieId + "::" + movieRating);

                }

            } else {
                throw new RuntimeException("Unknown family: " + family);
            }

        }

        Put put = null;

        if (movieTitle != null) {
            put = new Put(Bytes.toBytes(movieId));
            put.addColumn(Bytes.toBytes(FAMILY_NAME), MovieDao.TITLE, Bytes.toBytes(movieTitle));
            put.addColumn(Bytes.toBytes(FAMILY_NAME), MovieDao.GENRES, Bytes.toBytes(movieGenres));
        } else if (movieRating != null) {
            put = new Put(Bytes.toBytes(movieId));
            put.addColumn(Bytes.toBytes(FAMILY_NAME), RatingDao.RATING, Bytes.toBytes(movieRating));
        } else {
            throw new UnsupportedOperationException();
        }

        return put;
    }

}
