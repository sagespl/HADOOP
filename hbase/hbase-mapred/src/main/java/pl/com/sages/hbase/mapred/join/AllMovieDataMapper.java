package pl.com.sages.hbase.mapred.join;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.loader.LoadMovieData;
import pl.com.sages.hbase.api.loader.LoadMovieRatingData;
import pl.com.sages.hbase.mapred.movies.AverageRatingReducer;

import java.io.IOException;

public class AllMovieDataMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public static final String TABLE_NAME = "movies_data";
    public static final String FAMILY_NAME = "movies_data";

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
        String movieGenres = null;
        String movieTags = null;
        Double movieRating = null;

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {

            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            System.out.println(family + ":" + column);

            if (family.equals(LoadMovieData.FAMILY_NAME)) {

                movieId = Bytes.toString(cell.getRow());

                if (LoadMovieData.TITLE.equals(column)) {

                    movieTitle = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(movieId + "::" + movieTitle);

                } else if (LoadMovieData.GENRES.equals(column)) {
                    movieGenres = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(movieId + "::" + movieGenres);
                }

            } else if (family.equals("ratingaverage")) {

                movieId = Bytes.toString(cell.getRow());
                System.out.println(movieId);
                if (column.equals("average")) {

                    movieRating = Bytes.toDouble(CellUtil.cloneValue(cell));
                    System.out.println(movieId + "::" + movieRating);

                }

            }

        }

        Put put = null;

        if (movieTitle != null) {
            put = new Put(Bytes.toBytes(movieId));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(LoadMovieData.TITLE), Bytes.toBytes(movieTitle));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(LoadMovieData.GENRES), Bytes.toBytes(movieGenres));
        } else if (movieRating != null) {
            put = new Put(Bytes.toBytes(movieId));
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(LoadMovieRatingData.RATING), Bytes.toBytes(movieRating));
        }

        return put;
    }

}
