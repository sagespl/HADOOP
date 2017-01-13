package pl.com.sages.hbase.api.loader;

import pl.com.sages.hbase.api.dao.TagDao;
import pl.com.sages.hbase.api.model.Tag;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadMovieTagsData {

    public static final String TAG_DATA = "/home/radek/Sages/dane/ml-10M100K/tags.dat";
    public static final int COMMIT = 100;

    public static void main(String[] args) throws IOException {
        new LoadMovieTagsData().run();
    }


    public void run() throws IOException {
        HBaseUtil.recreateTable(TagDao.TABLE, TagDao.CF);

        TagDao tagDao = new TagDao();

        //UserID::MovieID::Tag::Timestamp
        BufferedReader br = new BufferedReader(new FileReader(new File(TAG_DATA)));
        String line;
        String delimeter = "::";

        int count = 0;
        List<Tag> tags = new ArrayList<>(COMMIT);
        while ((line = br.readLine()) != null) {

            String[] data = line.split(delimeter);
            int userId = Integer.parseInt(data[0]);
            int movieId = Integer.parseInt(data[1]);
            String tag = data[2];
            int timestamp = Integer.parseInt(data[3]);

            tags.add(new Tag(userId,movieId,tag,timestamp));

            count++;
            if (count % COMMIT == 0) {
                tagDao.save(tags);
                tags = new ArrayList<>(COMMIT);
                System.out.println("Wczytano " + count + " wierszy");
            }

            if (count > 10000) {
                break; // wczytujemy tylko pierwsze 10 tys wierszy
            }
        }

        br.close();
    }

}
