package pl.com.sages.hbase.api.loader;

import pl.com.sages.hadoop.data.model.movielens.Tag;
import pl.com.sages.hadoop.data.model.movielens.factory.TagFactory;
import pl.com.sages.hbase.api.dao.TagDao;
import pl.com.sages.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadMovieTagsData extends HBaseLoader {

    private static final String TAG_DATA = System.getenv("HADOOP_DATA") + "/movielens/ml-10M100K/tags.dat";

    public static void main(String[] args) throws IOException {
        new LoadMovieTagsData().load();
    }

    @Override
    public void load() {
        try {

            HBaseUtil.recreateTable(TagDao.TABLE, TagDao.CF);

            TagDao tagDao = new TagDao();

            //UserID::MovieID::Tag::Timestamp
            BufferedReader br = new BufferedReader(new FileReader(new File(TAG_DATA)));
            String line;
            int count = 0;
            List<Tag> tags = new ArrayList<>(COMMIT);
            while ((line = br.readLine()) != null) {

                Tag tag = TagFactory.create(line);
                tags.add(tag);

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

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
