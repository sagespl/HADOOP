
cd $HADOOP_DATA
rm -rf movie
cp -r ml-10M100K movie

cd movie
sed -i -e 's/::/@/g' movies.dat
sed -i -e 's/::/@/g' ratings.dat
sed -i -e 's/::/@/g' tags.dat

hdfs dfs -rm -r -skipTrash /user/sages/dane/movie
hdfs dfs -mkdir /user/sages/dane/movie

hdfs dfs -mkdir /user/sages/dane/movie/movies/
hdfs dfs -copyFromLocal -f /home/sages/dane/movie/movies.dat /user/sages/dane/movie/movies/

hdfs dfs -mkdir /user/sages/dane/movie/ratings/
hdfs dfs -copyFromLocal -f /home/sages/dane/movie/ratings.dat /user/sages/dane/movie/ratings/

hdfs dfs -mkdir /user/sages/dane/movie/tags/
hdfs dfs -copyFromLocal -f /home/sages/dane/movie/tags.dat /user/sages/dane/movie/tags/


