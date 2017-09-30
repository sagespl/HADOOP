
cd $HADOOP_DATA
rm -rf movie
cp -r ml-10M100K movie

cd movie
sed -i -e 's/::/@/g' movies.dat
sed -i -e 's/::/@/g' ratings.dat
sed -i -e 's/::/@/g' tags.dat

hdfs dfs -rm -r -skipTrash /user/`whoami`/dane/movie
hdfs dfs -mkdir /user/`whoami`/dane/movie

hdfs dfs -mkdir /user/`whoami`/dane/movie/movies/
hdfs dfs -copyFromLocal -f /home/`whoami`/dane/movielens/hive/movies.dat /user/`whoami`/dane/movie/movies/

hdfs dfs -mkdir /user/`whoami`/dane/movie/ratings/
hdfs dfs -copyFromLocal -f /home/`whoami`/dane/movielens/hive/ratings.dat /user/`whoami`/dane/movie/ratings/

hdfs dfs -mkdir /user/`whoami`/dane/movie/tags/
hdfs dfs -copyFromLocal -f /home/`whoami`/dane/movielens/hive/tags.dat /user/`whoami`/dane/movie/tags/


