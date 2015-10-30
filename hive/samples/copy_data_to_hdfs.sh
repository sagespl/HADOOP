
hdfs dfs -copyFromLocal -f /home/sages/Sages/dane/movie /user/sages/dane

hdfs dfs -mkdir /user/sages/dane/movie/movies/
hdfs dfs -copyFromLocal -f /home/sages/Sages/dane/movie/movies.dat /user/sages/dane/movie/movies/

hdfs dfs -mkdir /user/sages/dane/movie/ratings/
hdfs dfs -copyFromLocal -f /home/sages/Sages/dane/movie/ratings.dat /user/sages/dane/movie/ratings/

hdfs dfs -mkdir /user/sages/dane/movie/tags/
hdfs dfs -copyFromLocal -f /home/sages/Sages/dane/movie/tags.dat /user/sages/dane/movie/tags/


