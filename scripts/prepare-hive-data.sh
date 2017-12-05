
cd $HADOOP_DATA/movielens
pwd

rm -rf hive
mkdir hive
mkdir hive/movies
mkdir hive/ratings
mkdir hive/tags

cp -r ml-10M100K/movies.dat hive/movies
cp -r ml-10M100K/ratings.dat hive/ratings
cp -r ml-10M100K/tags.dat hive/tags

cd $HADOOP_DATA/movielens/hive/movies
sed -i -e 's/::/@/g' movies.dat

cd $HADOOP_DATA/movielens/hive/ratings
sed -i -e 's/::/@/g' ratings.dat

cd $HADOOP_DATA/movielens/hive/tags
sed -i -e 's/::/@/g' tags.dat