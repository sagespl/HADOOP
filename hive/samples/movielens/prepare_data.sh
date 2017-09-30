
cd $HADOOP_DATA
rm -rf movie
cp -r ml-10M100K movie

cd movie
sed -i -e 's/::/@/g' movies.dat
sed -i -e 's/::/@/g' ratings.dat
sed -i -e 's/::/@/g' tags.dat



