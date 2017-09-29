
DROP TABLE IF EXISTS moviesg;
CREATE TABLE moviesg
  AS select movieid, title, split(genres,"\\|") AS genre from movies;

select * from moviesg limit 10;
select movieid, title, genre[0] as g1, genre[1] as g2, genre[2] as g3 from moviesg limit 10;