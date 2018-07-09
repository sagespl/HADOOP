
-- describe
describe movies;
describe formatted movies;

-- select
select * from movies limit 10;
select * from emovies limit 10;
select * from ratings limit 10;
select * from eratings limit 10;
select * from tags limit 10;
select * from etags limit 10;

select movieid, title from movies limit 10;
select m.movieid, m.title from movies m limit 10;

select upper(title) from movies limit 10;

select count(*) from movies;
select avg(rating) from ratings;

SELECT count(*) FROM ratings;
SELECT count(DISTINCT userid) FROM ratings;
SELECT count(DISTINCT userid),count(DISTINCT movieid) FROM ratings;

--zamiana na tablicę ze stringa
select movieid, title, split(genres,"\\|") AS genre from movies limit 10;

-- rozbicie tablicy per wiersz
select explode(split(genres,"\\|")) AS genre from movies limit 10;

-- lateral view
select * from movies
LATERAL VIEW explode(split(genres,"\\|")) adTable AS adid;

-- lateral view z grupowaniem
select gen, count(1) from movies
LATERAL VIEW explode(split(genres,"\\|")) latview AS gen
GROUP BY gen;

SELECT userid, movieid,
 CASE
  WHEN rating < 1 THEN 'bad'
  WHEN rating >= 1 AND rating < 3 THEN 'ok'
  WHEN rating >= 3 AND rating < 5 THEN 'good'
  ELSE 'very good'
END AS human_rating
FROM ratings limit 100;

select * from ratings where rating >= 3 limit 100;

select * from movies where title like 'Toy%' limit 10;

select * from movies where title rlike '.*(Hulk|Spiderman|Avengers|Batman).*';

select avg(rating) from ratings;
select userid, avg(rating) from ratings GROUP BY userid limit 10;
select userid, avg(rating) from ratings GROUP BY userid HAVING avg(rating) >= 4 limit 10;
