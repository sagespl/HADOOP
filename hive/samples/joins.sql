
-- innner join

select m.movieid, m.title, m.genres, t.tag
from movies m
JOIN tags t on t.movieid = m.movieid
limit 100;

select m.movieid, m.title, m.genres, t.tag
from movies m
JOIN etags t on t.movieid = m.movieid
limit 100;

select m.movieid, m.title, m.genres, t.tag
from emovies m
JOIN etags t on t.movieid = m.movieid
ORDER by m.title
limit 100;

select m.movieid, m.title, m.genres, t.tag
from emovies m
JOIN etags t on t.movieid = m.movieid
SORT by m.title
limit 100;


-- left join

select m.movieid, m.title, m.genres, t.tag
from emovies m
LEFT JOIN etags t on t.movieid = m.movieid
limit 100;

select m.movieid, m.title, m.genres, t.tag
from emovies m
LEFT JOIN etags t on t.movieid = m.movieid
ORDER by m.title
limit 100;


-- semi join (if exists)

select m.movieid, m.title, m.genres, t.tag
from emovies m
LEFT JOIN etags t on t.movieid = m.movieid and t.tag like '%romantic%'
WHERE t.tag is not null
ORDER by m.title
limit 100;



-- full outer with

select m.movieid, m.title, m.genres, t.tag
from emovies m
FULL OUTER JOIN etags t on t.movieid = m.movieid
ORDER by m.title
limit 100;




