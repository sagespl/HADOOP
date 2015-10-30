
CREATE VIEW moviesWithTag AS
SELECT m.movieid, m.title, m.genres, t.tag
from movies m
JOIN etags t on t.movieid = m.movieid;

describe moviesWithTag;
describe formatted moviesWithTag;


select * from moviesWithTag limit 10;
select * from moviesWithTag WHERE title LIKE '%(201%' limit 10;