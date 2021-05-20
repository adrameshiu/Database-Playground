create database biased_random;
\c biased_random

CREATE TABLE P(i integer, probability float);
INSERT INTO P VALUES
(1,0.2),
(2,0.2),
(67,0.4),
(3,0.2);

\qecho 'table of Probability'
table P;

create or replace function
getCumulativeProbabilityPercentage(x integer)
  returns integer as
  $$
    select floor(SUM(probability)*100)::int
    from P
    WHERE p.i <= getCumulativeProbabilityPercentage.x;
  $$ language sql;

create or replace function
UnbiasedBinaryRelationOverIntegers(n int, l_1 int, u_1 int, l_2 int, u_2 int)
  returns table (x int, y int) as
  $$
    select floor(random() * (u_1-l_1+1) + l_1)::int as x,
    floor(random()*100)::int as y
    from generate_series(1,n);
  $$ language sql;

create or replace function
getbiasedRandomValue(randomValue integer)
  returns INTEGER as
  $$
    WITH
    	cumProb AS (
    		SELECT i,
        			(SELECT --when it is the first element
        				CASE
        					WHEN getCumulativeProbabilityPercentage(i-1) is null then 0
        					ELSE getCumulativeProbabilityPercentage(i-1)
        				END AS lo
        			),
        			getCumulativeProbabilityPercentage(i) as hi
      		FROM P
          	ORDER BY i
    	)

    select i
    from cumProb
    where lo <= getbiasedRandomValue.randomValue and hi > getbiasedRandomValue.randomValue;
  $$ language sql;

--sql query to get req output
select x, getbiasedRandomValue(y) as y
from UnbiasedBinaryRelationOverIntegers(5,3,8,1,10);


\c postgres
drop database biased_random;
