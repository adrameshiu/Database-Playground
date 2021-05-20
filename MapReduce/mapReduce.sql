create database mapReduce;
\c mapReduce


-- Write, in PostgreSQL, a basic MapReduce program, i.e., a mapper
-- function and a reducer function, as well as a 3-phases simulation that
-- implements the anti semijoin of two relations $R(A,B)$ and $S(A,B,C)$,
-- i.e., the relation $R\, \overline{\ltimes}\, S$.  Recall that
-- $R\,\overline{\ltimes}\, S = R - R\bowtie \pi_{A,B}(S)$.  You can assume that
-- the domain of $A$ and $B$ is integer.  (Notice that $R$ and $S$ have
-- two attributes.)  Use the encoding and decoding methods described
-- above.

-- Notice that since R(A,B,C), then in this case,
--  R\,\overline{\ltimes}\, S = R - R\bowtie \pi_{A,B}(S) = R - \pi_{A,B}(S)

-- Create Tables
CREATE TABLE R(
    A INTEGER,
    B INTEGER
);

CREATE TABLE S(
    A INTEGER,
    B INTEGER,
    C INTEGER
);

-- Populate table
INSERT INTO R VALUES (1, 2), (2, 4), (3, 6), (4,6);
INSERT INTO S VALUES (1, 2, 7), (2, 5, 8), (1, 4, 9), (3, 6, 10), (5, 7, 11);

\qecho 'Anti Join'
create table EncodingOfRandS(key text, value jsonb);

insert into EncodingOfRandS
(select 'R' as key, jsonb_build_object('a', r.a, 'b', r.b) as value
from R r)
union
(select 'S' as key, jsonb_build_object('a', s.a, 'b', s.b, 'c', s.c) as value
from S s)
order by 1;

table EncodingOfRandS;


-- mapper function
CREATE OR REPLACE FUNCTION mapper(key text, value jsonb)
RETURNS TABLE(k jsonb, v text) AS
$mapped$
  SELECT jsonb_build_object('a', value->'a', 'b', value->'b') as k, key as v;
$mapped$ LANGUAGE sql;

-- reducer function
CREATE OR REPLACE FUNCTION reducer(key jsonb, valuesArray text[])
RETURNS TABLE(key text, value jsonb) AS
$$
SELECT 'R antijoin S'::text, key
WHERE NOT ARRAY['S'] <@ valuesArray;
$$ LANGUAGE SQL;

-- 3-phases simulation of MapReduce Program followed by a decoding step

WITH
Map_Phase AS (
SELECT m.k as key, m.v as value
FROM EncodingOfRandS, LATERAL(SELECT k, v FROM mapper(key, value)) m
),
Group_Phase AS (
SELECT key, array_agg(value) as value --can also use json_object_agg
FROM Map_Phase
GROUP BY (key)
),
Reduce_Phase AS (
SELECT r.key, r.value
FROM Group_Phase, LATERAL(SELECT key, value FROM reducer(key, value)) r
)
SELECT value->'a' as a, value->'b' as b FROM Reduce_Phase;


-- Write, in PostgreSQL, a basic MapReduce program, i.e., a mapper
-- function and a reducer function, as well as a 3-phases simulation that
-- implements the natural join $R \bowtie S$ of two relations $R(A, B)$
-- and $S(B,C)$.  You can assume that the domains of $A$, $B$, and $C$
-- are integer.  Use the encoding and decoding methods described above.

-- Create Tables
DROP TABLE IF EXISTS R;
CREATE TABLE R(
    A INTEGER,
    B INTEGER
);

DROP TABLE IF EXISTS S;
CREATE TABLE S(
    B INTEGER,
    C INTEGER
);

-- Populate table
INSERT INTO R VALUES (1, 2), (1,3), (1,5), (2, 4), (2,6), (3, 6), (4,6);
INSERT INTO S VALUES (2, 7), (2, 5), (6, 4), (6, 8), (5, 7);



-- EncodingOfRandS;
drop table EncodingOfRandS;
create table EncodingOfRandS(key text, value jsonb);

insert into EncodingOfRandS
(select 'R' as key, jsonb_build_object('a', r.a, 'b', r.b) as value
from R r)
union
(select 'S' as key, jsonb_build_object('b', s.b, 'c', s.c) as value
from S s)
order by 1;

table EncodingOfRandS;


-- mapper function
DROP FUNCTION mapper;
CREATE OR REPLACE FUNCTION mapper(key text, value jsonb)
RETURNS TABLE(key jsonb, value text) AS
$$
select jsonb_build_object('a', value -> 'a', 'b1', value -> 'b'), key where key = 'R'
union
select jsonb_build_object('b2', value -> 'b', 'c', value -> 'c'), key where key = 'S';
$$ LANGUAGE SQL;

-- reducer function
DROP FUNCTION reducer;
CREATE OR REPLACE FUNCTION reducer(key jsonb, valuesArray text[])
RETURNS TABLE(key text, value jsonb) AS
$$
SELECT 'R natural join S'::text, jsonb_build_object('a', key -> 'a', 'b', key -> 'b1', 'c', key -> 'c')
WHERE ARRAY['R', 'S'] <@ valuesArray and key -> 'b1' = key -> 'b2';
$$ LANGUAGE SQL;

-- 3-phases simulation of MapReduce Program followed by a decoding step
WITH
Map_Phase AS (
SELECT m.key, m.value
FROM encodingOfRandS, LATERAL(SELECT key, value FROM mapper(key, value)) m
),
Group_Phase AS (
select distinct m1.key || m2.key as key, array(select m1.value union select m2.value) as value
from map_phase m1, map_phase m2
),
Reduce_Phase AS (
SELECT r.key, r.value
FROM Group_Phase, LATERAL(SELECT key, value FROM reducer(key, value)) r
)
SELECT p.value->'a' as a, p.value->'b' as b, p.value->'c' as c FROM Reduce_Phase p
order by 1, 2, 3;

-- Write, in PostgreSQL, a basic MapReduce program, i.e., a mapper
-- function and a reducer function, as well as a 3-phases simulation


-- Create Tables
DROP TABLE IF EXISTS R;
CREATE TABLE R(
    A INTEGER,
    B INTEGER
);

-- Populate table
INSERT INTO R VALUES (1, 2), (1,3), (1,5), (2, 4), (2,6), (3, 6), (4,6);

-- EncodingOfR;
create table EncodingOfR(key text, value jsonb);

insert into EncodingOfR
(select 'R' as key, jsonb_build_object('a', r.a, 'b', r.b) as value
from R r)
order by 1;

table EncodingOfR;

-- mapper function
DROP FUNCTION mapper;
CREATE OR REPLACE FUNCTION mapper(key text, value jsonb)
RETURNS TABLE(key int, value int) AS
$$
SELECT (value->'a')::int, (value->'b')::int;
$$ LANGUAGE SQL;

-- reducer function
DROP FUNCTION reducer;
CREATE OR REPLACE FUNCTION reducer(key int, valueArray int[])
RETURNS TABLE(key int, value jsonb) AS
$$
SELECT key, jsonb_build_object('array_agg',valueArray,'cardinality', CARDINALITY(valueArray))
WHERE CARDINALITY(valueArray)>=2;
$$ LANGUAGE SQL;

-- 3-phases simulation of MapReduce Program followed by a decoding step
WITH
Map_Phase AS (
SELECT m.key, m.value
FROM encodingOfR, LATERAL(SELECT key, value FROM mapper(key, value)) m
),
Group_Phase AS (
SELECT key, array_agg(value) as value
FROM Map_Phase
GROUP BY (key)
),
Reduce_Phase AS (
SELECT r.key, r.value
FROM Group_Phase, LATERAL(SELECT key, value FROM reducer(key, value)) r
)
SELECT key as "A", value->'array_agg' as "array_agg(r.B)", value->'cardinality' as "cardinality(array_agg(r.B))" FROM Reduce_Phase;



-- Let $R(K,V)$ and $S(K,W)$ be two binary key-value pair relations.  You
-- can assume that the domains of $K$, $V$, and $W$ are integers.
-- Consider the cogroup transformation {\tt R.cogroup(S)} introduced in
-- the lecture on {\tt Spark}.


-- Define a PostgreSQL view {\tt coGroup} computes a complex-object
-- relation that represent the co-group transformation {\tt
-- R.cogroup(S)}.  Show that this view works.
DROP TABLE IF EXISTS R CASCADE;
CREATE TABLE R (k INT, v INT);
INSERT INTO R VALUES (1, 0), (2, 2), (2, 3), (3,1), (3, 5), (4,1), (6, 7);

DROP TABLE IF EXISTS S CASCADE;
CREATE TABLE S (k INT, w INT);
INSERT INTO S VALUES (2, 4), (2, 5), (3, 1), (4,1), (7, 6);

CREATE TYPE coGroupType AS (RV_values int[], SW_values int[]);
-- the keys in the cogroup will be the union of all keys in the two key-value pair relations
CREATE VIEW cogroup_keys AS
      SELECT r.K FROM R r
        UNION
      SELECT s.K FROM S s;

--creating a view that will aggregate/group all values corresponding to a key into an array
--we also need to find its union with an empty value for those keys that are in S but not in R
CREATE VIEW cogroup_R AS
    SELECT r.K as k, ARRAY_AGG(r.V) AS RV_values
        FROM   R r
        GROUP BY (r.K)
        UNION
        SELECT k.K, '{}' AS RV_values
        FROM   cogroup_keys k
        WHERE  k.K NOT IN (SELECT r.K FROM R r);


--we also need to find its union with an empty value for those keys that are in R but not in S
CREATE VIEW cogroup_S AS
    SELECT s.K as k, ARRAY_AGG(s.W) AS SW_values
            FROM   S s
            GROUP BY (s.K)
            UNION
            SELECT k.K, '{}' AS SW_values
            FROM   cogroup_keys k
            WHERE  k.K NOT IN (SELECT s.K FROM S s);

CREATE MATERIALIZED VIEW co_group AS
SELECT  K as key,  array_agg(row(RV_values, SW_values)::coGroupType) as value
  FROM    cogroup_R NATURAL JOIN cogroup_S
  GROUP BY cogroup_R.k;


\qecho 'co group  of R and S'
WITH
 A as (
SELECT key, UNNEST (value) as v
FROM co_group)

SELECT key, (v).rv_values, (v).sw_values FROM A;


-- Write a PostgreSQL query that use this {\tt coGroup} view to
-- compute $R\,\overline{\ltimes}\, S$, in other words compute the
-- relation $R - (R \bowtie \pi_{K}(S))$.
\qecho 'to get join, we unnest, and check the cardinality..only those RV_values should contain elements and SW_values should be empty'
WITH
 A as (
SELECT key, UNNEST (value) as v
FROM co_group)

SELECT key, UNNEST((v).rv_values )
FROM A
WHERE CARDINALITY((v).sw_values) = 0 AND CARDINALITY((v).rv_values) > 0;
\qecho 'Problem 11.c'

-- PostgreSQL query that uses this coGroup view to implement the SQL
-- query

WITH
 c as (
SELECT key, UNNEST (value) as v
FROM co_group)

SELECT c1.key as R_key, c2.key as S_key
FROM C c1, C c2
WHERE CARDINALITY((c2.v).sw_values) > 0 AND CARDINALITY((c1.v).rv_values) > 0
AND (c2.v).sw_values <@ (c1.v).rv_values;



-- Let {\tt A(x)} and {\tt B(x)} be the schemas to represent two set of
-- integers $A$ and $B$.  Consider the {\tt cogroup} transformation
-- introduced in the lecture on {\tt Spark}.  Using an approach analogous
-- to the one in Problem~\ref{cogroup} solve the following
-- problems:\footnote{An important aspect of this problem is to represent
-- $A$ and $B$ as a key-value stores.}


-- Write a PostgreSQL query that uses the cogroup transformation to
--compute $A\cap B$.

CREATE TABLE A(x int);
CREATE TABLE B(x int);

INSERT INTO A VALUES (1),(2),(3),(4);
INSERT INTO B VALUES (1),(3),(5);


drop view if exists cogroup;
create or replace view cogroup as
with Avalues as
(select '()' as K, array_agg(x) as Aarr from A group by K),
Bvalues as
(select '()' as K, array_agg(x) as Barr from B group by K)
select a.K as Key, a.Aarr as A, b.Barr as B from Avalues a, Bvalues b;

select * from cogroup;

select x as A_intersect_B from
(select unnest(A) as x from cogroup
intersect
select unnest(B) as x from cogroup) q
order by 1;


-- Write a PostgreSQL query that uses the cogroup operator
-- to compute the symmetric difference of $A$ and $B$, i.e., the expression
-- $$(A - B) \cup (B-A).$$
with diff1 as
(select unnest(A) as x from cogroup
except
select unnest(B) as x from cogroup),
diff2 as
(select unnest(B) as x from cogroup
except
select unnest(A) as x from cogroup)
select x as A_symmetric_difference_B from
(select d1.x from diff1 d1
union
select d2.x from diff2 d2) q
order by 1;



\c postgres
drop database mapReduce;
