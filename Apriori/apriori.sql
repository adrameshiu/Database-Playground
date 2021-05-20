create database apriori;
\c apriori

----------------------------------------------------------------------------
-- helper functions
----------------------------------------------------------------------------
-- Subset test $A\subseteq B$:
create or replace function subset(A anyarray, B anyarray)
returns boolean as
$$
   select A <@ B;
$$ language sql;

-- Set union $A \cup B$:
create or replace function setunion(A anyarray, B anyarray)
returns anyarray as
$$
   select array(select unnest(A) union select unnest(B) order by 1);
$$ language sql;

-- Disjointness test $A\cap B = \emptyset$:
create or replace function disjoint(A anyarray, B anyarray)
returns boolean as
$$
   select not A && B;
$$ language sql;


----------------------------------------------------------------------------
-- create sample data
----------------------------------------------------------------------------
create table document (doc int, words text[]);
insert into document values (1, '{"A","B","C"}');
insert into document values (2, '{"B","C","D"}');
insert into document values (3, '{"A","E"}');
insert into document values (4, '{"B","B","A","D"}');
insert into document values (5, '{"E","F"}');
insert into document values (6, '{"A","D","G"}');
insert into document values (7, '{"C","B","A"}');
insert into document values (8, '{"B","A"}');


table document;

create table documentWord (doc int, words text);
insert into documentWord select d.doc, unnest(d.words) as words from document d;

table documentWord;

create table apriori_t_significant (words text[]);


  ----------------------------------------------------------------------------
  -- Function to find if a combination from the aprioiri significant table is also t-significant
  -- need to find a cross(self) join of the apriori relation and then the (distinct to ensure we dont get multiple such combos) setUnion as long as we arent doing it with the same set(done by using disjoint)
  -- use the above combination and see if it is a subset(using subset() helper func) of the document words..we then find the number of occurances using groupby
  -- also remove those combinations which are already present in the t significant relation
  ----------------------------------------------------------------------------
create or replace function find_bigger_subset(t int)
  returns table (words text[]) AS
  $$
  WITH t_significant_union as (select distinct setunion(a1.words, a2.words) as words
                              from apriori_t_significant a1 cross join apriori_t_significant a2
                              where disjoint(a1.words,a2.words))
  (select   a.words as  words --, COUNT(a) as  count
  from t_significant_union a JOIN document d ON (subset(a.words,d.words))
  group by (a.words)
  having  count(a.words) >= t)
    except --to remove those entries from this newly generated set which is already present in the relation
  (select words
   from apriori_t_significant);
  $$ LANGUAGE SQL;

----------------------------------------------------------------------------
--base rule is that all 1 word that appear t times are t-apriori_t_significant
--2nd rule is that all combination of existing t significant words that appear t times are also t-significant
----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION frequentSets (t int)
RETURNS table (words text[]) AS
$$
  BEGIN
    delete from apriori_t_significant;

    insert into apriori_t_significant values ('{}'); --empty set
    insert into apriori_t_significant
      select array_agg(DISTINCT d.words)
      from documentWord d
      group by (d.words)
      having  count(d.words) >= t;

    WHILE exists(select * from find_bigger_subset(t))
      LOOP
        insert into apriori_t_significant select * from find_bigger_subset(t);
      END LOOP;
  RETURN QUERY SELECT * FROM apriori_t_significant;
  END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------------------
-- Outputs
----------------------------------------------------------------------------
\qecho '1-frequent sets'
SELECT * from frequentSets(1);

\qecho '2-frequent sets'
SELECT * from frequentSets(2);

\qecho '3-frequent sets'
SELECT * from frequentSets(3);

\qecho '4-frequent sets'
SELECT * from frequentSets(4);

\qecho '5-frequent sets'
SELECT * from frequentSets(5);

\qecho '6-frequent sets'
SELECT * from frequentSets(6);


\c postgres
drop database apriori;
