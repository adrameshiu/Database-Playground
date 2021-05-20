create database buddy;
\c buddy

create or replace function
  hasBirthdayCollision(firstDay integer, totalDaysPerBand integer, peopleCount integer)
  returns boolean as
  $$
    WITH
    	random_buddays as (
          SELECT floor(random() * (totalDaysPerBand - firstDay + 1) + firstDay)::int as dob, s
          from generate_series(1,peopleCount) s
        ),
      birthday_count as (
        select dob, count(s)
        FROM random_buddays
        GROUP BY dob
        ORDER BY count desc
        )

        SELECT EXISTS ( SELECT 1 FROM birthday_count WHERE count > 1) as collisions;
  $$ language sql;

create or replace function
  findCollisionsForPossibleDays(firstDay integer, totalDaysPerBand integer, peopleCount integer)
  returns table(peopleCount integer, has_birthday_collisions boolean) as
  $$
    SELECT peopleCount, hasBirthdayCollision(firstDay, totalDaysPerBand, peopleCount) as has_birthday_collisions
    from generate_series(firstDay,totalDaysPerBand) n;
  $$ language sql;


create or replace function
  initializeRuns(firstDay integer, totalDaysPerBand integer)
  returns void as
  $$
    begin
    drop table if exists runsTable;
    create table runsTable (peopleCount int, collisionCount int);
    INSERT INTO runsTable
            SELECT pplCnt,0 FROM generate_series(firstDay,totalDaysPerBand+1) pplCnt; --max number of people can be days + 1

    end;
  $$  language plpgsql;

create or replace function
  updateRuns(firstDay integer, totalDaysPerBand integer)
  returns void as
  $$
  begin
    UPDATE runsTable r
    SET collisionCount = collisionCount + 1
    FROM (SELECT q, hasBirthdayCollision(firstDay, totalDaysPerBand, q) as has_birthday_collisions
          from generate_series(firstDay,totalDaysPerBand) q) n
    WHERE n.q = r.peopleCount AND n.has_birthday_collisions = true;
    end;
  $$ language plpgsql;

  create or replace function
    getMinPeopleCountForCollision(firstDay integer, totalDaysPerBand integer)
    returns table (minPeople int) as
    $$
      begin
        PERFORM initializeRuns(firstDay,totalDaysPerBand);
        PERFORM updateRuns(firstDay, totalDaysPerBand) FROM generate_series(1,100) runs; --running it 100 times to get a round set for getting probability over 100 runs

        return QUERY
        --SELECT * FROM runsTable ORDER BY peopleCount;
        SELECT r.peoplecount
        FROM runsTable r
        WHERE r.collisioncount >= 50
        ORDER BY r.collisioncount LIMIT 1;
      end;
    $$  language plpgsql;


SELECT 100 as "total Days in band",minPeople as "Min People Required for Collision" FROM  getMinPeopleCountForCollision(1, 100);
SELECT 365 as "total Days in band",minPeople as "Min People Required for Collision" FROM  getMinPeopleCountForCollision(1, 365);

create or replace function
getnForD(d integer)
returns integer as
$$
	SELECT minPeople FROM getMinPeopleCountForCollision(1, d) LIMIT 1;
$$  language sql;

SELECT d as "total Days in band",getnForD(d) as "Min People Required for Collision"
FROM generate_series(1,100) d;



\c postgres
drop database buddy;
