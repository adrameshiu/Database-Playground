create database composition;
\c composition


create or replace function polyPower(poly text, powr numeric)
   returns table(coefficient NUMERIC, degree integer) as
	$polyPower$
    begin
		IF powr=0 THEN
			RETURN QUERY
				SELECT 1::numeric, 0;
		ELSIF powr=1 THEN
			RETURN QUERY
				execute 'SELECT p2.coefficient, p2.degree FROM '||poly||' p2';
		ELSIF powr=2  THEN
			RETURN QUERY
				execute 'SELECT * FROM  multiplyPolynomials('''||poly||''', '''||poly||''')';
		ELSE
			RETURN	QUERY
				execute 'SELECT * FROM multiplyPolynomials('''||poly||''', ''polyPower('''''||poly||''''','''''||powr -1||''''')'')';
		END IF;  --base case
    end;
	$polyPower$ language plpgsql;

create or replace function compositionPolynomials(polynomial1 text, polynomial2 text)
	returns table(coefficient numeric, degree integer) as
	  $$
	  begin
		return query
		execute '
				WITH
				A AS (
					select *
					from '||polynomial1||'
					)

	select SUM(a.coefficient * b.coefficient) AS coefficient, b.degree
	FROM  A a, polyPower('''||polynomial2||''',a.degree) b
	GROUP BY B.degree
	ORDER BY B.degree DESC;
		';

  end;
  $$ language plpgsql;

\qecho 'P (Q)'

SELECT * FROM compositionPolynomials('P','Q');


\qecho 'Q(P)'
SELECT * FROM compositionPolynomials('Q','P');

\qecho 'P(P(P))'
CREATE TABLE compo(coefficient numeric, degree integer);
INSERT INTO compo (SELECT * FROM compositionPolynomials('P','P'));
select * from compositionPolynomials('P', 'compo');


\c postgres
drop database composition;
