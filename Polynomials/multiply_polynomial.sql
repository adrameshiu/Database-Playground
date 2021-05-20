create database multiply;
\c multiply

create table P(coefficient NUMERIC, degree integer);
insert into P values  (2, 2),  (-3, 1),  (1, 0);

create table Q(coefficient NUMERIC, degree integer);
insert into Q values  (4, 3),  (0, 2),  (-3,1),  (0,0);

create or replace function multiplyPolynomials(polynomial1 text, polynomial2 text)
	returns table(coefficient NUMERIC, degree integer) as
  $$
  begin
    return query
	execute '
			WITH
			A AS (
				select *
				from '||polynomial1||'
				),
			B AS (
				select *
				from '||polynomial2||'
				),
			AB_polynomial_relation AS (
				SELECT A.coefficient as Acoefficient, A.degree AS Adegree,
				B.coefficient as Bcoefficient, B.degree AS Bdegree
				FROM A,B
			  ),
			AxB_relation_mul AS (
				SELECT (AB.acoefficient * AB.bcoefficient) AS axb_coeff, (AB.adegree + AB.bdegree) AS AxB_degree
				FROM AB_polynomial_relation ab
				ORDER BY axb_degree
			),
			AxB AS (
				SELECT SUM(AB.AxB_coeff) AS coefficient , AB.axb_degree AS degree
				FROM AxB_relation_mul AB
				GROUP BY AB.AxB_degree
				ORDER BY AB.AxB_degree DESC
			)

SELECT AB.coefficient, AB.degree FROM AxB AB
	';

  end;
  $$ language plpgsql;


\qecho 'P * Q'
select * from multiplyPolynomials('P', 'Q');

\qecho 'P * P'
select * from multiplyPolynomials('P', 'P');

\qecho 'P * (Q * P)'
select * from multiplyPolynomials('P', 'multiplyPolynomials(''Q'',''P'')');


\c postgres
drop database multiply;
