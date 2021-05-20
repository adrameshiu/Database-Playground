create database bool_multiply;
\c bool_multiply

create or replace function booleanMatrixMultiplication (matrix1 text, matrix2 text)
returns table (rw integer, colmn integer, value boolean) as
$$
begin
    return query
	execute '
		WITH
		M AS (
			select *
			from '||matrix1||'
	),
		N AS (
			select *
			from '||matrix2||'
	)

	select M.rw,  N.colmn, (SELECT EXISTS(
								SELECT M1.rw AS Mrow, N1.colmn AS Ncol
								FROM M M1, N N1
								WHERE (M1.colmn = N1.rw)
										AND (M1.value = true AND N1.value = true)
										AND (M1.rw= M.rw AND N1.colmn = N.colmn)
							 )
						   )
	FROM M m,N n
	GROUP BY (M.rw, N.colmn)
	ORDER BY M.rw, N.colmn
';



  end;
  $$ language plpgsql;


  \c postgres
  drop database bool_multiply;
