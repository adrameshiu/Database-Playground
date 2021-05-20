create database sameGen;
\c sameGen


WITH
	RECURSIVE sameGeneration (x1,x2,n) AS (
	(
		SELECT p1.parent, p1.parent, 0
		FROM PC p1
		WHERE NOT EXISTS (
			SELECT *
			FROM PC p2
			WHERE p1.parent = p2.child
		)
	) UNION
	(
		SELECT p1.child, p2.child, s.n + 1
		FROM PC p1, PC p2, sameGeneration s
		WHERE s.x1 = p1.parent AND s.x2 = p2.parent --basically both parents will be at same level
	)
)

SELECT * FROM sameGeneration
ORDER BY n, x1,x2;


\c postgres
drop database sameGen;
