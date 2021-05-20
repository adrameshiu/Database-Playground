Dynamic SQL  permits writing programs that generate queries, that can also be evaluated in the programs that generated them.
We can construct strings that represent an SQL query dynamically(basically passing arguments as text to  a function).

When an execute statement in the dynamic
program is then applied to that string, the corresponding query is
evaluated.

## Useful Resources

https://www.postgresql.org/docs/9.1/ecpg-dynamic.html