create database heaps;
\c heaps

-- Heap data structure
create table data(index int, value int);
insert into data values (1,3), (2,1), (3,2), (4,8), (5,7), (6,3), (7,6);


create table heapData (index int, value int);
create table sortedData (index int, value int);


----------------------------------------------------------------------------
-- helper functions
----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION swapElement(a int, b int)
RETURNS void AS
$$
  DECLARE aValue integer;
  DECLARE bValue integer;
  BEGIN
       SELECT INTO  aValue value from heapData where index = a;
       SELECT INTO  bValue value from heapData where index = b;

       update heapData
          set value= aValue
          where index=b;

      update heapData
         set value= bValue
         where index=a;
  END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION heap_up(child_index int)
RETURNS void AS
$$
  DECLARE parent_index integer;
  DECLARE parent_value integer;
  DECLARE child_value integer;
  BEGIN
      SELECT INTO  child_value h.value from heapData h where h.index=child_index;
      parent_index := floor(child_index / 2); --+1 because this will give us 0 indexed
      IF parent_index > 0
        THEN
            SELECT INTO  parent_value h.value from heapData h where h.index=parent_index; --in our relations, all tuples are 1 indexed
            IF child_value > parent_value
                THEN
                    perform swapElement(parent_index, child_index); --in our relations, all tuples are 1 indexed
                    perform heap_up(parent_index);
            END IF;
      ELSE parent_value :=0; --otherwise since there is no value it will come as null
      END IF;
  END;
$$ language plpgsql;


CREATE OR REPLACE FUNCTION heap_down(parent_index int)
RETURNS void AS
$$
  DECLARE largest_value_index integer;
  DECLARE largest_value integer;
  DECLARE parent_value integer;
  DECLARE left_child_index integer;
  DECLARE left_child_value integer;
  DECLARE right_child_index integer;
  DECLARE right_child_value integer;
  DECLARE heap_records_count integer;
  BEGIN
      SELECT INTO  heap_records_count COUNT(*) from heapData;
      left_child_index := 2 * parent_index;
      right_child_index := 2 * parent_index + 1;

      largest_value_index := parent_index; --parent value is expected to be the largest

      IF left_child_index > heap_records_count
        THEN
            left_child_value:=-2147483648; --min possible value for int if the we are at leaf parent
      ELSE
          SELECT INTO  left_child_value h.value from heapData h where h.index=left_child_index;
          SELECT INTO  largest_value h.value from heapData h where h.index=largest_value_index;

          IF left_child_value > largest_value
              THEN
                largest_value_index := left_child_index;
          END IF;
      END IF;

      IF right_child_index > heap_records_count
        THEN
            right_child_value:=-2147483648; --min possible value for int if the we are at leaf parent
      ELSE
          SELECT INTO  right_child_value h.value from heapData h where h.index=right_child_index;
          SELECT INTO  largest_value h.value from heapData h where h.index=largest_value_index;

          IF right_child_value > largest_value
              THEN
                largest_value_index := right_child_index;
          END IF;
      END IF;

      --if the largest element is not the parent, we need to swap
      IF largest_value_index <> parent_index
        THEN
            perform swapElement(parent_index, largest_value_index);
            perform heap_down(largest_value_index);
      END IF;
  END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION buildHeap()
RETURNS table (index int, value int) AS
$$
  DECLARE cursor record;
  BEGIN
       for cursor in select * from data
       loop
          perform insert(cursor.value);
       end loop;

    RETURN QUERY SELECT d.index, d.value FROM heapData d order by d.index;
  END;
$$ language plpgsql;

----------------------------------------------------------------------------
-- Function Definition
----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION insert(value int)
RETURNS void AS
$$
  DECLARE heap_records_count integer;
  DECLARE parent_index integer;
  DECLARE parent_value integer;
  DECLARE child_index integer;
  DECLARE child_value integer;
  BEGIN
      SELECT INTO  heap_records_count COUNT(*) from heapData;
      heap_records_count := heap_records_count +1;
      insert into heapData VALUES (heap_records_count, value); --inserting element in last position
      child_index := heap_records_count; --new record will be intserted in this index

      perform heap_up(child_index);
  END;
$$ language plpgsql;


CREATE OR REPLACE FUNCTION extractMax()
RETURNS integer AS
$$
  DECLARE max_value integer;
  DECLARE heap_records_count integer;
  DECLARE root_index integer;
  BEGIN
    root_index := 1;
    SELECT INTO  max_value h.value from heapData h where h.index=root_index;
    SELECT INTO  heap_records_count COUNT(*) from heapData;
    perform swapElement(root_index, heap_records_count);
    DELETE FROM heapData WHERE index=heap_records_count;
    perform heap_down(root_index);

    RETURN max_value;
  END;
$$ language plpgsql;

CREATE OR REPLACE FUNCTION heapSort()
RETURNS table (index int, value int) AS
$$
  DECLARE max_value integer;
  DECLARE heap_records_count integer;
  DECLARE sorted_index_pos integer;
  DECLARE root_index integer;
  BEGIN
    DELETE FROM sortedData; --delete all contents of the sorted table
    SELECT INTO  heap_records_count COUNT(*) from heapData;
    sorted_index_pos := heap_records_count;
    WHILE (sorted_index_pos > 0)
      LOOP
          SELECT INTO  max_value extractMax();
          INSERT INTO sortedData VALUES (sorted_index_pos, max_value);
          sorted_index_pos := sorted_index_pos - 1;
      END LOOP;

    RETURN QUERY SELECT d.index, d.value FROM sortedData d order by d.index;
  END;
$$ language plpgsql;


\qecho 'calling INSERT() function on each element of data using a cursor to populate heapData'

select * from buildHeap();

-- \qecho 'Heap built from data'
-- select * from heapData order by index;

\qecho 'calling HEAPSORT() function to sort data\n'
\qecho 'internally uses EXTRACTMAX() to extract the max element and the insert into sortedData relation as required'
select * from heapSort();

\c postgres
drop database heaps;
