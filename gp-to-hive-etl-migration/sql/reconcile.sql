-- Run the same query against the old and the new source, then compare the results.
-- Replace :source_table with the table that is being checked.
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT item_id) AS distinct_items,
       SUM(CASE WHEN item_name IS NULL THEN 1 ELSE 0 END) AS null_names
FROM :source_table;
