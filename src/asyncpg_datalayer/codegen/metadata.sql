SELECT c.table_name,
       c.column_name,
       c.data_type,
       c.udt_name,
       c.ordinal_position,
       c.column_default,
       c.is_nullable::BOOLEAN                           AS is_nullable,
       EXISTS (SELECT 1
               FROM information_schema.key_column_usage kcu
                        JOIN information_schema.table_constraints tc
                             ON kcu.constraint_name = tc.constraint_name
                                 AND tc.constraint_type = 'PRIMARY KEY'
               WHERE kcu.table_name = c.table_name
                 AND kcu.column_name = c.column_name
                 AND kcu.table_schema = c.table_schema) AS is_primary_key
FROM information_schema.columns c
JOIN information_schema.tables t USING (table_schema, table_name)
WHERE c.table_schema = 'public'
AND t.table_type = 'BASE TABLE'
ORDER BY c.table_name, c.ordinal_position;
