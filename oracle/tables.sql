--查询所有表名：
select t.table_name from user_tables t;
--查询所有字段名：
select t.column_name from user_col_comments t;
--查询指定表的所有字段名：
select t.column_name from user_col_comments t where t.table_name = 'BIZ_DICT_XB';
--查询指定表的所有字段名和字段说明：
select t.column_name, t.column_name from user_col_comments t where t.table_name = 'BIZ_DICT_XB';
--查询所有表的表名和表说明：
select t.table_name,f.comments from user_tables t inner join user_tab_comments f on t.table_name = f.table_name;
--查询模糊表名的表名和表说明：
select t.table_name from user_tables t where t.table_name like 'BIZ_DICT%';
select t.table_name,f.comments from user_tables t inner join user_tab_comments f on t.table_name = f.table_name where t.table_name like 'BIZ_DICT%';

--查询表的数据条数、表名、中文表名
select a.num_rows, a.TABLE_NAME, b.COMMENTS
from user_tables a, user_tab_comments b
WHERE a.TABLE_NAME = b.TABLE_NAME
order by TABLE_NAME;