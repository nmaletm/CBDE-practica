select * from user_ts_quotas;
select bytes / max_bytes * 100 from user_ts_quotas where tablespace_name = 'USERS';

select max(l_shipdate), min(l_shipdate) from lineitem;

Begin
for t in (select view_name from user_views) loop
execute immediate ('drop view '||t.view_name);
end loop;
for t in (select mview_name from user_mviews) loop
execute immediate ('drop materialized view '||t.mview_name);
end loop;
for t in (select table_name from user_tables) loop
execute immediate ('drop table '||t.table_name||' cascade constraints');
end loop;
for c in (select cluster_name from user_clusters) loop
execute immediate ('drop cluster '||c.cluster_name);
end loop;
for i in (select index_name from user_indexes) loop
execute immediate ('drop index '||i.index_name);
end loop;
for i in (select type_name from user_types where typecode = 'COLLECTION') loop
execute immediate ('drop type '||i.type_name);
end loop;
for i in (select type_name from user_types) loop
execute immediate ('drop type '||i.type_name);
end loop;
execute immediate ('purge recyclebin');
End;
