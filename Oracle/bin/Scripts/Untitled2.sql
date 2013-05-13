delete from LINEITEM;
delete from customer;
delete from NATION;
delete from orders;
delete from part;
delete from partsupp;
delete from region;
delete from supplier;
commit;
rollback;
select DBMS_RANDOM.VALUE*9999 + SYSDATE from dual;

select * from lineitem;
select * from region;
select * from part;
select * from customer;
select * from orders;

SELECT l_shipdate, l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE 
1=1
--l_shipdate <= '07/11/99' 
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;


select * from user_ts_quotas;
purge recyclebin;

