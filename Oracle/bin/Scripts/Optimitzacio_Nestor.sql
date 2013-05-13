BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q2'); end;
/
BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q2_SUB'); end;
/
BEGIN
DBMS_SNAPSHOT.REFRESH('VISTA_Q3'); 
end;
/
BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q4'); end;
/

SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order 
  FROM lineitem 
  WHERE l_shipdate <= '19/03/30' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

drop index taula_q1;

CREATE INDEX taula_q1 ON lineitem (l_shipdate, l_returnflag, l_linestatus,l_quantity, l_extendedprice, l_discount, l_tax) PCTFREE 33;

CREATE INDEX taula_q1 ON lineitem (l_shipdate) PCTFREE 33;
--- Query 2:

CREATE MATERIALIZED VIEW vista_q2 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE 
AS (
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost, r_name, p_type, p_size
  FROM part, supplier, partsupp, nation, region 
  WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND 
     s_nationkey = n_nationkey AND n_regionkey = r_regionkey
);
CREATE INDEX vista_q2 ON vista_q2 (r_name, p_size, p_type) PCTFREE 33;

CREATE MATERIALIZED VIEW vista_q2_sub ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE
AS (
  SELECT min(ps_supplycost) as min_ps_supplycost, ps_partkey, r_name FROM partsupp, supplier, nation, region WHERE s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey group by ps_partkey, r_name
);
CREATE INDEX vista_q2_sub ON vista_q2_sub (r_name, ps_partkey, min_ps_supplycost) PCTFREE 33;

-- Query 3
CREATE MATERIALIZED VIEW vista_q3 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE 
AS (
SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority, l_shipdate, c_mktsegment
FROM customer, orders, lineitem WHERE 
c_custkey = o_custkey AND l_orderkey = o_orderkey 
GROUP BY l_orderkey, o_orderdate, o_shippriority, l_shipdate, c_mktsegment
);
CREATE INDEX vista_q3 ON vista_q3 (c_mktsegment, l_shipdate,o_orderdate) PCTFREE 33;

--  Query 4
CREATE MATERIALIZED VIEW vista_q4 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE
AS (
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue, r_name, o_orderdate
 FROM customer, orders, lineitem, supplier, nation, region
 WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND
    s_nationkey = n_nationkey AND n_regionkey = r_regionkey
    GROUP BY n_name, r_name, o_orderdate
);
CREATE INDEX vista_q4 ON vista_q4 (r_name, o_orderdate) PCTFREE 33;