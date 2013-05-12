---------- QUERY1: ------------
SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= '19/03/30' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
---------- QUERY2: ------------
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = '6424' AND p_type like '%NICJLSXFGKQWTDYRXNKIPJILDCCUVLNF' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'FPLUMCADWMKERFDSCDOHFCSTNROPSYWJ' AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'FPLUMCADWMKERFDSCDOHFCSTNROPSYWJ') ORDER BY s_acctbal desc, n_name, s_name, p_partkey
---------- QUERY3: ------------
SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'KENNZVGOFQSVSCMJRJFFAJCLAUVTSKXC' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '05/12/2018' AND l_shipdate > '01/09/2014' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue desc, o_orderdate
---------- QUERY4: ------------
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'MFONEZPIQJMFMLJFPONZRHRNBVEKPDTV' AND o_orderdate >= '28/05/2015' AND o_orderdate < '27/05/2016' GROUP BY n_name ORDER BY revenue desc