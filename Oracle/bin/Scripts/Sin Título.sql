Com fer B+:
ALTER TABLE poll_answers SHRINK SPACE;-- Compress the table
CREATE INDEX test ON poll_answers (edat, pobl, val) PCTFREE 33;

BitMaps:
ALTER TABLE poll_answers MINIMIZE RECORDS_PER_BLOCK;
CREATE BITMAP INDEX bitmap1 ON poll_answers(cand, val) PCTFREE 0;

select r_regionkey from region;

ALTER TABLE customer add CONSTRAINT customer_pk PRIMARY KEY (c_custkey);
ALTER TABLE nation add CONSTRAINT nation_pk PRIMARY KEY (n_nationkey);
ALTER TABLE orders add CONSTRAINT orders_pk PRIMARY KEY (o_orderkey);
ALTER TABLE part add CONSTRAINT part_pk PRIMARY KEY (p_partkey);
ALTER TABLE region add CONSTRAINT region_pk PRIMARY KEY (r_regionkey);
ALTER TABLE supplier add CONSTRAINT supplier_pk PRIMARY KEY (s_suppkey);

--ALTER TABLE partsupp add CONSTRAINT partsupp_pk PRIMARY KEY (ps_suppkey,ps_partkey);
--ALTER TABLE lineitem add CONSTRAINT lineitem_pk PRIMARY KEY (l_orderkey,l_suppkey,l_partkey);

CREATE INDEX PARTSUPP_INDEX1 ON PARTSUPP (PS_PARTKEY, PS_SUPPKEY) PCTFREE 0;
CREATE INDEX LINEITEM_INDEX1 ON LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY) PCTFREE 0;


select p_size,p_type from part;
select r_name from region;

delete from customer;
delete from LINEITEM;
delete from NATION;
delete from orders;
delete from part;
delete from partsupp;
delete from region;
delete from supplier;

drop table customer;
drop table LINEITEM;
drop table NATION;
drop table orders;
drop table part;
drop table partsupp;
drop table region;
drop table supplier;

select * from lineitem;


select count(*) from lineitem;

select count(*) from customer;
select count(*) from LINEITEM;
select count(*) from NATION;
select count(*) from orders;
select count(*) from part;
select count(*) from partsupp;
select count(*) from region;
select count(*) from supplier;