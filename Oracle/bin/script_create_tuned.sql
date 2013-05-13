CREATE TABLE customer(
    C_CustKey int NULL,
    C_Name varchar2(64) NULL,
    C_Address varchar2(64) NULL,
    C_NationKey int NULL,
    C_Phone varchar2(64) NULL,
    C_AcctBal number(13, 2) NULL,
    C_MktSegment varchar2(64) NULL,
    C_Comment varchar2(120) NULL,
    skip varchar2(64) NULL
)PCTFREE 0 ENABLE ROW MOVEMENT;

CREATE TABLE nation(
    N_NationKey int NULL,
    N_Name varchar2(64) NULL,
    N_RegionKey int NULL,
    N_Comment varchar2(160) NULL,
    skip varchar2(64) NULL
);

CREATE TABLE orders(
    O_OrderKey int NULL,
    O_CustKey int NULL,
    O_OrderStatus varchar2(64) NULL,
    O_TotalPrice number(13, 2) NULL,
    O_OrderDate date NULL,
    O_OrderPriority varchar2(15) NULL,
    O_Clerk varchar2(64) NULL,
    O_ShipPriority int NULL,
    O_Comment varchar2(80) NULL,
    skip varchar2(64) NULL
)PCTFREE 0 ENABLE ROW MOVEMENT;

CREATE TABLE part(
    P_PartKey int NULL,
    P_Name varchar2(64) NULL,
    P_Mfgr varchar2(64) NULL,
    P_Brand varchar2(64) NULL,
    P_Type varchar2(64) NULL,
    P_Size int NULL,
    P_Container varchar2(64) NULL,
    P_RetailPrice number(13, 2) NULL,
    P_Comment varchar2(64) NULL,
    skip varchar2(64) NULL
)PCTFREE 0 ENABLE ROW MOVEMENT;

CREATE TABLE partsupp(
    PS_PartKey int NULL,
    PS_SuppKey int NULL,
    PS_AvailQty int NULL,
    PS_SupplyCost number(13, 2) NULL,
    PS_Comment varchar2(200) NULL,
    skip varchar2(64) NULL
)PCTFREE 0 ENABLE ROW MOVEMENT;

CREATE TABLE region(
    R_RegionKey int NULL,
    R_Name varchar2(64) NULL,
    R_Comment varchar2(160) NULL,
    skip varchar2(64) NULL
)PCTFREE 0 ENABLE ROW MOVEMENT;

CREATE TABLE supplier(
    S_SuppKey int NULL,
    S_Name varchar2(64) NULL,
    S_Address varchar2(64) NULL,
    S_NationKey int NULL,
    S_Phone varchar2(18) NULL,
    S_AcctBal number(13, 2) NULL,
    S_Comment varchar2(105) NULL,
    skip varchar2(64) NULL
)PCTFREE 0 ENABLE ROW MOVEMENT;

CREATE TABLE lineitem(
    L_OrderKey int NULL,
    L_PartKey int NULL,
    L_SuppKey int NULL,
    L_LineNumber int NULL,
    L_Quantity int NULL,
    L_ExtendedPrice number(13, 2) NULL,
    L_Discount number(13, 2) NULL,
    L_Tax number(13, 2) NULL,
    L_ReturnFlag varchar2(64) NULL,
    L_LineStatus varchar2(64) NULL,
    L_ShipDate date NULL,
    L_CommitDate date NULL,
    L_ReceiptDate date NULL,
    L_ShipInstruct varchar2(64) NULL,
    L_ShipMode varchar2(64) NULL,
    L_Comment varchar2(64) NULL,
    skip varchar2(64) NULL
)
PARTITION BY RANGE (L_ShipDate)
(
  PARTITION part1 VALUES LESS THAN (TO_DATE('01/01/2020', 'DD/MM/YYYY')),
  PARTITION part2 VALUES LESS THAN (TO_DATE('01/01/2027', 'DD/MM/YYYY')),
  PARTITION part3 VALUES LESS THAN (TO_DATE('01/01/2034', 'DD/MM/YYYY')),
  PARTITION part4 VALUES LESS THAN (MAXVALUE)
);

CREATE INDEX taula_q ON lineitem (l_shipdate) PCTFREE 33;
CREATE INDEX t ON lineitem (L_SuppKey, L_OrderKey) PCTFREE 33;
CREATE INDEX t1 ON part (P_partkey, P_Type, P_Size) PCTFREE 33;


CREATE INDEX bplus_1 ON customer(c_mktsegment, c_CustKey) PCTFREE 33;
CREATE INDEX bplus_2 ON customer(c_nationkey) PCTFREE 33;

CREATE INDEX bplus_3 ON nation(n_regionkey) PCTFREE 33;

CREATE INDEX bplus_4 ON orders(o_CustKey) PCTFREE 33;
CREATE INDEX bplus_5 ON orders(O_OrderDate, O_ShipPriority) PCTFREE 33;

CREATE INDEX bplus_6 ON region(r_name) PCTFREE 33;

CREATE INDEX bplus_7 ON supplier(s_suppkey) PCTFREE 33;
CREATE INDEX bplus_8 ON supplier(s_nationkey) PCTFREE 33;

CREATE INDEX bplus_9 ON partsupp(PS_PartKey, PS_SuppKey) PCTFREE 33;