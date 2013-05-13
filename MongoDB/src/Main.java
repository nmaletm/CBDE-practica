import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

//http://www.mkyong.com/mongodb/java-mongodb-hello-world-example/

public class Main {

	private String qu1_data = "19/03/30";
		
	private String qu2_size;
	private String qu2_type;
	private String qu2_region;
	
	private String qu3_segment;
	private String qu3_data1;
	private String qu3_data2;

	private String qu4_region;
	private String qu4_data1;
	private String qu4_data2;
	
	private DB db;
	private Random rnd = new Random();

	private List<Tuple> cacheClausPartSup = new ArrayList<Tuple>();
	private List<Integer> cacheClausPartkey = new ArrayList<Integer>();
	private List<Integer> cacheClausSuppkey = new ArrayList<Integer>();
	
	private String QUERY1;
	private String QUERY2;
	private String QUERY3;
	private String QUERY4;
	
	private List<String> taules;
	

	public Main(){
		taules = new ArrayList<String>();
		taules.add("lineitem");
		taules.add("nation");
		taules.add("customer");
		taules.add("orders");
		taules.add("part");
		taules.add("partsupp");
		taules.add("region");
		taules.add("supplier");
		
	}

	public static void main(String[] argv) {
		Main m = new Main();
		m.run();
	}
	
	private void run(){
		try {
			MongoClient mongo = new MongoClient( "localhost" , 27017 );
			db = mongo.getDB("CBDE");
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
 
		try{
			deleteCollections();
			
			insertBatchData(1000);
			
			updateValues();
			
			//runQueries();
			
			insertBatchData(6000);
			
			//runQueries();
			
		}
		catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		
	}
		
	/*  ---------------------- Queries ------------------------------  */
	
	private void runQueries(){
		System.out.println("------------- QUERIES ---------------");
		long temps = 0;
		temps += exeQuery(1,QUERY1);
		temps += exeQuery(2,QUERY2);
		temps += exeQuery(3,QUERY3);
		temps += exeQuery(4,QUERY4);
		System.out.println("Avg de temps amb tots els inserts " + String.format("%s",(float)temps/(float)4) + " mili ");

	}
	
	private long exeQuery(int num, String sql){
		long temps = 9999999;
		for(int i = 0; i < 5; i++){
			long t = executeQuery(num, sql);
			if(t < temps && t != 0){
				temps = t;
			}
		}
		System.out.println("Query "+num+" trigat " + temps + " mili ");
		return temps;
	}
	
	
	private long executeQuery(int num, String sql){
/*		long temps_ini = 0, temps_fin = 0; 
		boolean teResultat = false;
		
		int intents = 20;
		
		while(!teResultat && intents > 0){
			temps_ini = System.currentTimeMillis();
			ResultSet resultSet = statement.executeQuery(sql);
			temps_fin = System.currentTimeMillis();
			if (resultSet.next()){
				teResultat = true;
			}
			intents--;
		}
		if(intents == 0){
			System.out.println("Me pasaaoooo!! No trobo cap resultat :(  (query "+num+")");
		}
		return temps_fin-temps_ini;*/
		return 1;
	}


	
	/*  ---------------------- Inserts ------------------------------  */
	
	private void insertBatchData(int start) throws SQLException{
		System.out.println("------------- INSERTS ---------------");

		Map<String,List<BasicDBObject>> llista = new HashMap<String,List<BasicDBObject>>();
		System.out.println("Preparem valors inserts");
		llista.put("region",insertsRegion(start));
		llista.put("nation",insertsNation(start));
		llista.put("part",insertsPart(start));
		llista.put("supplier",insertsSupplier(start));
		llista.put("customer",insertsCustomer(start));
		llista.put("partSupp",insertsPartSupp(start));
		llista.put("orders",insertsOrders(start));
		llista.put("lineItem",insertsLineItem(start));
		System.out.println("Valors preparats, ara anem a fer insert");

		long temps_ini, temps_fin; 
		temps_ini = System.currentTimeMillis();
		for (Map.Entry<String, List<BasicDBObject>> entry : llista.entrySet()) {
		    String name_collection = entry.getKey();
		    List<BasicDBObject> values = entry.getValue();
		    DBCollection col = db.getCollection(name_collection);
		    for(BasicDBObject ob : values){
		    	col.save(ob);
		    }
		}
		temps_fin = System.currentTimeMillis();
		System.out.println("S'ha trigat a fer els inserts: " + (temps_fin-temps_ini) + " mili ");
		
	}
	
	private List<BasicDBObject> insertsRegion(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){
			BasicDBObject document = new BasicDBObject();
			document.append("R_RegionKey", i);
			document.append("R_Name", rStr(64/2));
			document.append("R_Comment", rStr(160/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsNation(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 25+start; i++){
			BasicDBObject document = new BasicDBObject();
			document.append("N_NationKey", i);
			document.append("N_Name", rStr(64/2));
			document.append("N_RegionKey", rBetween(start,start+5));
			document.append("N_Comment", rStr(160/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsPart(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){
			cacheClausPartkey.add(i);
			BasicDBObject document = new BasicDBObject();
			document.append("P_PartKey", i);
			document.append("P_Name", rStr(64/2));
			document.append("P_Mfgr", rStr(64/2));
			document.append("P_Brand", rStr(64/2));
			document.append("P_Type", rStr(64/2));
			document.append("P_Size", rInt(4));
			document.append("P_Container", rStr(64/2));
			document.append("P_RetailPrice", rInt(4));
			document.append("P_Comment", rStr(64/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsSupplier(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){
			cacheClausSuppkey.add(i);
			BasicDBObject document = new BasicDBObject();
			document.append("S_SuppKey", i);
			document.append("S_Name", rStr(64/2));
			document.append("S_Address", rStr(64/2));
			document.append("S_NationKey", rBetween(start,start+25));
			document.append("S_Phone", rStr(18/2));
			document.append("S_AcctBal", rInt(13/2));
			document.append("S_Comment", rStr(105/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsCustomer(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){
			BasicDBObject document = new BasicDBObject();
			document.append("C_CustKey", i);
			document.append("C_Name", rStr(64/2));
			document.append("C_Address", rStr(64/2));
			document.append("C_NationKey", rBetween(start,start+25));
			document.append("C_Phone", rStr(18/2));
			document.append("C_AcctBal", rInt(13/2));
			document.append("C_MktSegment", rStr(64/2));
			document.append("C_Comment", rStr(102/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsPartSupp(int start) {
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){
			int partkey = cacheClausPartkey.get(rBetween(0, cacheClausPartkey.size()));
			int suppkey = cacheClausSuppkey.get(rBetween(0, cacheClausSuppkey.size()));
			Tuple t = new Tuple(partkey, suppkey);
			cacheClausPartSup.add(t);
			
			BasicDBObject document = new BasicDBObject();
			document.append("PS_PartKey", partkey);
			document.append("PS_SuppKey", suppkey);
			document.append("PS_AvailQty", rInt(4));
			document.append("PS_SupplyCost", rInt(13/2));
			document.append("PS_Comment", rStr(200/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
		
	private List<BasicDBObject> insertsOrders(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){			
			BasicDBObject document = new BasicDBObject();
			document.append("O_OrderKey", i);
			document.append("O_CustKey", rBetween(start,start+499));
			document.append("O_OrderStatus", rStr(64/2));
			document.append("O_TotalPrice", rInt(13/2));
			document.append("O_OrderDate", rDate());
			document.append("O_OrderPriority", rStr(15/2));
			document.append("O_Clerk", rStr(64/2));
			document.append("O_ShipPriority", rInt(4));
			document.append("O_Comment", rStr(80/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}

	
	private List<BasicDBObject> insertsLineItem(int start) {
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 5+start; i++){
			Tuple t = cacheClausPartSup.get(rBetween(0, cacheClausPartSup.size()));
			
			BasicDBObject document = new BasicDBObject();
			document.append("L_OrderKey", rBetween(start,start+4999));
			document.append("L_PartKey", t.a);
			document.append("L_SuppKey", t.b);
			document.append("L_LineNumber", rInt(4));
			document.append("L_Quantity", rInt(4));
			document.append("L_ExtendedPrice", rInt(13/2));
			document.append("L_Discount", rInt(13/2));
			document.append("L_Tax", rInt(13/2));
			document.append("L_ReturnFlag", rStr(64/2));
			document.append("L_LineStatus", rStr(64/2));
			document.append("L_ShipDate", rDate());
			document.append("L_CommitDate", rDate());
			document.append("L_ReceiptDate", rDate());
			document.append("L_ShipInstruct", rStr(64/2));
			document.append("L_ShipMode", rStr(64/2));
			document.append("L_Comment", rStr(64/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	

	private void deleteCollections(){
		System.out.println("---------- DELETE COLLECTIONS ------------");
		for(String taula : taules){
			db.getCollection(taula).drop();
		}
	}
	
	
	/*  ---------------------- Calcular dades -----------------------  */
	private void updateValues(){
/*		System.out.println("---------- UPDATE VALUES ------------");
		try{
			Statement statement = connection.createStatement();
			String sql = "SELECT p_size, p_type, r_name FROM part, supplier, partsupp, nation, region WHERE "+
	"  rownum = 1 AND"+
	"  p_partkey = ps_partkey AND "+
	"  s_suppkey = ps_suppkey AND "+
	"  s_nationkey = n_nationkey AND "+
	"  n_regionkey = r_regionkey AND "+
	"  ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE "+
	"                    p_partkey = ps_partkey AND "+
	"                    s_suppkey = ps_suppkey AND "+
	"                    s_nationkey = n_nationkey AND "+
	"                    n_regionkey = r_regionkey  "+
	 "                   ) ORDER BY s_acctbal desc, n_name, s_name, p_partkey";
			ResultSet resultSet = statement.executeQuery(sql);
			if (resultSet.next()){
				qu2_size = resultSet.getString(1);
				qu2_type = resultSet.getString(2);
				qu2_region = resultSet.getString(3);		
			}
			sql = "SELECT c_mktsegment, to_char(o_orderdate+1, 'DD/MM/YYYY') , to_char(l_shipdate-1, 'DD/MM/YYYY')  "+
	" FROM customer, orders, lineitem WHERE "+
	"  rownum = 1 and "+
	"  c_custkey = o_custkey AND "+
	"  l_orderkey = o_orderkey ";
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()){
				qu3_segment = resultSet.getString(1);
				qu3_data1 = resultSet.getString(2);
				qu3_data2 = resultSet.getString(3);
			}
			
			sql = "SELECT r_name, to_char(o_orderdate, 'DD/MM/YYYY'), to_char(o_orderdate+365, 'DD/MM/YYYY') "+
	"	FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey "+
	"  and rownum = 1 "+
	"	AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey "+
	"	AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey ";

			resultSet = statement.executeQuery(sql);
			if (resultSet.next()){
				qu4_region = resultSet.getString(1);
				qu4_data1 = resultSet.getString(2);
				qu4_data2 = resultSet.getString(3);
			}
		}
		catch(Exception e){
			System.out.println("No s'han pogut posar els valors per les queries");
		}
		setQueries();
*/	}
	
	private void setQueries(){
		QUERY1 = "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= '"+qu1_data+"' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
		QUERY2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = '"+qu2_size+"' AND p_type like '%"+qu2_type+"' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu2_region+"' AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu2_region+"') ORDER BY s_acctbal desc, n_name, s_name, p_partkey";
		QUERY3 = "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = '"+qu3_segment+"' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '"+qu3_data1+"' AND l_shipdate > '"+qu3_data2+"' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue desc, o_orderdate";
		QUERY4 = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu4_region+"' AND o_orderdate >= '"+qu4_data1+"' AND o_orderdate < '"+qu4_data2+"' GROUP BY n_name ORDER BY revenue desc";
	
	}
	

	/*  ---------------------- Randoms ------------------------------  */

	
	private final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private final String NUMB = "0123456789";
	
	public int rBetween(int min, int max){
		return min+(int)(Math.random() * ( max - min ));
	}

	public String rStr(int len){
		StringBuilder sb = new StringBuilder( len );
		for( int i = 0; i < len; i++ ) 
			sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
		return sb.toString();
	}

	public int rInt(int len){
		StringBuilder sb = new StringBuilder( len );
		for( int i = 0; i < len; i++ ) 
			sb.append( NUMB.charAt( rnd.nextInt(NUMB.length()) ) );
		return Integer.valueOf(sb.toString());
	}
	
	public Date rDate(){
		long unixtime=(long) (946706400+rnd.nextDouble()*60*60*24*365*100);
		return new Date(unixtime);
	}

	private class Tuple{
		public int a;
		public int b;
		
		public Tuple(int a, int b){
			this.a = a;
			this.b = b;
		}
	}
}