import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

//http://www.mkyong.com/mongodb/java-mongodb-hello-world-example/

public class Main {

	private Date qu1_data;
		
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
		
		try {
			qu1_data = new SimpleDateFormat("d/M/y", Locale.ENGLISH).parse("19/03/2020");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
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
			
			//updateValues();
			
			runQueries();
			
			insertBatchData(6000);
			
			runQueries();
			
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
		temps += exeQuery(1);
		temps += exeQuery(2);
		temps += exeQuery(3);
		temps += exeQuery(4);
		System.out.println("Avg de temps amb tots els inserts " + String.format("%s",(float)temps/(float)4) + " mili ");

	}
	
	private long exeQuery(int num){
		long temps = 9999999;
		for(int i = 0; i < 5; i++){
			long t = runQuery(num);
			if(t < temps && t != 0){
				temps = t;
			}
		}
		System.out.println("Query "+num+" trigat " + temps + " mili ");
		return temps;
	}
	
	private long runQuery(int num){
		switch(num){
			case 1:	return query1();
			case 2:	return query2();
			case 3:	return query3();
			case 4:	return query4();
		}
		return -1;
	}
	
	private long query1(){
		long temps_ini = 0, temps_fin = 0; 
		temps_ini = System.currentTimeMillis();
		
		List<HashMap<String, String>> resultat = new ArrayList<HashMap<String, String>>();
		
		DBCollection collection = db.getCollection("lineitem");
		
		BasicDBObject query = new BasicDBObject();
		query.put("l_shipdate", new BasicDBObject("$lte", qu1_data));
		collection.find(query);
		DBCursor cursor = collection.find()
			.sort(new BasicDBObject("l_linestatus", -1))
			.sort(new BasicDBObject("l_returnflag", -1));
	
		String last_l_returnflag = null;
		String last_l_linestatus = null;
		int i = 0, sum_qty = 0, sum_base_price = 0, sum_disc_price = 0, sum_charge = 0, sum_discount = 0;

		while(cursor.hasNext()) {
			DBObject ob = cursor.next();
			String l_returnflag = (String) ob.get("l_returnflag");
			String l_linestatus = (String) ob.get("l_linestatus");
			

			int l_discount = (Integer)ob.get("l_discount");
			int l_extendedprice = (Integer)ob.get("l_extendedprice");
			sum_qty += (Integer)ob.get("l_quantity");
			sum_base_price += l_extendedprice;
			sum_disc_price += l_extendedprice*(1-l_discount);
			sum_charge += l_extendedprice*(1-l_discount)*(1+(Integer)ob.get("l_tax"));
			sum_discount += l_discount;
			i++;
			
			// Mirem si pertany al groupby anterior
			boolean next_group = true;
			if(l_returnflag.equals(last_l_returnflag)){
				if(l_linestatus.equals(last_l_linestatus)){
					next_group = false;
				}
			}
			
			if(next_group){
				// Calculem la tupla
				HashMap<String, String> t = new HashMap<String, String>();
				t.put("l_returnflag", l_returnflag);
				t.put("l_linestatus", l_linestatus);
				t.put("sum_qty", ""+sum_qty);
				t.put("sum_base_price", ""+sum_base_price);
				t.put("sum_disc_price", ""+sum_disc_price);
				t.put("sum_charge", ""+sum_charge);
				t.put("avg_qty", ""+(sum_qty/i));
				t.put("avg_price", ""+(sum_base_price/i));
				t.put("avg_disc", ""+(sum_discount/i));
				t.put("count_order", ""+i);
				 
				resultat.add(t);

				// Ho deixem bé per la següent iteració
				last_l_returnflag = l_returnflag;
				last_l_linestatus = l_linestatus;
				sum_qty = 0; 
				sum_base_price = 0;
				sum_disc_price = 0;
				sum_charge = 0;
				sum_discount = 0;
				i = 0;
			}
		}

		temps_fin = System.currentTimeMillis();
		return temps_fin-temps_ini;
	}
	
	private long query2(){
		return 1;
	}	
	
	private long query3(){
		return 1;
	}	
	
	private long query4(){
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
		llista.put("partsupp",insertsPartSupp(start));
		llista.put("orders",insertsOrders(start));
		llista.put("lineitem",insertsLineItem(start));
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
			document.append("_id", i);
			document.append("r_name", rStr(64/2));
			document.append("r_comment", rStr(160/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsNation(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 25+start; i++){
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("n_name", rStr(64/2));
			document.append("n_regionkey", rBetween(start,start+5));
			document.append("n_comment", rStr(160/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsPart(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 666+start; i++){
			cacheClausPartkey.add(i);
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("p_name", rStr(64/2));
			document.append("p_mfgr", rStr(64/2));
			document.append("p_brand", rStr(64/2));
			document.append("p_type", rStr(64/2));
			document.append("p_size", rInt(4));
			document.append("p_container", rStr(64/2));
			document.append("p_retailprice", rInt(4));
			document.append("p_comment", rStr(64/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsSupplier(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 33+start; i++){
			cacheClausSuppkey.add(i);
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("s_name", rStr(64/2));
			document.append("s_address", rStr(64/2));
			document.append("s_nationkey", rBetween(start,start+25));
			document.append("s_phone", rStr(18/2));
			document.append("s_acctbal", rInt(13/2));
			document.append("s_comment", rStr(105/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsCustomer(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 499+start; i++){
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("c_name", rStr(64/2));
			document.append("c_address", rStr(64/2));
			document.append("c_nationkey", rBetween(start,start+25));
			document.append("c_phone", rStr(18/2));
			document.append("c_acctbal", rInt(13/2));
			document.append("c_mktsegment", rStr(64/2));
			document.append("c_comment", rStr(102/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
	
	private List<BasicDBObject> insertsPartSupp(int start) {
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 2666+start; i++){
			int partkey = cacheClausPartkey.get(rBetween(0, cacheClausPartkey.size()));
			int suppkey = cacheClausSuppkey.get(rBetween(0, cacheClausSuppkey.size()));
			Tuple t = new Tuple(partkey, suppkey);
			cacheClausPartSup.add(t);
			
			BasicDBObject document = new BasicDBObject();
			document.append("ps_partkey", partkey);
			document.append("ps_suppkey", suppkey);
			document.append("ps_availqty", rInt(4));
			document.append("ps_supplycost", rInt(13/2));
			document.append("ps_comment", rStr(200/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}
		
	private List<BasicDBObject> insertsOrders(int start){
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 4999+start; i++){			
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("o_custkey", rBetween(start,start+499));
			document.append("o_orderstatus", rStr(64/2));
			document.append("o_totalprice", rInt(13/2));
			document.append("o_orderdate", rDate());
			document.append("o_orderpriority", rStr(15/2));
			document.append("o_clerk", rStr(64/2));
			document.append("o_shippriority", rInt(4));
			document.append("o_comment", rStr(80/2));
			document.append("skip", rStr(64/2));
			res.add(document);
		}
		return res;
	}

	
	private List<BasicDBObject> insertsLineItem(int start) {
		List<BasicDBObject> res = new ArrayList<BasicDBObject>();
		for(int i = start; i < 19999+start; i++){
			Tuple t = cacheClausPartSup.get(rBetween(0, cacheClausPartSup.size()));
			BasicDBObject document = new BasicDBObject();
			document.append("l_orderkey", rBetween(start,start+4999));
			document.append("l_partkey", t.a);
			document.append("l_suppkey", t.b);
			document.append("l_linenumber", rInt(4));
			document.append("l_quantity", rInt(4));
			document.append("l_extendedprice", rInt(13/2));
			document.append("l_discount", rInt(13/2));
			document.append("l_tax", rInt(13/2));
			document.append("l_returnflag", rStr(64/2));
			document.append("l_linestatus", rStr(64/2));
			document.append("l_shipdate", rDate());
			document.append("l_commitdate", rDate());
			document.append("l_receiptdate", rDate());
			document.append("l_shipinstruct", rStr(64/2));
			document.append("l_shipmode", rStr(64/2));
			document.append("l_comment", rStr(64/2));
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
		long unixtime=(long) (946706400+rnd.nextDouble()*60*60*24*365*100000);
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