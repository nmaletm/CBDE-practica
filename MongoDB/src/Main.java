import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

//http://www.mkyong.com/mongodb/java-mongodb-hello-world-example/

public class Main {

	private Date qu1_data;
		
	private int qu2_size;
	private String qu2_type;
	private String qu2_region;
	
	private String qu3_segment;
	private Date qu3_data1;
	private Date qu3_data2;

	private String qu4_region;
	private Date qu4_data1;
	private Date qu4_data2;
	
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

			qu2_size = 5212;
			qu2_type = "RDEWNKUCPWVEJQTNFMCJVFEARHEFITHH";
			qu2_region = "JQNWOMRCBAGNMFKDMVYQSXIFPFHBRCCV";
			
			qu3_segment = "SLVWXUKNFHLYWLKGLHZFOAPTLUAUNTWS";
			qu3_data1 = new SimpleDateFormat("d/M/y", Locale.ENGLISH).parse("01/09/2040");
			qu3_data2 = new SimpleDateFormat("d/M/y", Locale.ENGLISH).parse("28/08/1992");
			
			qu4_region = qu2_region;
			qu4_data1 = qu3_data1;
			qu4_data2 = new SimpleDateFormat("d/M/y", Locale.ENGLISH).parse("01/09/2031");
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
			
			runQueries();
			
			insertBatchData(6000);
			
			runQueries();
			
		}
		catch (Exception e) {
			e.printStackTrace();
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
		long temps = Long.MAX_VALUE;
		for(int i = 0; i < 5; i++){
		//for(int i = 0; i < 1; i++){
			long t = runQuery(num);
			if(t < temps && t != 0){
				temps = t;
			}
			System.out.print(".");
		}
		System.out.println("Query "+num+" trigat " + temps + " mili ");
		return temps;
	}
	
	private long runQuery(int num){
		long temps_ini = 0, temps_fin = 0; 
		temps_ini = System.currentTimeMillis();
		switch(num){
			case 1:	query1();
			case 2:	query2();
			case 3:	query3();
			case 4:	query4();
		}

		temps_fin = System.currentTimeMillis();
		return temps_fin-temps_ini;
	}

	private void query1(){
		List<Map<String, Object>> resultat = new ArrayList<Map<String, Object>>();
		
		String last_l_returnflag = null;
		String last_l_linestatus = null;
		int i = 0, sum_qty = 0, sum_base_price = 0, sum_disc_price = 0, sum_charge = 0, sum_discount = 0;

		BasicDBObject query = new BasicDBObject();
		query.put("l_shipdate", new BasicDBObject("$lte", qu1_data));
		DBCursor cursor = db.getCollection("lineitem").find(query)
			.sort(new BasicDBObject("l_linestatus", 1))
			.sort(new BasicDBObject("l_returnflag", 1));
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
				Map<String, Object> t = new HashMap<String, Object>();
				t.put("l_returnflag", l_returnflag);
				t.put("l_linestatus", l_linestatus);
				t.put("sum_qty", sum_qty);
				t.put("sum_base_price", sum_base_price);
				t.put("sum_disc_price", sum_disc_price);
				t.put("sum_charge", sum_charge);
				t.put("avg_qty", (sum_qty/i));
				t.put("avg_price", (sum_base_price/i));
				t.put("avg_disc", (sum_discount/i));
				t.put("count_order", i);
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
		if(resultat.size() == 0){
			System.out.println("ALERTA: El resultat de la query 1 no retorna cap resultat!!!");
		}
	}

	private void query2(){
		List<Map<String, Object>> resultat = new ArrayList<Map<String, Object>>();
		
		BasicDBObject query_p = new BasicDBObject();
		query_p.put("p_size", qu2_size);
		query_p.put("p_type", Pattern.compile(qu2_type));
		DBCursor cursor_p = db.getCollection("part").find(query_p);
		while(cursor_p.hasNext()) {
			DBObject ob_p = cursor_p.next();
			Integer p_partkey = (Integer) ob_p.get("_id");
			
			// Fem la subconsulta
			int min_subconsulta = query2Subquery(p_partkey);

			BasicDBObject query_ps = new BasicDBObject();
			query_ps.put("ps_supplycost", min_subconsulta);
//			query_ps.put("ps_partkey", p_partkey);
			DBCursor cursor_ps = db.getCollection("partsupp").find(query_ps);	
			while(cursor_ps.hasNext()) {
				DBObject ob_ps = cursor_ps.next();
				Integer ps_suppkey = (Integer) ob_ps.get("ps_suppkey");
				
				BasicDBObject query_s = new BasicDBObject();
				query_s.put("_id", ps_suppkey);
				DBCursor cursor_s = db.getCollection("supplier").find(query_s);
				while(cursor_s.hasNext()) {
					DBObject ob_s = cursor_s.next();
					Integer s_nationkey = (Integer) ob_s.get("_id");
					
					BasicDBObject query_n = new BasicDBObject();
					query_n.put("_id", s_nationkey);
					DBCursor cursor_n = db.getCollection("nation").find(query_n);
					while(cursor_n.hasNext()) {
						DBObject ob_n = cursor_n.next();
						Integer n_regionkey = (Integer) ob_n.get("_id");
						
						BasicDBObject query_r = new BasicDBObject();
						query_r.put("_id", n_regionkey);
						query_r.put("r_name", qu2_region);
						DBCursor cursor_r = db.getCollection("region").find(query_r);
						while(cursor_r.hasNext()) {
							cursor_r.next();

							Map<String, Object> t = new HashMap<String, Object>();
							t.put("s_acctbal", ob_s.get("s_acctbal"));
							t.put("s_name", ob_s.get("s_name"));
							t.put("n_name", ob_n.get("n_name"));
							t.put("p_partkey", ob_p.get("_id"));
							t.put("p_mfgr", ob_p.get("p_mfgr"));
							t.put("s_address", ob_s.get("s_address"));
							t.put("s_phone", ob_s.get("s_phone"));
							t.put("s_comment", ob_s.get("s_comment"));
							 
							resultat.add(t);
						}
					}
				}
			}
		}

		Collections.sort(resultat, new ComparatorQuery2());
		
		if(resultat.size() == 0){
			System.out.println("ALERTA: El resultat de la query 2 no retorna cap resultat!!!");
		}
	}

	
	private Integer query2Subquery(Integer p_partkey){
		Integer min_subconsulta = Integer.MAX_VALUE;

		BasicDBObject query_ps = new BasicDBObject();
		query_ps.put("ps_suppkey", p_partkey);
		DBCursor cursor_ps = db.getCollection("partsupp").find(query_ps);
		
		while(cursor_ps.hasNext()) {
			DBObject ob_ps = cursor_ps.next();
			Integer ps_suppkey = (Integer) ob_ps.get("ps_suppkey");
			Integer ps_supplycost = (Integer) ob_ps.get("ps_supplycost");
			
			BasicDBObject query_s = new BasicDBObject();
			query_s.put("_id", ps_suppkey);
			DBCursor cursor_s = db.getCollection("supplier").find(query_s);
			while(cursor_s.hasNext()) {
				DBObject ob_s = cursor_s.next();
				Integer s_nationkey = (Integer) ob_s.get("_id");
				
				BasicDBObject query_n = new BasicDBObject();
				query_n.put("_id", s_nationkey);
				DBCursor cursor_n = db.getCollection("nation").find(query_n);
				while(cursor_n.hasNext()) {
					DBObject ob_n = cursor_n.next();
					Integer n_regionkey = (Integer) ob_n.get("_id");
					
					BasicDBObject query_r = new BasicDBObject();
					query_r.put("_id", n_regionkey);
					query_r.put("r_name", qu2_region);
					DBCursor cursor_r = db.getCollection("region").find(query_r);
					while(cursor_r.hasNext()) {
						cursor_r.next();
						if(min_subconsulta > ps_supplycost){
							min_subconsulta = ps_supplycost;
						}
					}
				}
			}
		}
		return min_subconsulta;
	}
	
	private void query3(){
		List<Map<String, Object>> resultat = new ArrayList<Map<String, Object>>();
		
		Integer last_l_orderkey = null;
		Date last_o_orderdate = null;
		Integer last_o_shippriority = null;
		Integer revenue = 0;
		
		BasicDBObject query_l = new BasicDBObject();
		query_l.put("l_shipdate", new BasicDBObject("$gt", qu3_data2));
		DBCursor cursor_l = db.getCollection("lineitem").find(query_l);
		while(cursor_l.hasNext()) {
			DBObject ob_l = cursor_l.next();
			Integer l_orderkey = (Integer) ob_l.get("l_orderkey");
			Integer l_extendedprice = (Integer) ob_l.get("l_extendedprice");
			Integer l_discount = (Integer) ob_l.get("l_discount");
			
			BasicDBObject query_o = new BasicDBObject();
			query_o.put("_id", l_orderkey);
			query_o.put("o_orderdate", new BasicDBObject("$lte", qu3_data1));
			DBCursor cursor_o = db.getCollection("orders").find(query_o);
			while(cursor_o.hasNext()) {
				DBObject ob_o = cursor_o.next();
				Integer o_orderkey = (Integer) ob_o.get("_id");
				Date o_orderdate = (Date) ob_o.get("o_orderdate");
				Integer o_shippriority = (Integer) ob_o.get("o_shippriority");
				
				BasicDBObject query_c = new BasicDBObject();
				query_c.put("_id", o_orderkey);
				query_c.put("c_mktsegment", qu3_segment);
				DBCursor cursor_c = db.getCollection("customer").find(query_c);
				while(cursor_c.hasNext()) {
					cursor_c.next();
					
					revenue += l_extendedprice*(1-l_discount);
					
					// Mirem si pertany al groupby anterior
					boolean next_group = true;
					if(l_orderkey.equals(last_l_orderkey)){
						if(o_orderdate.equals(last_o_orderdate)){
							if(o_shippriority.equals(last_o_shippriority)){
								next_group = false;
							}
						}
					}
					if(next_group){
						// Calculem la tupla
						Map<String, Object> t = new HashMap<String, Object>();
						t.put("l_orderkey", l_orderkey);
						t.put("revenue", revenue);
						t.put("o_orderdate", o_orderdate);
						t.put("o_shippriority", o_shippriority);
						resultat.add(t);

						// Ho deixem bé per la següent iteració
						last_l_orderkey = l_orderkey;
						last_o_orderdate = o_orderdate;
						last_o_shippriority = o_shippriority;
						revenue = 0;
					}
				}
			}
		}

		Collections.sort(resultat, new ComparatorQuery3());

		if(resultat.size() == 0){
			System.out.println("ALERTA: El resultat de la query 3 no retorna cap resultat!!!");
		}
	}	
	
	private void query4(){
		List<Map<String, Object>> resultat = new ArrayList<Map<String, Object>>();
		Integer revenue = 0;
		String last_n_name = null;
		
		BasicDBObject query_c = new BasicDBObject();
		DBCursor cursor_c = db.getCollection("customer").find(query_c);
		while(cursor_c.hasNext()) {
			DBObject ob_c = cursor_c.next();
			Integer c_custkey = (Integer) ob_c.get("_id");
				
			BasicDBObject query_o = new BasicDBObject();
			query_o.put("o_custkey", c_custkey);
			query_o.put("o_orderdate", new BasicDBObject("$gte", qu4_data1));
			query_o.put("o_orderdate", new BasicDBObject("$lt", qu4_data2));
			DBCursor cursor_o = db.getCollection("orders").find(query_o);
			while(cursor_o.hasNext()) {
				DBObject ob_o = cursor_o.next();
				Integer o_orderkey = (Integer) ob_o.get("_id");
				
	
				BasicDBObject query_l = new BasicDBObject();
				query_l.put("l_orderkey", o_orderkey);
				DBCursor cursor_l = db.getCollection("lineitem").find(query_l);
				while(cursor_l.hasNext()) {
					DBObject ob_l = cursor_l.next();
					Integer l_suppkey = (Integer) ob_l.get("l_suppkey");
					Integer l_extendedprice = (Integer) ob_l.get("l_extendedprice");
					Integer l_discount = (Integer) ob_l.get("l_discount");
					
					BasicDBObject query_s = new BasicDBObject();
					query_s.put("_id", l_suppkey);
					DBCursor cursor_s = db.getCollection("supplier").find(query_s);
					while(cursor_s.hasNext()) {
						DBObject ob_s = cursor_s.next();
						Integer s_nationkey = (Integer) ob_s.get("s_nationkey");
						
						BasicDBObject query_n = new BasicDBObject();
						query_n.put("_id", s_nationkey);
						DBCursor cursor_n = db.getCollection("nation").find(query_n);
						while(cursor_n.hasNext()) {
							DBObject ob_n = cursor_n.next();
							Integer n_regionkey = (Integer) ob_n.get("n_regionkey");
							String n_name = (String) ob_n.get("n_name");
							
							BasicDBObject query_r = new BasicDBObject();
							query_r.put("_id", n_regionkey);
							query_r.put("r_name", qu4_region);
							DBCursor cursor_r = db.getCollection("region").find(query_r);
							while(cursor_r.hasNext()) {
								cursor_r.next();
	
								revenue += l_extendedprice * (1 - l_discount);

								// Mirem si pertany al groupby anterior
								boolean next_group = true;
								if(n_name.equals(last_n_name)){
									next_group = false;
								}
								if(next_group){
									Map<String, Object> t = new HashMap<String, Object>();
									t.put("n_name", ob_n.get("n_name"));
									t.put("revenue", revenue);
									 
									resultat.add(t);
									revenue = 0;
								}
							}
						}
					}
				}
			}
		}

		Collections.sort(resultat, new ComparatorQuery4());

		if(resultat.size() == 0){
			System.out.println("ALERTA: El resultat de la query 4 no retorna cap resultat!!!");
		}
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
			
			String r_name = rStr(64/2);
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				r_name = qu2_region;
			}
	
			document.append("_id", i);
			document.append("r_name", r_name);
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
			
			int p_size = rInt(4);
			String p_type = rStr(64/2);
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				p_size = qu2_size;
				p_type = qu2_type;
			}
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("p_name", rStr(64/2));
			document.append("p_mfgr", rStr(64/2));
			document.append("p_brand", rStr(64/2));
			document.append("p_type", p_type);
			document.append("p_size", p_size);
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
			
			String c_mktsegment = rStr(64/2);
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				c_mktsegment = qu3_segment;
			}
			
			document.append("_id", i);
			document.append("c_name", rStr(64/2));
			document.append("c_address", rStr(64/2));
			document.append("c_nationkey", rBetween(start,start+25));
			document.append("c_phone", rStr(18/2));
			document.append("c_acctbal", rInt(13/2));
			document.append("c_mktsegment", c_mktsegment);
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
			
			Date o_orderdate = rDate();
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				o_orderdate = qu3_data1;
			}
			
			document.append("_id", i);
			document.append("o_custkey", rBetween(start,start+499));
			document.append("o_orderstatus", rStr(64/2));
			document.append("o_totalprice", rInt(13/2));
			document.append("o_orderdate", o_orderdate);
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
			
			Date l_shipdate = rDate();
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				l_shipdate = qu3_data2;
			}
			
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
			document.append("l_shipdate", l_shipdate);
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
	
	/* --------------------- Comparadors ----------------------------  */
	private class ComparatorQuery2 implements Comparator<Map<String, Object>>{
		@Override
		public int compare(Map<String, Object> a, Map<String, Object> b) {
			String sa, sb;
			Integer ia, ib;

			ia = (Integer) a.get("p_partkey");
			ib = (Integer) b.get("p_partkey");
			if(ia.compareTo(ib) != 0)	return ia.compareTo(ib);

			sa = (String) a.get("s_name");
			sb = (String) b.get("s_name");
			if(sa.compareTo(sb) != 0)	return sa.compareTo(sb);
			
			sa = (String) a.get("n_name");
			sb = (String) b.get("n_name");
			if(sa.compareTo(sb) != 0)	return sa.compareTo(sb);
			
			sa = (String) a.get("s_acctbal");
			sb = (String) b.get("s_acctbal");
			return -1 * sa.compareTo(sb);
		}
	}
	
	private class ComparatorQuery3 implements Comparator<Map<String, Object>>{
		@Override
		public int compare(Map<String, Object> a, Map<String, Object> b) {
			Date da, db;
			Integer ia, ib;

			da = (Date) a.get("o_orderdate");
			db = (Date) b.get("o_orderdate");
			if(da.compareTo(db) != 0)	return da.compareTo(db);

			ia = (Integer) a.get("revenue");
			ib = (Integer) b.get("revenue");
			return -1*ia.compareTo(ib);
		}
	}
	
	private class ComparatorQuery4 implements Comparator<Map<String, Object>>{
		@Override
		public int compare(Map<String, Object> a, Map<String, Object> b) {
			Integer ia, ib;

			ia = (Integer) a.get("revenue");
			ib = (Integer) b.get("revenue");
			return -1*ia.compareTo(ib);
		}
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