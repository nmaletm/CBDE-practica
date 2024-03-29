import java.net.UnknownHostException;
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

public class MainTuned {

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

	private String SEPARADOR_KEY = "@@@";
	private DB db;
	private Random rnd = new Random();

	private List<Object> cacheClausPartSup = new ArrayList<Object>();
	private Map<Object,Tuple> cacheTuplesPartSup = new HashMap<Object,Tuple>();
	private List<Object> cacheClausPartkey = new ArrayList<Object>();
	private List<Object> cacheClausSuppkey = new ArrayList<Object>();

	private Map<Object,BasicDBObject> dataRegion;
	private Map<Object,BasicDBObject> dataNation;
	private Map<Object,BasicDBObject> dataPart;
	private Map<Object,BasicDBObject> dataSupplier;
	private Map<Object,BasicDBObject> dataPartSupp;
	private Map<Object,BasicDBObject> dataCustomer;
	private Map<Object,BasicDBObject> dataOrders;
	private Map<Object,BasicDBObject> dataLineItem;


	private List<String> taules;


	public MainTuned(){
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
		MainTuned m = new MainTuned();
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
			ensureIndexs();
			
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
			case 1:	query1();break;
			case 2:	query2();break;
			case 3:	query3();break;
			case 4:	query4();break;
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

				// Ho deixem b� per la seg�ent iteraci�
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
			
			DBCursor cursor_ps = db.getCollection("partsupp").find(query_ps);	
			while(cursor_ps.hasNext()) {
				DBObject ob_ps = cursor_ps.next();
				Integer ps_suppkey = (Integer) ob_ps.get("ps_suppkey");

				BasicDBObject query_s = new BasicDBObject();
				query_s.put("_id", ps_suppkey);
				DBCursor cursor_s = db.getCollection("supplier").find(query_s);
				while(cursor_s.hasNext()) {
					DBObject ob_s = cursor_s.next();
					BasicDBObject n_nation = (BasicDBObject) ob_s.get("s_nation");
					BasicDBObject r_region = (BasicDBObject) n_nation.get("n_region");

					if(r_region.getString("r_name").equals(qu2_region)){
						Map<String, Object> t = new HashMap<String, Object>();
						t.put("s_acctbal", ob_s.get("s_acctbal"));
						t.put("s_name", ob_s.get("s_name"));
						t.put("n_name", n_nation.get("n_name"));
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
				BasicDBObject n_nation = (BasicDBObject) ob_s.get("s_nation");
				BasicDBObject r_region = (BasicDBObject) n_nation.get("n_region");
				
				if(r_region.getString("r_name").equals(qu2_region)) {
					if(min_subconsulta > ps_supplycost){
						min_subconsulta = ps_supplycost;
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
			BasicDBObject o_order = (BasicDBObject) ob_l.get("l_order");

			Date o_orderdate = (Date) o_order.get("o_orderdate");
			Integer o_shippriority = (Integer) o_order.get("o_shippriority");

			if(o_orderdate.before(qu3_data1)){
				BasicDBObject c_cust = (BasicDBObject) o_order.get("o_cust");

				if(c_cust.getString("c_mktsegment").equals(qu3_segment)){
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

						// Ho deixem b� per la seg�ent iteraci�
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
			
			// Per a que utilitzi el index, li hem de dir que nom�s volem els atributs necessaris
			BasicDBObject values_o = new BasicDBObject();
			values_o.put("_id", 1);
			
			DBCursor cursor_o = db.getCollection("orders").find(query_o, values_o);
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
						BasicDBObject n_nation = (BasicDBObject) ob_s.get("s_nation");

						BasicDBObject r_region = (BasicDBObject) n_nation.get("n_region");
						String n_name = (String) n_nation.get("n_name");

						if(r_region.getString("r_name").equals(qu4_region)){
							revenue += l_extendedprice * (1 - l_discount);

							// Mirem si pertany al groupby anterior
							boolean next_group = true;
							if(n_name.equals(last_n_name)){
								next_group = false;
							}
							if(next_group){
								Map<String, Object> t = new HashMap<String, Object>();
								t.put("n_name", n_name);
								t.put("revenue", revenue);

								resultat.add(t);
								revenue = 0;
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

	private void insertBatchData(int start){
		System.out.println("------------- INSERTS ---------------");

		dataRegion = generateRegion(start);
		dataNation = generateNation(start);
		dataPart = generatePart(start);
		dataSupplier = generateSupplier(start);
		dataCustomer = generateCustomer(start);
		dataPartSupp = generatePartSupp(start);
		dataOrders = generateOrders(start);
		dataLineItem = generateLineItem(start);

		materializeData();
		Map<String,Map<Object,BasicDBObject>> llista = new HashMap<String,Map<Object,BasicDBObject>>();

		System.out.println("Preparem valors inserts");
		llista.put("part", dataPart);
		llista.put("supplier", dataSupplier);
		llista.put("customer", dataCustomer);
		llista.put("partsupp", dataPartSupp);
		llista.put("orders", dataOrders);
		llista.put("lineitem", dataLineItem);

		
		System.out.println("Valors preparats, ara anem a fer insert");

		long temps_ini, temps_fin; 
		temps_ini = System.currentTimeMillis();
		for (Map.Entry<String, Map<Object,BasicDBObject>> entry : llista.entrySet()) {
			String name_collection = entry.getKey();
			List<BasicDBObject> values = new ArrayList<BasicDBObject>(entry.getValue().values());
			DBCollection col = db.getCollection(name_collection);
			for(BasicDBObject ob : values){
				col.save(ob);
			}
		}
		temps_fin = System.currentTimeMillis();
		System.out.println("S'ha trigat a fer els inserts: " + (temps_fin-temps_ini) + " mili ");

	}

// System.out.println(cursor_o.explain());
	
	private void ensureIndexs(){
		BasicDBObject obj;
		
		obj = new BasicDBObject();
		obj.put("p_size", 1);
		obj.put("p_type", 1);
		db.getCollection("part").ensureIndex(obj, new BasicDBObject("name", "part_index"));
		//db.getCollection("part").dropIndex("part_index");
		
		db.getCollection("partsupp").ensureIndex(new BasicDBObject("ps_supplycost", 1), new BasicDBObject("name", "partsupp_index"));
		//db.getCollection("partsupp").dropIndex("partsupp_index");
		
		obj = new BasicDBObject();
		obj.put("o_custkey", 1);
		obj.put("o_orderdate", 1);
		obj.put("_id", 1);
		db.getCollection("orders").ensureIndex(obj, new BasicDBObject("name", "orders_index"));
		//db.getCollection("orders").dropIndex("orders_index");
		
		db.getCollection("lineitem").ensureIndex(new BasicDBObject("l_shipdate", 1), new BasicDBObject("name", "lineitem_index"));
		//db.getCollection("lineitem").dropIndex("lineitem_index");
		
		db.getCollection("lineitem").ensureIndex(new BasicDBObject("l_orderkey", 1), new BasicDBObject("name", "lineitem_index2"));
		//db.getCollection("lineitem").dropIndex("lineitem_index2");
		
	}

	private void materializeData(){

		// Materialitzem region a dintre de nation:
		for(Object nationKey : dataNation.keySet()){
			BasicDBObject nation = dataNation.get(nationKey);
			nation.put("n_region", dataRegion.get(nation.get("n_regionkey")));
		}
		// Materialitzem nation a dintre de supplier:
		for(Object suppkey : dataSupplier.keySet()){
			BasicDBObject supplier = dataSupplier.get(suppkey);
			supplier.put("s_nation", dataNation.get(supplier.get("s_nationkey")));
		}


		// Materialitzem customer a dintre de order:
		for(Object orderkey : dataOrders.keySet()){
			BasicDBObject order = dataOrders.get(orderkey);
			order.put("o_cust", dataCustomer.get(order.get("o_custkey")));
		}
		// Materialitzem order a dintre de lineitem:
		for(Object lineitemkey : dataLineItem.keySet()){
			BasicDBObject lineitem = dataLineItem.get(lineitemkey);
			lineitem.put("l_order", dataOrders.get(lineitem.get("l_orderkey")));
		}

	}

	private Map<Object,BasicDBObject> generateRegion(int start){
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
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
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,BasicDBObject> generateNation(int start){
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
		for(int i = start; i < 25+start; i++){
			BasicDBObject document = new BasicDBObject();
			document.append("_id", i);
			document.append("n_name", rStr(64/2));
			document.append("n_regionkey", rBetween(start,start+5));
			document.append("n_comment", rStr(160/2));
			document.append("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,BasicDBObject> generatePart(int start){
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
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
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,BasicDBObject> generateSupplier(int start){
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
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
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,BasicDBObject> generateCustomer(int start){
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
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
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,BasicDBObject> generatePartSupp(int start) {
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
		for(int i = start; i < 2666+start; i++){
			Object partkey = cacheClausPartkey.get(rBetween(0, cacheClausPartkey.size()));
			Object suppkey = cacheClausSuppkey.get(rBetween(0, cacheClausSuppkey.size()));
			Tuple t = new Tuple(partkey, suppkey);
			String id = ""+partkey+SEPARADOR_KEY+suppkey;
			cacheClausPartSup.add(id);
			cacheTuplesPartSup.put(id, t);

			BasicDBObject document = new BasicDBObject();
			document.append("_id",id);
			document.append("ps_partkey", partkey);
			document.append("ps_suppkey", suppkey);
			document.append("ps_availqty", rInt(4));
			document.append("ps_supplycost", rInt(13/2));
			document.append("ps_comment", rStr(200/2));
			document.append("skip", rStr(64/2));
			res.put(id,document);
		}
		return res;
	}

	private Map<Object,BasicDBObject> generateOrders(int start){
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
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
			res.put(i,document);
		}
		return res;
	}


	private Map<Object,BasicDBObject> generateLineItem(int start) {
		Map<Object,BasicDBObject> res = new HashMap<Object,BasicDBObject>();
		for(int i = start; i < 19999+start; i++){
			Object idPartSupp = cacheClausPartSup.get(rBetween(0, cacheClausPartSup.size()));
			BasicDBObject document = new BasicDBObject();

			Date l_shipdate = rDate();
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				l_shipdate = qu3_data2;
			}
			Tuple t = cacheTuplesPartSup.get(idPartSupp);

			Integer l_orderkey = rBetween(start,start+4999);

			Object id = ""+l_orderkey+SEPARADOR_KEY+idPartSupp;

			document.append("l_orderkey", l_orderkey);
			document.append("l_idpartkeysuppkey", idPartSupp);
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
			res.put(id, document);
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

	private static class Tuple{
		public Object a;
		public Object b;

		public Tuple(Object a, Object b){
			this.a = a;
			this.b = b;
		}
	}
}