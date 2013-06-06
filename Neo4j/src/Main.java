import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;


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

	private String SEPARADOR_KEY = "@@@";
	private GraphDatabaseService db;
	private Random rnd = new Random();
	
	private List<Object> cacheClausPartSup = new ArrayList<Object>();
	private Map<Object,Tuple> cacheTuplesPartSup = new HashMap<Object,Tuple>();
	private List<Object> cacheClausPartkey = new ArrayList<Object>();
	private List<Object> cacheClausSuppkey = new ArrayList<Object>();

	private Map<Object,Node> dataRegion;
	private Map<Object,Node> dataNation;
	private Map<Object,Node> dataPart;
	private Map<Object,Node> dataSupplier;
	private Map<Object,Node> dataPartSupp;
	private Map<Object,Node> dataCustomer;
	private Map<Object,Node> dataOrders;
	private Map<Object,Node> dataLineItem;

	private List<String> taules;

	
	private static enum RelTypes implements RelationshipType{
	    KNOWS
	}
	
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
	
	public static void main(String[] args) {
		Main main = new Main();
		main.run();
	}
	
	private void run(){
		initNeo4j();
		
		Transaction tx = db.beginTx();
		try{

			insertBatchData(1000);
			
			
			//runQueries();

			insertBatchData(6000);

		    tx.success();
		}
		finally{
		    tx.finish();
		}
		

		stopNeo4j();
	}
	/*
	private void test(){
		Node firstNode;
		Node secondNode;
		Relationship relationship;
		
		firstNode = db.createNode();
		firstNode.setProperty( "message", "Hello, " );
		secondNode = db.createNode();
		secondNode.setProperty( "message", "World!" );
		 
		relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
		relationship.setProperty( "message", "brave Neo4j " );
		
		System.out.print( firstNode.getProperty( "message" ) );
		System.out.print( relationship.getProperty( "message" ) );
		System.out.print( secondNode.getProperty( "message" ) );
		
		firstNode.getSingleRelationship( RelTypes.KNOWS, Direction.OUTGOING ).delete();
		firstNode.delete();
		secondNode.delete();
	}
	*/
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

//TODO		materializeData();
		Map<String,Map<Object,Node>> llista = new HashMap<String,Map<Object,Node>>();

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
		for (Map.Entry<String, Map<Object,Node>> entry : llista.entrySet()) {
			String name_collection = entry.getKey();
			entry.getValue().values();
		}
		temps_fin = System.currentTimeMillis();
		System.out.println("S'ha trigat a fer els inserts: " + (temps_fin-temps_ini) + " mili ");

	}
	
	private Map<Object,Node> generateRegion(int start){
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 5+start; i++){
			Node document = db.createNode();

			String r_name = rStr(64/2);
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				r_name = qu2_region;
			}

			document.setProperty("_id", i);
			document.setProperty("r_name", r_name);
			document.setProperty("r_comment", rStr(160/2));
			document.setProperty("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,Node> generateNation(int start){
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 25+start; i++){
			Node document = db.createNode();
			document.setProperty("_id", i);
			document.setProperty("n_name", rStr(64/2));
			document.setProperty("n_regionkey", rBetween(start,start+5));
			document.setProperty("n_comment", rStr(160/2));
			document.setProperty("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,Node> generatePart(int start){
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 666+start; i++){
			cacheClausPartkey.add(i);

			int p_size = rInt(4);
			String p_type = rStr(64/2);
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				p_size = qu2_size;
				p_type = qu2_type;
			}
			Node document = db.createNode();
			document.setProperty("_id", i);
			document.setProperty("p_name", rStr(64/2));
			document.setProperty("p_mfgr", rStr(64/2));
			document.setProperty("p_brand", rStr(64/2));
			document.setProperty("p_type", p_type);
			document.setProperty("p_size", p_size);
			document.setProperty("p_container", rStr(64/2));
			document.setProperty("p_retailprice", rInt(4));
			document.setProperty("p_comment", rStr(64/2));
			document.setProperty("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,Node> generateSupplier(int start){
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 33+start; i++){
			cacheClausSuppkey.add(i);
			Node document = db.createNode();
			document.setProperty("_id", i);
			document.setProperty("s_name", rStr(64/2));
			document.setProperty("s_address", rStr(64/2));
			document.setProperty("s_nationkey", rBetween(start,start+25));
			document.setProperty("s_phone", rStr(18/2));
			document.setProperty("s_acctbal", rInt(13/2));
			document.setProperty("s_comment", rStr(105/2));
			document.setProperty("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,Node> generateCustomer(int start){
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 499+start; i++){
			Node document = db.createNode();

			String c_mktsegment = rStr(64/2);
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				c_mktsegment = qu3_segment;
			}

			document.setProperty("_id", i);
			document.setProperty("c_name", rStr(64/2));
			document.setProperty("c_address", rStr(64/2));
			document.setProperty("c_nationkey", rBetween(start,start+25));
			document.setProperty("c_phone", rStr(18/2));
			document.setProperty("c_acctbal", rInt(13/2));
			document.setProperty("c_mktsegment", c_mktsegment);
			document.setProperty("c_comment", rStr(102/2));
			document.setProperty("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}

	private Map<Object,Node> generatePartSupp(int start) {
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 2666+start; i++){
			Object partkey = cacheClausPartkey.get(rBetween(0, cacheClausPartkey.size()));
			Object suppkey = cacheClausSuppkey.get(rBetween(0, cacheClausSuppkey.size()));
			Tuple t = new Tuple(partkey, suppkey);
			String id = ""+partkey+SEPARADOR_KEY+suppkey;
			cacheClausPartSup.add(id);
			cacheTuplesPartSup.put(id, t);

			Node document = db.createNode();
			document.setProperty("_id",id);
			document.setProperty("ps_partkey", partkey);
			document.setProperty("ps_suppkey", suppkey);
			document.setProperty("ps_availqty", rInt(4));
			document.setProperty("ps_supplycost", rInt(13/2));
			document.setProperty("ps_comment", rStr(200/2));
			document.setProperty("skip", rStr(64/2));
			res.put(id,document);
		}
		return res;
	}

	private Map<Object,Node> generateOrders(int start){
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 4999+start; i++){			
			Node document = db.createNode();

			Date o_orderdate = rDate();
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				o_orderdate = qu3_data1;
			}

			document.setProperty("_id", i);
			document.setProperty("o_custkey", rBetween(start,start+499));
			document.setProperty("o_orderstatus", rStr(64/2));
			document.setProperty("o_totalprice", rInt(13/2));
			document.setProperty("o_orderdate", o_orderdate.getTime());
			document.setProperty("o_orderpriority", rStr(15/2));
			document.setProperty("o_clerk", rStr(64/2));
			document.setProperty("o_shippriority", rInt(4));
			document.setProperty("o_comment", rStr(80/2));
			document.setProperty("skip", rStr(64/2));
			res.put(i,document);
		}
		return res;
	}


	private Map<Object,Node> generateLineItem(int start) {
		Map<Object,Node> res = new HashMap<Object,Node>();
		for(int i = start; i < 19999+start; i++){
			Object idPartSupp = cacheClausPartSup.get(rBetween(0, cacheClausPartSup.size()));
			Node document = db.createNode();

			Date l_shipdate = rDate();
			// Falsejem un 10% dels inserts per assegurar les queries
			if (i%10 == 0) {
				l_shipdate = qu3_data2;
			}
			Tuple t = cacheTuplesPartSup.get(idPartSupp);

			Integer l_orderkey = rBetween(start,start+4999);

			Object id = ""+l_orderkey+SEPARADOR_KEY+idPartSupp;

			document.setProperty("l_orderkey", l_orderkey);
			document.setProperty("l_idpartkeysuppkey", idPartSupp);
			document.setProperty("l_partkey", t.a);
			document.setProperty("l_suppkey", t.b);
			document.setProperty("l_linenumber", rInt(4));
			document.setProperty("l_quantity", rInt(4));
			document.setProperty("l_extendedprice", rInt(13/2));
			document.setProperty("l_discount", rInt(13/2));
			document.setProperty("l_tax", rInt(13/2));
			document.setProperty("l_returnflag", rStr(64/2));
			document.setProperty("l_linestatus", rStr(64/2));
			document.setProperty("l_shipdate", l_shipdate.getTime());
			document.setProperty("l_commitdate", rDate().getTime());
			document.setProperty("l_receiptdate", rDate().getTime());
			document.setProperty("l_shipinstruct", rStr(64/2));
			document.setProperty("l_shipmode", rStr(64/2));
			document.setProperty("l_comment", rStr(64/2));
			document.setProperty("skip", rStr(64/2));
			res.put(id, document);
		}
		return res;
	}

	/*  ---------------------- Start and stop ------------------------------  */

	private void initNeo4j(){
		
		Map<String, String> config = new HashMap<String, String>();
		config.put( "neostore.nodestore.db.mapped_memory", "10M" );
		config.put( "string_block_size", "60" );
		config.put( "array_block_size", "300" );
		db = new GraphDatabaseFactory()
		    .newEmbeddedDatabaseBuilder( "target/database/location" )
		    .setConfig( config )
		    .newGraphDatabase();
		
		//db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		
	}
	private void stopNeo4j() {
		db.shutdown();
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
