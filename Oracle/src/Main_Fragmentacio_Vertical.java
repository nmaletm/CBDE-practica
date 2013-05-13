import java.sql.*;
import java.util.*;
import java.lang.Math.*;

public class Main_Fragmentacio_Vertical {

	/*
	S'han de posar valors que existeixin a la BD
	*/
	private static String qu1_data = "19/03/30";

	private static String qu2_size = "4841";
	private static String qu2_type = "YYLMXRZVOXWMCWKWHFVAAXMPWGQQKEVW";
	private static String qu2_region = "AJOOFVSLSUPXECQTVPKEHPHHQNXZAOFM";
	
	private static String qu3_segment = "YQLDAEXZVJWKSYJCMPJRXQLEBNCZBLBK";
	private static String qu3_data1 = "18/07/29";
	private static String qu3_data2 = "24/09/27";

	private static String qu4_region = "AJOOFVSLSUPXECQTVPKEHPHHQNXZAOFM";
	private static String qu4_data1 = "10/01/18";
	private static String qu4_data2 = "10/01/19";
	
	private static Connection connection;
	private static Random rnd = new Random();

	private static List<Tuple> cacheClausPartSup = new ArrayList<Tuple>();
	private static List<Integer> cacheClausPartkey = new ArrayList<Integer>();
	private static List<Integer> cacheClausSuppkey = new ArrayList<Integer>();
	
	private static String QUERY1 = "SELECT L2.l_returnflag, L2.l_linestatus, sum(L2.l_quantity) as sum_qty, sum(L1.l_extendedprice) as sum_base_price, sum(L1.l_extendedprice*(1-L1.l_discount)) as sum_disc_price, sum(L1.l_extendedprice*(1-L1.l_discount)*(1+L2.l_tax)) as sum_charge, avg(L2.l_quantity) as avg_qty, avg(L1.l_extendedprice) as avg_price, avg(L1.l_discount) as avg_disc, count(*) as count_order FROM lineitem L, TABLE(L.part1) L1, TABLE(L.part2) L2 WHERE L1.l_shipdate <= '"+qu1_data+"' GROUP BY L2.l_returnflag, L2.l_linestatus ORDER BY L2.l_returnflag, L2.l_linestatus";
	private static String QUERY2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = '"+qu2_size+"' AND p_type like '%"+qu2_type+"' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu2_region+"' AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu2_region+"') ORDER BY s_acctbal desc, n_name, s_name, p_partkey";
	private static String QUERY3 = "SELECT L1.l_orderkey, sum(L1.l_extendedprice*(1-L1.l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem L, TABLE(L.part1) L1 WHERE c_mktsegment = '"+qu3_segment+"' AND c_custkey = o_custkey AND L1.l_orderkey = o_orderkey AND o_orderdate < '"+qu3_data1+"' AND L1.l_shipdate > '"+qu3_data2+"' GROUP BY L1.l_orderkey, o_orderdate, o_shippriority ORDER BY revenue desc, o_orderdate";
	private static String QUERY4 = "SELECT n_name, sum(L1.l_extendedprice * (1 - L1.l_discount)) as revenue FROM customer, orders, lineitem L, TABLE(L.part1) L1, supplier, nation, region WHERE c_custkey = o_custkey AND L1.l_orderkey = o_orderkey AND L1.l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu4_region+"' AND o_orderdate >= '"+qu4_data1+"' AND o_orderdate < '"+qu4_data2+"' GROUP BY n_name ORDER BY revenue desc";
	
	
	private static class Tuple{
		public int a;
		public int b;
		
		public Tuple(int a, int b){
			this.a = a;
			this.b = b;
		}
	}
	
	public static void main(String[] argv) {
		long temps_ini, temps_fin; 
		temps_ini = System.currentTimeMillis();
		
 		try {
			boolean nestor = true;
			if(nestor){
				System.out.println("Estem treballant amb la BD de Nestor");
				connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@oraclefib.fib.upc.es:1521:ORABD", "nestor.malet",
					"DB310791");
			}
			else{
				System.out.println("Estem treballant amb la BD de Mireia");
				connection = DriverManager.getConnection(
					"jdbc:oracle:thin:@oraclefib.fib.upc.es:1521:ORABD", "mireia.rodriguez",
					"DB070390");
			}
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
 
		try{
			connection.setAutoCommit(false);
			//insertBatchData(1000);
			//insertBatchData(6000);
			
			long temps = 0;
			temps += queries(1,QUERY1);
			temps += queries(2,QUERY2);
			temps += queries(3,QUERY3);
			temps += queries(4,QUERY4);
			System.out.println("Avg de temps " + temps + "/4 mili ");

			connection.commit();
		}
		catch (Exception e) {
			if (connection != null) {
				try{
					connection.rollback();
				}catch (Exception e2) {};
			}
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try{
					connection.close();
				}catch (Exception e2) {};
			}
		}

		
		temps_fin = System.currentTimeMillis();
		System.out.println("El total ha trigat " + (temps_fin-temps_ini) + " mili ");
		
	}
	
	/*  ---------------------- Queries ------------------------------  */
	
	private static long queries(int num, String sql) throws SQLException{
		long temps = 9999999;
		for(int i = 0; i < 5; i++){
			long t = executeQuery(num, sql);
			if(t < temps && t != 0){
				temps = t;
			}
		}
		System.out.println("\nQuery "+num+" trigat " + temps + " mili ");
		return temps;
	}
	
	
	private static long executeQuery(int num, String sql) throws SQLException{
		long temps_ini = 0, temps_fin = 0; 
		boolean teResultat = false;
		Statement statement = connection.createStatement();
		
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
		return temps_fin-temps_ini;
	}
	
	/*  ---------------------- Inserts ------------------------------  */
	
	private static void insertBatchData(int start) throws SQLException{
		System.out.println("------------- INSERTS ---------------");

		List<PreparedStatement> llista = new ArrayList<PreparedStatement>();
		System.out.println("Preparem valors inserts");
		llista.add(insertsRegion(start));
		llista.add(insertsNation(start));
		llista.add(insertsPart(start));
		llista.add(insertsSupplier(start));
		llista.add(insertsCustomer(start));
		llista.add(insertsPartSupp(start));
		llista.add(insertsOrders(start));
		llista.add(insertsLineItem(start));
		System.out.println("Valors preparats, ara anem a fer insert");

		long temps_ini, temps_fin; 
		temps_ini = System.currentTimeMillis();
		for(PreparedStatement t : llista){
			t.executeBatch();
		}
		temps_fin = System.currentTimeMillis();
		System.out.println("S'ha trigat a fer els inserts: " + (temps_fin-temps_ini) + " mili ");
		
	}
	
	private static PreparedStatement insertsRegion(int start) throws SQLException{
		String selectSQL = "INSERT INTO region VALUES (?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 5+start; i++){
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, rStr(64/2));
			preparedStatement.setString(3, rStr(160/2));
			preparedStatement.setString(4, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	private static PreparedStatement insertsNation(int start) throws SQLException{
		String selectSQL = "INSERT INTO nation VALUES (?,?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 25+start; i++){
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, rStr(64/2));
			preparedStatement.setInt(3, rBetween(start,start+5));
			preparedStatement.setString(4, rStr(160/2));
			preparedStatement.setString(5, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	private static PreparedStatement insertsPart(int start) throws SQLException{
		Statement statement = connection.createStatement();
		String selectSQL = "INSERT INTO part VALUES (?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 666+start; i++){
			cacheClausPartkey.add(i);
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, rStr(64/2));
			preparedStatement.setString(3, rStr(64/2));
			preparedStatement.setString(4, rStr(64/2));
			preparedStatement.setString(5, rStr(64/2));
			preparedStatement.setInt(6, rInt(4));
			preparedStatement.setString(7, rStr(64/2));
			preparedStatement.setInt(8, rInt(4));
			preparedStatement.setString(9, rStr(64/2));
			preparedStatement.setString(10, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	private static PreparedStatement insertsSupplier(int start) throws SQLException{
		Statement statement = connection.createStatement();
		String selectSQL = "INSERT INTO supplier VALUES (?,?,?,?,?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 33+start; i++){
			cacheClausSuppkey.add(i);
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, rStr(64/2));
			preparedStatement.setString(3, rStr(64/2));
			preparedStatement.setInt(4, rBetween(start,start+25));
			preparedStatement.setString(5, rStr(18/2));
			preparedStatement.setInt(6, rInt(13/2));
			preparedStatement.setString(7, rStr(105/2));
			preparedStatement.setString(8, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	private static PreparedStatement insertsCustomer(int start) throws SQLException{
		Statement statement = connection.createStatement();
		String selectSQL = "INSERT INTO customer VALUES (?,?,?,?,?,?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 499+start; i++){
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, rStr(64/2));
			preparedStatement.setString(3, rStr(64/2));
			preparedStatement.setInt(4, rBetween(start,start+25));
			preparedStatement.setString(5, rStr(64/2));
			preparedStatement.setInt(6, rInt(13/2));
			preparedStatement.setString(7, rStr(64/2));
			preparedStatement.setString(8, rStr(102/2));
			preparedStatement.setString(9, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	private static PreparedStatement insertsPartSupp(int start) throws SQLException{
		Statement statement = connection.createStatement();
		String selectSQL = "INSERT INTO partsupp VALUES (?,?,?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 2666+start; i++){
			int partkey = cacheClausPartkey.get(rBetween(0, cacheClausPartkey.size()));
			int suppkey = cacheClausSuppkey.get(rBetween(0, cacheClausSuppkey.size()));
			Tuple t = new Tuple(partkey, suppkey);
			cacheClausPartSup.add(t);
			
			preparedStatement.setInt(1, partkey);
			preparedStatement.setInt(2, suppkey);
			preparedStatement.setInt(3, rInt(4));
			preparedStatement.setInt(4, rInt(13/2));
			preparedStatement.setString(5, rStr(200/2));
			preparedStatement.setString(6, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
		
	private static PreparedStatement insertsOrders(int start) throws SQLException{
		Statement statement = connection.createStatement();
		String selectSQL = "INSERT INTO orders VALUES (?,?,?,?,"+rDate()+",?,?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 4999+start; i++){
		
			preparedStatement.setInt(1, i);
			preparedStatement.setInt(2, rBetween(start,start+499));
			preparedStatement.setString(3, rStr(64/2));
			preparedStatement.setInt(4, rInt(13/2));
			// Data generada amb SQL
			preparedStatement.setString(5, rStr(15/2));
			preparedStatement.setString(6, rStr(64/2));
			preparedStatement.setInt(7, rInt(4));
			preparedStatement.setString(8, rStr(80/2));
			preparedStatement.setString(9, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}



   
	private static PreparedStatement insertsLineItem(int start) throws SQLException{
		Statement statement = connection.createStatement();
		String selectSQL = "INSERT INTO lineitem VALUES( "+
			"   lineitems1_T(lineitems1(?, ?, ?, ?, "+rDate()+")), "+
			"   lineitems2_T(lineitems2(?, ?, ?, ?)), "+
			"   lineitems3_T(lineitems3(?, ?, "+rDate()+", "+rDate()+", ?, ?, ?, ?)) "+
			")";
		System.out.println(selectSQL);
		
		
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int j = start; j < 1999+start; j++){
			Tuple t = cacheClausPartSup.get(rBetween(0, cacheClausPartSup.size()));
			
			int i = 1;
			preparedStatement.setInt(i++, rBetween(start,start+4999));
			preparedStatement.setInt(i++, t.b);
			preparedStatement.setInt(i++, rInt(13/2));
			preparedStatement.setInt(i++, rInt(13/2));
			// Data generada amb SQL
			
			preparedStatement.setInt(i++, rInt(4));
			preparedStatement.setInt(i++, rInt(13/2));
			preparedStatement.setString(i++, rStr(64/2));
			preparedStatement.setString(i++, rStr(64/2));
			
			preparedStatement.setInt(i++, t.a);
			preparedStatement.setInt(i++, rInt(4));
			// Data generada amb SQL
			// Data generada amb SQL
			preparedStatement.setString(i++, rStr(64/2));
			preparedStatement.setString(i++, rStr(64/2));
			preparedStatement.setString(i++, rStr(64/2));
			preparedStatement.setString(i++, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	
	/*  ---------------------- Randoms ------------------------------  */

	
	private static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final String NUMB = "0123456789";
	
	public static int rBetween(int min, int max){
		return min+(int)(Math.random() * ( max - min ));
	}

	public static String rStr(int len){
		StringBuilder sb = new StringBuilder( len );
		for( int i = 0; i < len; i++ ) 
			sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
		return sb.toString();
	}

	public static int rInt(int len){
		StringBuilder sb = new StringBuilder( len );
		for( int i = 0; i < len; i++ ) 
			sb.append( NUMB.charAt( rnd.nextInt(NUMB.length()) ) );
		return Integer.valueOf(sb.toString());
	}
	
	public static String rDate(){
		return " DBMS_RANDOM.VALUE*9999 + SYSDATE ";
	}
/*

SELECT p_size, p_type, r_name FROM part, supplier, partsupp, nation, region WHERE 
  rownum = 1 AND
  p_partkey = ps_partkey AND 
  s_suppkey = ps_suppkey AND 
  s_nationkey = n_nationkey AND 
  n_regionkey = r_regionkey AND 
  ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE 
                    p_partkey = ps_partkey AND 
                    s_suppkey = ps_suppkey AND 
                    s_nationkey = n_nationkey AND 
                    n_regionkey = r_regionkey  
                    ) ORDER BY s_acctbal desc, n_name, s_name, p_partkey;
					
SELECT c_mktsegment, o_orderdate+1, L1.l_shipdate-1 FROM customer, orders, lineitem L, TABLE(L.part1) L1
  WHERE rownum = 1 and
  c_custkey = o_custkey AND 
  L1.l_orderkey = o_orderkey ;
  
SELECT r_name, o_orderdate
	FROM customer, orders, lineitem L, TABLE(L.part1) L1, supplier, nation, region WHERE c_custkey = o_custkey 
  and rownum = 1
	AND L1.l_orderkey = o_orderkey AND L1.l_suppkey = s_suppkey AND c_nationkey = s_nationkey 
	AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey ;
*/
}




