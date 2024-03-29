import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {

	/* Variables de les queries, tots es calculen automaticament amb updateValues() 
		excepte qu1_data que serveix per a qualsevol combinaci� d'inserts*/
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
	
	private Connection connection;
	private Random rnd = new Random();

	private List<Tuple> cacheClausPartSup = new ArrayList<Tuple>();
	private List<Integer> cacheClausPartkey = new ArrayList<Integer>();
	private List<Integer> cacheClausSuppkey = new ArrayList<Integer>();
	
	private String QUERY1;
	private String QUERY2;
	private String QUERY3;
	private String QUERY4;
	

	/*
		Per a poder executar el main amb dos comptes d'oracle diferents
		hem fet que si es passa l'argument nestor o mireia al main per
		determinar quina utiltizar.
	*/
	public static void main(String[] argv) {
		boolean nestor = false;
		if(argv.length > 0 && "nestor".equals(argv[0])){
			nestor = true;
		}
		Main m = new Main();
		m.run(nestor);
	}
	
	private void run(boolean nestor){
 		try {
			
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
			
			// Primer esborrem tots els valors de la bd
			deleteValues();
			
			// Fem el primer lot d'inserts
			//  1000 per garantir que els enters tinguin 4 digits
			insertBatchData(1000);
			
			// Abans refrescavem les vistes en aquest punt
			//refeshViews();
			
			// Actualitzem els valors de les queries per a que retornin algun valor
			updateValues();
			
			// Executem les queries
			runQueries();
			
			// Fem el segon lot d'inserts
			//  6000 ja que aix� garantim que els enters tinguin 4 digits i alhora
			//  garantim la clau primaria manualment
			insertBatchData(6000);
			
			// Abans refrescavem les vistes en aquest punt
			//refeshViews();
			
			// Executem les queries
			runQueries();

			connection.commit();
			
			printQueries(nestor);
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
		
	}
		
	/*  ---------------------- Queries ------------------------------  */
	private void runQueries() throws SQLException{
		System.out.println("------------- QUERIES ---------------");
		long temps = 0;
		temps += exeQuery(1,QUERY1);
		temps += exeQuery(2,QUERY2);
		temps += exeQuery(3,QUERY3);
		temps += exeQuery(4,QUERY4);
		System.out.println("Avg de temps amb tots els inserts " + String.format("%s",(float)temps/(float)4) + " mili ");

	}
	
	/*
		Executem la query 5 vegades, i ens quedem el temps menor
	*/
	private long exeQuery(int num, String sql) throws SQLException{
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
	
	/*
		Al excutar, verifiquem que hi hagi com a minim una tupla al resultat.
		Si no �s aix�, per evitar bucles infinits fem un m�xim de 20 intents
	*/
	private long executeQuery(int num, String sql) throws SQLException{
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


	
	/*  
		Fem tots els inserts. Per fer-ho primer preparem tots els insert en preparedstatements
		i despr�s els executem seq�encialment mitjan�ant el executeBatch.
		D'aquesta manera nom�s contem el temps d'insert real, i no el temps de preparaci�.
	*/
	private void insertBatchData(int start) throws SQLException{
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
	
	/*
		Preparem els inserts de region: amb tot randoms excepte r_regionkey que �s una seq��ncia
		a partir d'start. Fem aix� per despr�s garantir la clau forana manualment
	*/	
	private PreparedStatement insertsRegion(int start) throws SQLException{
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
	
	/*
		Preparem els inserts de nation: amb tot randoms.
		A l'atribut n_regionkey estem garantin la clau forana a region, assignant-li un valor
		que pertany al rang de valors establert a insertRegion (r_regionkey).
		
		NOTA: aquesta forma de garantir la clau forana l'hem seguit per a la resta d'inserts
	*/	
	private PreparedStatement insertsNation(int start) throws SQLException{
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
	/*
		Preparem els inserts de part: amb tot randoms.
		Ens guardem en una cache els valors de p_partkey per despr�s poder garantir la clau 
		forana composta manualment.
	*/
	private PreparedStatement insertsPart(int start) throws SQLException{
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
	/*
		Preparem els inserts de supplier: amb tot randoms.
		Ens guardem en una cache els valors de s_suppkey per despr�s poder garantir la clau 
		forana composta manualment.
	*/	
	private PreparedStatement insertsSupplier(int start) throws SQLException{
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
	/*
		Preparem els inserts de customer: amb tot randoms.
	*/	
	private PreparedStatement insertsCustomer(int start) throws SQLException{
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
	/*
		Preparem els inserts de partSupp: amb tot randoms. Excepte els atributs ps_partkey i ps_suppkey
		que els obtenim aleatoriament de les caches que hem guardat pr�viament
		
		I un cop hem triat els valors, els guardem conjuntament en una altra cache per a garantir la clau 
		forana composta de lineitem.
	*/	
	private PreparedStatement insertsPartSupp(int start) throws SQLException{
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

	/*
		Preparem els inserts de orders: amb tot randoms.
	*/	
	private PreparedStatement insertsOrders(int start) throws SQLException{
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

	/*
		Preparem els inserts de lineitem: amb tot randoms excepte l_partkey i l_suppkey que el plenem
		escollint valors de la cache que hem plenat a insertPartSupp.
	*/
	private PreparedStatement insertsLineItem(int start) throws SQLException{
		String selectSQL = "INSERT INTO lineitem VALUES (?,?,?,?,?,?,?,?,?,?,"+rDate()+","+rDate()+","+rDate()+",?,?,?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);
		for(int i = start; i < 19999+start; i++){
			Tuple t = cacheClausPartSup.get(rBetween(0, cacheClausPartSup.size()));
			
			preparedStatement.setInt(1, rBetween(start,start+4999));
			preparedStatement.setInt(2, t.a);
			preparedStatement.setInt(3, t.b);
			preparedStatement.setInt(4, rInt(4));
			preparedStatement.setInt(5, rInt(4));
			preparedStatement.setInt(6, rInt(13/2));
			preparedStatement.setInt(7, rInt(13/2));
			preparedStatement.setInt(8, rInt(13/2));
			preparedStatement.setString(9, rStr(64/2));
			preparedStatement.setString(10, rStr(64/2));
			// Data generada amb SQL
			// Data generada amb SQL
			// Data generada amb SQL
			preparedStatement.setString(11, rStr(64/2));
			preparedStatement.setString(12, rStr(64/2));
			preparedStatement.setString(13, rStr(64/2));
			preparedStatement.setString(14, rStr(64/2));
			preparedStatement.addBatch();
		}
		return preparedStatement;
	}
	
	/*  ---------------------- Buidar taules -----------------------  */
	private void deleteValues(){
		System.out.println("---------- DELETE VALUES ------------");
		try{
			Statement statement = connection.createStatement();
			List<String> deletes = new ArrayList<String>();
			deletes.add("DELETE FROM LINEITEM");
			deletes.add("DELETE FROM NATION");
			deletes.add("DELETE FROM customer");
			deletes.add("DELETE FROM orders");
			deletes.add("DELETE FROM part");
			deletes.add("DELETE FROM partsupp");
			deletes.add("DELETE FROM region");
			deletes.add("DELETE FROM supplier");
			for(String sql : deletes){
				statement.executeQuery(sql);
			}
		}
		catch(Exception e){
			System.out.println("No s'han pogut posar buidar les taules");
		}
	}
	
	/* Refresca manualment les vistes materialitzades 
	   A la �ltima versi� no s'utilitza aquesta funci�
	*/
	private void refeshViews(){
		System.out.println("---------- REFRESH VIEWS ------------");
		try{
			Statement statement = connection.createStatement();
			List<String> acc = new ArrayList<String>();
			acc.add("BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q2'); end;");
			acc.add("BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q2_SUB'); end;");
			acc.add("BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q3'); end;");
			acc.add("BEGIN DBMS_SNAPSHOT.REFRESH('VISTA_Q4'); end;");
			for(String sql : acc){
				statement.executeQuery(sql);
			}
		}
		catch(Exception e){
			System.out.println("No s'han pogut refer les vistes");
		}
	}
	
	/*  Calcular les dades per a que retorni alguna tupla  */
	private void updateValues(){
		System.out.println("---------- UPDATE VALUES ------------");
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
	}
	
	/* Queries que retornen alguna tupla */
	private void setQueries(){
		QUERY1 = "SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order FROM lineitem WHERE l_shipdate <= '"+qu1_data+"' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
		QUERY2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = '"+qu2_size+"' AND p_type like '%"+qu2_type+"' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu2_region+"' AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu2_region+"') ORDER BY s_acctbal desc, n_name, s_name, p_partkey";
		QUERY3 = "SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = '"+qu3_segment+"' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < '"+qu3_data1+"' AND l_shipdate > '"+qu3_data2+"' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue desc, o_orderdate";
		QUERY4 = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '"+qu4_region+"' AND o_orderdate >= '"+qu4_data1+"' AND o_orderdate < '"+qu4_data2+"' GROUP BY n_name ORDER BY revenue desc";
	
	}
	
	/* Serveix per generar un fitxer del que poder exectuar les queries que sabem que retornen alguna tupla */
	private void printQueries(boolean nestor){
		try{
			PrintWriter writer = new PrintWriter("queries-"+((nestor)?"nestor":"mireia")+".sql", "UTF-8");
			writer.println("---------- QUERY1: ------------");
			writer.println(QUERY1);
			writer.println("---------- QUERY2: ------------");
			writer.println(QUERY2);
			writer.println("---------- QUERY3: ------------");
			writer.println(QUERY3);
			writer.println("---------- QUERY4: ------------");
			writer.println(QUERY4);
			writer.close();
		} catch(Exception e){}
	}
	
	
	/*  ---------------------- Secci� per calcular valor randoms ------------------------------  */
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
	
	/* Calculem la data random amb oracle */
	public String rDate(){
		return " DBMS_RANDOM.VALUE*9999 + SYSDATE ";
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
