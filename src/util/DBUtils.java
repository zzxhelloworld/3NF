package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import object.DataTypeEnum;
import object.FD;
import object.Key;
import object.Parameter;



/**
 * some tools here on database operations
 *
 */
public class DBUtils {
	/**
	 * 
	 * @param DBName database name
	 * @return
	 */
	public static Connection connectDB(String DBName) {
		String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
	    String DB_URL = "jdbc:mysql://localhost:3306/"+DBName+"?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
	    		+ "&useServerPrepStmts=true&cachePrepStmts=true&rewriteBatchedStatements=true";
	    String USER = "root";
	    String PASS = "zzxzzx";
	    Connection conn = null;
        try{
            Class.forName(JDBC_DRIVER);
        
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
        }catch(SQLException se){
            se.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }
        return conn;

	}

	
	/**
	 * 
	 * @param tableName
	 * @param R
	 * @throws SQLException
	 */
	public static void createTable(String DBName, String tableName,List<String> R) throws SQLException {
		Connection conn =connectDB(DBName);
		String sql = "CREATE TABLE `"+tableName+"` (  \n";
		sql += "`id` int NOT NULL AUTO_INCREMENT,\n";
		for(int i = 0;i< R.size();i ++) {
			String columnName = R.get(i);
			sql += "`"+columnName +"` varchar(50),\n";
		}
		sql += "PRIMARY KEY (`id`)\n ) CHARSET=utf8mb3";
		System.out.println("\n======================");
		System.out.println("creating table with name : "+tableName+" | schema : "+R.toString());
		System.out.println(sql);
		System.out.println("======================\n");
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(sql);
		stmt.close();
		conn.close();
	}
	
	public static void dropTable(String DBName, String tableName) throws SQLException {
		Connection conn =connectDB(DBName);
		String sql = "DROP TABLE IF EXISTS `"+tableName+"`";
		System.out.println("\n======================");
		System.out.println("dropping table with name : "+tableName);
		System.out.println(sql);
		System.out.println("======================\n");
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(sql);
		stmt.close();
		conn.close();
	}
	
	/**
	 * insert data into databases
	 * @param tableName
	 * @param dataset
	 * @return cost time
	 * @throws SQLException
	 */
	public static double insertData(String DBName, String tableName,List<List<String>> dataset) throws SQLException {
		if(dataset == null)
			return -1;
		if(dataset.isEmpty())
			return -1;
		Connection conn = connectDB(DBName);
		conn.setAutoCommit(false);//manual commit
		String insertSql = "INSERT INTO `"+tableName + "` VALUES ( NULL,";//increment column
		for(int i = 0;i < dataset.get(0).size();i ++) {
			if(i != (dataset.get(0).size() - 1))
				insertSql += " ? ,";
			else
				insertSql += " ? )";
		}
		PreparedStatement prepStmt1 = conn.prepareStatement(insertSql);
		System.out.println("\n==================");
		System.out.println("insert "+dataset.size()+" records into table : "+tableName);
		System.out.println(insertSql);
		
		long start = System.currentTimeMillis();
		int count = 0;
		for(List<String> data : dataset) {
			count ++;
			for(int i = 1;i <= data.size();i ++) {
				prepStmt1.setString(i, data.get(i-1));
			}
			prepStmt1.addBatch();//batch process
			if(count % 10000 == 0) {
				prepStmt1.executeBatch();
				conn.commit();
				prepStmt1.clearBatch();
			}
		}
		prepStmt1.executeBatch();
		conn.commit();//commit
		long end = System.currentTimeMillis();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		prepStmt1.close();
		conn.close();
		return (double)(end - start);
	}
	
	public static double insertDataWithoutID(String DBName, String tableName,List<List<String>> dataset) {
		if(dataset == null)
			return -1;
		if(dataset.isEmpty())
			return -1;
		Connection conn = connectDB(DBName);
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//manual commit
		String insertSql = "INSERT INTO `"+tableName + "` VALUES ( ";
		for(int i = 0;i < dataset.get(0).size();i ++) {
			if(i != (dataset.get(0).size() - 1))
				insertSql += " ? ,";
			else
				insertSql += " ? )";
		}
		PreparedStatement prepStmt1 = null;
		try {
			prepStmt1 = conn.prepareStatement(insertSql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("\n==================");
		System.out.println("insert "+dataset.size()+" records into table : "+tableName);
		System.out.println(insertSql);
		
		long start = System.currentTimeMillis();
		int count = 0;
		for(List<String> data : dataset) {
			count ++;
			try {
				for(int i = 1;i <= data.size();i ++) {
					prepStmt1.setString(i, data.get(i-1));
				}
				prepStmt1.addBatch();//batch process
				if(count % 10000 == 0) {
					prepStmt1.executeBatch();
					conn.commit();
					prepStmt1.clearBatch();
				}
			}catch (SQLException e) {
				System.out.println("Update denied.");
				System.out.println(e.getMessage());
				continue;
			}
		}
		try {
			prepStmt1.executeBatch();
			conn.commit();//commit
		}catch(SQLException e) {
			System.out.println("Update denied.");
			System.out.println(e.getMessage());
		}
		
		long end = System.currentTimeMillis();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		try {
			prepStmt1.close();
			conn.close();
		}catch(SQLException e) {
			System.out.println("DB connection failed to close.");
		}
		
		return (double)(end - start);
	}
	
	
	
	/**
	 * delete all data from table with specific condition
	 * @param tableName
	 * @param dataset
	 * @param whereCondition according condition to delete tuples
	 * @throws SQLException 
	 */
	public static void deleteData(String DBName, String tableName,String whereCondition) throws SQLException {
		Connection conn = connectDB(DBName);
		System.out.println("\n==================");
		System.out.println("delete inserted records from table : "+tableName);
		String delSql = "DELETE FROM `"+tableName + "` WHERE "+whereCondition;//delete records

		System.out.println(delSql);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(delSql);
		
		System.out.println("==================\n");
		stmt.close();
		conn.close();
	}
	
	/**
	 * specify a set of attributes (we call key) as an unique constraint on a table
	 * @param key
	 * @param tableName
	 * @throws SQLException
	 */
	public static void addUnique(String DBName, Key key,String tableName,String uniqueID) throws SQLException {
		Connection conn = connectDB(DBName);
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		
		String unique_constraint ="ALTER TABLE `"+tableName+"` ADD UNIQUE `"+uniqueID+"` ( ";//create an unique constraint
		for(int i =0;i < key.size();i ++) {
			if(i != (key.size() -1))
				unique_constraint += "`"+key.getAttributes().get(i) + "`,";
			else
				unique_constraint += "`"+key.getAttributes().get(i) + "` )";
		}
		
		System.out.println("\n==================");
		System.out.println("create an unique constarint : ");
		System.out.println(unique_constraint);
		
		stmt.executeUpdate(unique_constraint);
		
		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
	/**
	 * remove an unique constraint on a table
	 * @param tableName
	 * @param unique_id
	 * @throws SQLException
	 */
	public static void removeUnique(String DBName, String tableName,String uniqueID) throws SQLException {
		Connection conn = connectDB(DBName);
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		String del_unique_constraint = "DROP INDEX `"+uniqueID+"` ON `"+tableName+"`";//delete an unique constraint
		
//		System.out.println("\n==================");
//		System.out.println("delete an unique constarint : ");
//		System.out.println(del_unique_constraint);
		
		stmt.executeUpdate(del_unique_constraint);
		
//		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	

	
	public static void addTrigger(String DBName, List<FD> fd_list,String tableName,String trigger_id) throws SQLException {
		String delete_sql = "DELETE FROM `"+tableName+"` WHERE `"+tableName+"`.`id` = new.`id`";
		List<String> FD_check_query_list = new ArrayList<>();//each string to validate one FD
		
		for(int i = 0;i < fd_list.size();i ++) {
			String FDLeftFilter = " SELECT * FROM `"+tableName+"` WHERE ";
			String FDRightFilter = "";
			
			FD fd = fd_list.get(i);
			if(fd.getLeftHand().isEmpty())
				continue;
			for(int m = 0;m < fd.getLeftHand().size();m ++) {
				String attr = fd.getLeftHand().get(m);
				if(m != fd.getLeftHand().size() - 1)
					FDLeftFilter += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` AND ";
				else
					FDLeftFilter += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` ";
			}
			
			for(int j = 0;j < fd.getRightHand().size();j ++) {
				if(j != fd.getRightHand().size() - 1)
					FDRightFilter += "t"+i+".`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"` OR ";
				else
					FDRightFilter += "t"+i+".`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"`  ";
			}
			
			String valid_sql = "EXISTS( SELECT * FROM ("+FDLeftFilter+") as t"+i+" WHERE "+FDRightFilter+")";
			FD_check_query_list.add(valid_sql);
		}
		
		String union = "";
		for(int i = 0;i < FD_check_query_list.size();i ++) {
			if(i != FD_check_query_list.size() - 1)
				union += FD_check_query_list.get(i)+" OR ";
			else
				union += FD_check_query_list.get(i);
		}
		
		String TRIGGER = "CREATE TRIGGER `"+trigger_id+"`\r\n"
				+ "AFTER INSERT ON `"+tableName+"`\r\n"
				+ "FOR EACH ROW\r\n"
				+ "BEGIN \r\n"
				+ "   set @violation = IF("+union+",'YES','NO');\r\n"
				+ "   if @violation = 'YES' THEN\r\n"
				+ "       "+delete_sql+" ;\r\n"
				+ "   end if;\r\n"
				+ "END ";
		
		
		Connection conn = DBUtils.connectDB(DBName);
		Statement stmt = conn.createStatement();
		System.out.println("add TRIGGER "+trigger_id+" into table "+tableName+"...");
		System.out.println(TRIGGER);
		stmt.executeUpdate(TRIGGER);
		
		stmt.close();
		conn.close();
	}
	
	public static void addTrigger4TPCH(String DBName, List<FD> fd_list,String tableName,String trigger_id, Key key) throws SQLException {
		String delete_sql = "DELETE FROM `"+tableName+"` WHERE ";
		for(int i = 0;i < key.size();i ++) {
			String a = key.getAttributes().get(i);
			if(i != key.size() - 1)
				delete_sql += "`"+tableName+"`.`"+a+"` = new.`"+a+"` AND ";
			else
				delete_sql += "`"+tableName+"`.`"+a+"` = new.`"+a+"` ";
		}
		List<String> FD_check_query_list = new ArrayList<>();//each string to validate one FD
		
		for(int i = 0;i < fd_list.size();i ++) {
			String FDLeftFilter = " SELECT * FROM `"+tableName+"` WHERE ";
			String FDRightFilter = "";
			
			FD fd = fd_list.get(i);
			if(fd.getLeftHand().isEmpty())
				continue;
			for(int m = 0;m < fd.getLeftHand().size();m ++) {
				String attr = fd.getLeftHand().get(m);
				if(m != fd.getLeftHand().size() - 1)
					FDLeftFilter += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` AND ";
				else
					FDLeftFilter += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` ";
			}
			
			for(int j = 0;j < fd.getRightHand().size();j ++) {
				if(j != fd.getRightHand().size() - 1)
					FDRightFilter += "t"+i+".`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"` OR ";
				else
					FDRightFilter += "t"+i+".`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"`  ";
			}
			
			String valid_sql = "EXISTS( SELECT * FROM ("+FDLeftFilter+") as t"+i+" WHERE "+FDRightFilter+")";
			FD_check_query_list.add(valid_sql);
		}
		
		String union = "";
		for(int i = 0;i < FD_check_query_list.size();i ++) {
			if(i != FD_check_query_list.size() - 1)
				union += FD_check_query_list.get(i)+" OR ";
			else
				union += FD_check_query_list.get(i);
		}
		
		String TRIGGER = "CREATE TRIGGER `"+trigger_id+"`\r\n"
				+ "AFTER INSERT ON `"+tableName+"`\r\n"
				+ "FOR EACH ROW\r\n"
				+ "BEGIN \r\n"
				+ "   set @violation = IF("+union+",'YES','NO');\r\n"
				+ "   if @violation = 'YES' THEN\r\n"
				+ "       "+delete_sql+" ;\r\n"
				+ "   end if;\r\n"
				+ "END ";
		
		
		Connection conn = DBUtils.connectDB(DBName);
		Statement stmt = conn.createStatement();
		System.out.println("add TRIGGER "+trigger_id+" into table "+tableName+"...");
		System.out.println(TRIGGER);
		stmt.executeUpdate(TRIGGER);
		
		stmt.close();
		conn.close();
	}
	
	public static void removeTrigger(String DBName, String tableName,String trigger_id) throws SQLException {
		String remove_sql = "DROP TRIGGER IF EXISTS `"+trigger_id+"`";
		Connection conn = connectDB(DBName);
		Statement stmt = conn.createStatement();
//		System.out.println("remove TRIGGER "+trigger_id+" from table "+tableName+"...");
//		System.out.println(remove_sql);
		stmt.executeUpdate(remove_sql);
		
		stmt.close();
		conn.close();
	}
	
	
	
	/**
	 * 
	 * @param para
	 * @return database table name for corresponding data set name
	 */
	public static String getDBTableName(Parameter para) {
		String tableName = null;
		switch(para.dataset.DataType) {
			case COMPLETE:
				tableName = para.dataset.name;
				break;
			case NULL_EQUALITY:
				tableName = para.dataset.name+"(nulleq)";
				break;
			case NULL_UNCERTAINTY:
				tableName = para.dataset.name+"(nulluc)";
				break;
		}
		return tableName;
	}
	/**
	 * set null marker in databases as Null value
	 * @param para
	 * @throws SQLException 
	 */
	public static void setNullMarkerAsNull(String DBName, Parameter para) {
		Connection conn = null;
		Statement stmt = null;
		String tableName = getDBTableName(para);
		try {
			conn = connectDB(DBName);
			stmt = conn.createStatement();
			for(int attr = 0; attr < para.dataset.col_num; attr ++) {
				String update = "Update `"+tableName+"` set `"+attr+"` = NULL where `"+attr+"` = '"+para.dataset.nullMarker+"'";
				stmt.executeUpdate(update);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 
	 * @param key
	 * @return redundant column combination that validates keys
	 * @throws SQLException 
	 */
	public static List<List<String>> validateKeys(String DBName, Key key, String tableName) throws SQLException{
		List<List<String>> redundant = new ArrayList<>();
		Connection conn = connectDB(DBName);
		Statement stmt = conn.createStatement();
		String sql = "SELECT ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != key.size() - 1)
				sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += " FROM `"+tableName+"` GROUP BY ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "HAVING COUNT(*) > 1";
		
		System.out.println("\n======================");
		System.out.println("executing key validation...");
		System.out.println(sql);
		
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			List<String> r = new ArrayList<>();
			for(String a : key.getAttributes()) {
				r.add(rs.getString(a));
			}
			redundant.add(r);
		}
		
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("======================\n");
		return redundant;
	}
	
	/**
	 * 
	 * @param colAttrs column attributes
	 * @param tuplesToUpdate tuples on colAttrs that will be updated
	 * @param tableName table name
	 * @throws SQLException 
	 */
	public static void updateSpecificTuples(String DBName, List<String> colAttrs, List<List<String>> tuplesToUpdate, String tableName) throws SQLException {
		if(tuplesToUpdate == null)
			return ;
		if(tuplesToUpdate.isEmpty())
			return ;
		int id = 0;
		Connection conn = connectDB(DBName);
		Statement stmt = conn.createStatement();
		ResultSet rs = null;
		for(List<String> tuple : tuplesToUpdate) {
			List<Integer> IDs = new ArrayList<Integer>();
			String select = "SELECT `id` FROM `"+tableName+"` WHERE ";
			for(int i = 0;i < colAttrs.size();i ++) {
				if(i != colAttrs.size() - 1)
					select += "`"+colAttrs.get(i) + "` = '" + tuple.get(i) + "' AND ";
				else
					select += "`"+colAttrs.get(i) + "` = '" + tuple.get(i) + "';";
			}
			rs = stmt.executeQuery(select);
			while(rs.next()) {
				IDs.add(rs.getInt("id"));
			}
			rs.close();
			
			
			for(int i = 0;i < IDs.size();i ++) {
				String updateSQL = "UPDATE `"+tableName+"` SET `"+colAttrs.get(0)+"` = 'unknown" + id++ +"' WHERE ";
				int tid = IDs.get(i);
				updateSQL += "`id` = "+tid;
				System.out.println(updateSQL);
				stmt.executeUpdate(updateSQL);
			}
			
		}
		stmt.close();
		conn.close();
	}
	
	public static List<List<String>> getProjectionForSubschema(String DBName, List<String> subschema,String OriginTable) throws SQLException{
		List<List<String>> projection = new ArrayList<List<String>>();
		
		Connection conn = connectDB(DBName);
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		String str = "";
		for(int i = 0;i < subschema.size();i ++) {
			if(i != subschema.size() - 1)
				str += "`"+subschema.get(i)+"`, ";
			else
				str += "`"+subschema.get(i)+"` ";
		}
		String sql = "SELECT DISTINCT "+str+" FROM `"+OriginTable+"`";
		System.out.println("\n======================");
		System.out.println("get projection on "+subschema.toString()+" from "+OriginTable);
		System.out.println(sql);
		
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()) {
			int col_num = rs.getMetaData().getColumnCount();
			List<String> row = new ArrayList<String>();
			for(int i = 1;i <= col_num;i ++) {
				row.add(rs.getString(i));
			}
			projection.add(row);
		}
		System.out.println("projection row num : "+projection.size());
		System.out.println("======================\n");
		return projection;
	}
	
	
	public static List<List<String>> genInsertedDataset(int row_num,int col_num){
		List<List<String>> dataset = new ArrayList<List<String>>();
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				data.add(i+"_"+j);
			}
			dataset.add(data);
		}
		return dataset;
	}

	public static void main(String[] args) throws SQLException {
		Utils.getParameterList(Arrays.asList("uniprot"), DataTypeEnum.NULL_UNCERTAINTY).forEach(para->DBUtils.setNullMarkerAsNull("freeman",para));
	}

}
