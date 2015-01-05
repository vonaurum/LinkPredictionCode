package linkpred.trust;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

/**
 * @author Muhammad Aurangzeb Ahmad
 * 
 * Copyright 2010 Regents of the University of  Minnesota. All rights reserved.
 */
public class TestConnection {
	private static final String KEY_GEN = "SELECT * FROM churn";//"select ISNULL(MAX(churn_id)+1,1) from churn";
	private static final String USERNAME = "vweuiuser";
	private static final String PASSWORD = "VWE_User@456";
	
	public static void main (String args[]){
		TestConnection p = new TestConnection();
		p.run();
	}
	//"churn_id", "account_id", "churn_probability", "year", "month"
	public void run(){
		Connection conn = getSQLConnection();
		Statement stmt;
		Calendar cal = Calendar.getInstance();
		double begin = (double)cal.getTimeInMillis()/1000.0;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(KEY_GEN);
			ResultSetMetaData md = rs.getMetaData();
		    int numCols = md.getColumnCount();
			String str;
			//
			int j = 0;
			while  (rs != null && rs.next() && j < 1000) {
				for (int i = 1; i <= numCols; i++){
					//str = md.getColumnName(i);
					str = rs.getString(i);
					//prln(i + " "  +str +" ");
					
				}
				prln(j+"");
				j++;
				if (j == 5000000




){
					cal = Calendar.getInstance();
					double end = (double)cal.getTimeInMillis()/1000.0;
					prln("Total Records " + j);
					prln("" + (end-begin));
					exit();
				}
				//prln("\n");
			}
			/*
			cal = Calendar.getInstance();
			double end = (double)cal.getTimeInMillis()/1000.0;
			prln("Total Records " + j);
			prln("Time taken = " + (end-begin));
			*/
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static Connection getSQLConnection() {
		Connection conn = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String connectionUrl = "jdbc:sqlserver://128.101.36.103:1433;databaseName=VWE_Datamart;";
			conn = DriverManager.getConnection(connectionUrl, USERNAME,PASSWORD);
			if (conn != null)
				System.out.println("SQL Connection Successful!");

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error Trace in getConnection() : "
					+ e.getMessage());
		}
		return conn;
	}

	public void err(String str){
		System.err.println(str);
	}
	

	public void pr(String str){
		System.out.print(str);
	}	

	public void prln(String str){
		System.out.println(str);
	}
	
	public void exit(){
		System.exit(0);
	}	
}
