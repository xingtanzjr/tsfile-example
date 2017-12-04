package thu.iotdbjdbc.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCDemo {

	private static final String QUERY_SQL = "select axis1pos from root.yanmoji.shenzhen.d1 where time > 1502089355959";

	public static void main(String args[]) throws SQLException {
		Connection connection = null;
		Statement statement = null;
		try {
			Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
			connection = DriverManager.getConnection("jdbc:tsfile://211.159.153.193:6667/", "root", "root");
			statement = connection.createStatement();
			System.out.println("Start execute SQL : " + QUERY_SQL + "\n");
			long startTimestamp = System.currentTimeMillis();
			boolean hasResultSet = statement.execute(QUERY_SQL);
			if (hasResultSet) {
				ResultSet res = statement.getResultSet();
				while (res.next()) {
					System.out.println(
							res.getString("Time") + " | " + res.getString("root.yanmoji.shenzhen.d1.axis1pos"));
				}
			}
			long endTimestamp = System.currentTimeMillis();
			System.out.println("------------------------\nDone. Time: " + (endTimestamp - startTimestamp) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}
}
