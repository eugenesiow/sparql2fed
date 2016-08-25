package sparql2fed;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;

public class test {
	public static void main(String[] args) {
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
			Properties info = new Properties();
			info.setProperty("lex", "JAVA");
			info.setProperty("model", "jmodel.json");
			Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
			CalciteConnection calciteConnection =
			    connection.unwrap(CalciteConnection.class);
			
			Statement statement = calciteConnection.createStatement();
			ResultSet resultSet = statement.executeQuery(
			    "select AVG(environment.INSIDETEMP) from _ALT.ENVIRONMENT environment GROUP BY EXTRACT(HOUR FROM environment.TIMESTAMPUTC)");
			ResultSetMetaData rsmd = resultSet.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while (resultSet.next()) {
				for (int i = 1; i <= columnsNumber; i++) {
		            if (i > 1) System.out.print(",\t");
		            String columnValue = resultSet.getString(i);
		            System.out.print(columnValue);
		        }
		        System.out.println("");
		    }
			resultSet.close();
			statement.close();
			connection.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
