package uk.ac.soton.ldanalytics.sparql2fed.adapter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;

public class S2SMLSchema extends AbstractSchema {

	public S2SMLSchema(DataSource dataSource, SqlDialect dialect,JdbcConvention convention, String catalog, String schema) {
		super();
//		super(dataSource, dialect, convention, catalog, schema);
	}
	
	@Override 
	protected Map<String, Table> getTableMap() {
	    
	    // Build a map from table name to table; each file becomes a table.
	    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
//	    for (File file : files) {
	      String tableName = "_ALT";
	
	      final JdbcTable table =
	              new JdbcTable(this, catalogName, schemaName, tableName, tableType);
	      builder.put(tableName, table);
//	    }
	    return builder.build();
	}

	public static class Factory implements SchemaFactory {
	    public Schema create(
	        SchemaPlus parentSchema,
	        String name,
	        Map<String, Object> operand) {
	    		return S2SMLSchema.create(parentSchema, name, operand);
		}
	}
	
	public static S2SMLSchema create(
		SchemaPlus parentSchema,
	    String name,
	    DataSource dataSource,
	    String catalog,
	    String schema) {
	    final Expression expression =
	        Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
	    final SqlDialect dialect = createDialect(dataSource);
	    final JdbcConvention convention =
	        JdbcConvention.of(dialect, expression, name);
	    return new S2SMLSchema(dataSource, dialect, convention, catalog, schema);
	}

	private static SqlDialect createDialect(DataSource dataSource) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Schema create(SchemaPlus parentSchema, String name,
			Map<String, Object> operand) {
		DataSource dataSource;
	    try {
	      final String dataSourceName = (String) operand.get("dataSource");
	      if (dataSourceName != null) {
	        final Class<?> clazz = Class.forName((String) dataSourceName);
	        dataSource = (DataSource) clazz.newInstance();
	      } else {
	        final String jdbcUrl = (String) operand.get("jdbcUrl");
	        final String jdbcDriver = (String) operand.get("jdbcDriver");
	        final String jdbcUser = (String) operand.get("jdbcUser");
	        final String jdbcPassword = (String) operand.get("jdbcPassword");
	        dataSource = dataSource(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword);
	      }
	    } catch (Exception e) {
	      throw new RuntimeException("Error while reading dataSource", e);
	    }
	    String jdbcCatalog = (String) operand.get("jdbcCatalog");
	    String jdbcSchema = (String) operand.get("jdbcSchema");
	    return S2SMLSchema.create(
	        parentSchema, name, dataSource, jdbcCatalog, jdbcSchema);
	}
	
	

	/** Creates a JDBC data source with the given specification. */
	public static DataSource dataSource(String url, String driverClassName,String username, String password) {
		if (url.startsWith("jdbc:hsqldb:")) {
			// Prevent hsqldb from screwing up java.util.logging.
			System.setProperty("hsqldb.reconfig_logging", "false");
		}
		return JdbcUtils.DataSourcePool.INSTANCE.get(url, driverClassName, username,password);
  }
	
}
