package uk.ac.soton.ldanalytics.sparql2fed;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;

public class S2SMLSchema implements Schema {

	public S2SMLSchema(DataSource dataSource, SqlDialect dialect,JdbcConvention convention, String catalog, String schema) {
		super();
//		super(dataSource, dialect, convention, catalog, schema);
	}
	
	public Set<String> getTableNames() {
	    // This method is called during a cache refresh. We can take it as a signal
	    // that we need to re-build our own cache.
		System.out.println("getnames");
		return new HashSet<String>();
//	    return getTableMap(true).keySet();
	}
	
	public Table getTable(String name) {
		System.out.println("gettab");
		return null;
//		return getTableMap(false).get(name);
	}
	
//	private synchronized ImmutableMap<String, JdbcTable> getTableMap(boolean force) {
//		if (force || tableMap == null) {
//    		tableMap = computeTables();
//    	}
//    	return tableMap;
//	}

	public static class Factory implements SchemaFactory {
	    public Schema create(
	        SchemaPlus parentSchema,
	        String name,
	        Map<String, Object> operand) {
	    		return S2SMLSchema.create(parentSchema, name, operand);
    	}
	}

	public Collection<Function> getFunctions(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Schema create(SchemaPlus parentSchema, String name,
			Map<String, Object> operand) {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<String> getFunctionNames() {
		// TODO Auto-generated method stub
		return null;
	}

	public Schema getSubSchema(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<String> getSubSchemaNames() {
		// TODO Auto-generated method stub
		return null;
	}

	public Expression getExpression(SchemaPlus parentSchema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isMutable() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean contentsHaveChangedSince(long lastCheck, long now) {
		// TODO Auto-generated method stub
		return false;
	}
}
