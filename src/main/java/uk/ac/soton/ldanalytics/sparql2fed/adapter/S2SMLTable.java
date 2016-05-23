package uk.ac.soton.ldanalytics.sparql2fed.adapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class S2SMLTable extends AbstractQueryableTable implements TranslatableTable, ScannableTable {
	  private RelProtoDataType protoRowType;
	  private final S2SMLSchema S2SMLschema;
	  private final String jdbcCatalogName;
	  private final String jdbcSchemaName;
	  private final String jdbcTableName;
	  private final Schema.TableType jdbcTableType;

	protected S2SMLTable(S2SMLSchema S2SMLschema, String jdbcCatalogName,
		      String jdbcSchemaName, String tableName, Schema.TableType jdbcTableType) {
		super(Object[].class);
	    this.S2SMLschema = S2SMLschema;
	    this.jdbcCatalogName = jdbcCatalogName;
	    this.jdbcSchemaName = jdbcSchemaName;
	    this.jdbcTableName = tableName;
	    this.jdbcTableType = Preconditions.checkNotNull(jdbcTableType);
	}
	
	SqlString generateSql() {
		System.out.println("generate");
	    final SqlNodeList selectList =
	        new SqlNodeList(
	            Collections.singletonList(SqlIdentifier.star(SqlParserPos.ZERO)),
	            SqlParserPos.ZERO);
	    SqlSelect node =
	        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList,
	            tableName(), null, null, null, null, null, null, null);
	    final SqlPrettyWriter writer = new SqlPrettyWriter(S2SMLschema.dialect);
	    node.unparse(writer, 0, 0);
	    return writer.toSqlString();
	}

	SqlIdentifier tableName() {
	    final List<String> strings = new ArrayList<String>();
	    if (S2SMLschema.catalog != null) {
	      strings.add(S2SMLschema.catalog);
	    }
	    if (S2SMLschema.schema != null) {
	      strings.add(S2SMLschema.schema);
	    }
	    strings.add(jdbcTableName);
	    return new SqlIdentifier(strings, SqlParserPos.ZERO);
	}

	public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
			SchemaPlus schema, String tableName) {/** Enumerable that returns the contents of a {@link JdbcTable} by connecting
		   * to the JDBC data source. */
		return new JdbcTableQueryable<T>(queryProvider, schema, tableName);
	}

	public Enumerable<Object[]> scan(DataContext root) {
		final JavaTypeFactory typeFactory = root.getTypeFactory();
	    final SqlString sql = generateSql();
	    return ResultSetEnumerable.of(S2SMLschema.getDataSource(), sql.getSql(),
	        JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
	}

	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		System.out.println("torel");
	    return new S2SMLTableScan(context.getCluster(), relOptTable, this,
	            S2SMLschema.convention);
	}
	
	  private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses(final JavaTypeFactory typeFactory) { 
		  final RelDataType rowType = protoRowType.apply(typeFactory);
		  return Lists.transform(rowType.getFieldList(), new Function<RelDataTypeField, Pair<ColumnMetaData.Rep, Integer>>() {
			  public Pair<ColumnMetaData.Rep, Integer>
			  apply(RelDataTypeField field) {
				  final RelDataType type = field.getType();
				  final Class clazz = (Class) typeFactory.getJavaClass(type);
				  final ColumnMetaData.Rep rep = Util.first(ColumnMetaData.Rep.of(clazz),ColumnMetaData.Rep.OBJECT);
				  return Pair.of(rep, type.getSqlTypeName().getJdbcOrdinal());
	          }
	        });
	  }

	/** Enumerable that returns the contents of a {@link JdbcTable} by connecting
	   * to the JDBC data source. */
	private class JdbcTableQueryable<T> extends AbstractTableQueryable<T> {
		public JdbcTableQueryable(QueryProvider queryProvider, SchemaPlus schema,String tableName) {
			super(queryProvider, schema, S2SMLTable.this, tableName);
		}

		public Enumerator<T> enumerator() {
			final JavaTypeFactory typeFactory = ((CalciteConnection) queryProvider).getTypeFactory();
			final SqlString sql = generateSql();
			//noinspection unchecked
			System.out.println(sql.getSql());
			final Enumerable<T> enumerable = (Enumerable<T>) ResultSetEnumerable.of(
					S2SMLschema.getDataSource(),
					sql.getSql(),
					JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
			return enumerable.enumerator();
	    }
	}

	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		return typeFactory.builder().add("AIRTEMPERATURE",
		        typeFactory.createMapType(
		            typeFactory.createSqlType(SqlTypeName.VARCHAR),
		            typeFactory.createTypeWithNullability(
		                typeFactory.createSqlType(SqlTypeName.ANY), true))).build();
	}
}
