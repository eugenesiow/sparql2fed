package uk.ac.soton.ldanalytics.sparql2fed.adapter;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCluster;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptTable;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelTraitSet;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableScan;

public class S2SMLTableScan extends TableScan implements JdbcRel {
	  final S2SMLTable S2SMLTable;

	  protected S2SMLTableScan(RelOptCluster cluster, RelOptTable table, S2SMLTable S2SMLTable, JdbcConvention jdbcConvention) {
		  super(cluster, cluster.traitSetOf(jdbcConvention), table);
		  this.S2SMLTable = S2SMLTable;
		  assert S2SMLTable != null;
	  }

	  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		  assert inputs.isEmpty();
		  return new S2SMLTableScan(
				  getCluster(), table, S2SMLTable, (JdbcConvention) getConvention());
	  }

	  public JdbcImplementor.Result implement(JdbcImplementor implementor) {
		  return implementor.result(S2SMLTable.tableName(),Collections.singletonList(JdbcImplementor.Clause.FROM), this);
	  }
}
