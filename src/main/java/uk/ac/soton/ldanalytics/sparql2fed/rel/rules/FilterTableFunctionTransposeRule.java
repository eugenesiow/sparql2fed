/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.soton.ldanalytics.sparql2fed.rel.rules;

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCluster;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalTableFunctionScan;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelColumnMapping;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Planner rule that pushes
 * a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalFilter}
 * past a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalTableFunctionScan}.
 */
public class FilterTableFunctionTransposeRule extends RelOptRule {
  public static final FilterTableFunctionTransposeRule INSTANCE =
      new FilterTableFunctionTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterTableFunctionTransposeRule.
   */
  private FilterTableFunctionTransposeRule() {
    super(
        operand(LogicalFilter.class,
            operand(LogicalTableFunctionScan.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    LogicalTableFunctionScan funcRel = call.rel(1);
    Set<RelColumnMapping> columnMappings = funcRel.getColumnMappings();
    if (columnMappings == null || columnMappings.isEmpty()) {
      // No column mapping information, so no push-down
      // possible.
      return;
    }

    List<RelNode> funcInputs = funcRel.getInputs();
    if (funcInputs.size() != 1) {
      // TODO:  support more than one relational input; requires
      // offsetting field indices, similar to join
      return;
    }
    // TODO:  support mappings other than 1-to-1
    if (funcRel.getRowType().getFieldCount()
        != funcInputs.get(0).getRowType().getFieldCount()) {
      return;
    }
    for (RelColumnMapping mapping : columnMappings) {
      if (mapping.iInputColumn != mapping.iOutputColumn) {
        return;
      }
      if (mapping.derived) {
        return;
      }
    }
    final List<RelNode> newFuncInputs = new ArrayList<RelNode>();
    final RelOptCluster cluster = funcRel.getCluster();
    final RexNode condition = filter.getCondition();

    // create filters on top of each func input, modifying the filter
    // condition to reference the child instead
    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    List<RelDataTypeField> origFields = funcRel.getRowType().getFieldList();
    // TODO:  these need to be non-zero once we
    // support arbitrary mappings
    int[] adjustments = new int[origFields.size()];
    for (RelNode funcInput : funcInputs) {
      RexNode newCondition =
          condition.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  origFields,
                  funcInput.getRowType().getFieldList(),
                  adjustments));
      newFuncInputs.add(
          LogicalFilter.create(funcInput, newCondition));
    }

    // create a new UDX whose children are the filters created above
    LogicalTableFunctionScan newFuncRel =
        LogicalTableFunctionScan.create(cluster, newFuncInputs,
            funcRel.getCall(), funcRel.getElementType(), funcRel.getRowType(),
            columnMappings);
    call.transformTo(newFuncRel);
  }
}

// End FilterTableFunctionTransposeRule.java