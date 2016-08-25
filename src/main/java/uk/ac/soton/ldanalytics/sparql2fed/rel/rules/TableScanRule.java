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

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRuleCall;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptTable;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptUtil;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalTableScan;

/**
 * Planner rule that converts a
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalTableScan} to the result
 * of calling {@link RelOptTable#toRel}.
 */
public class TableScanRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final TableScanRule INSTANCE = new TableScanRule();

  //~ Constructors -----------------------------------------------------------

  private TableScanRule() {
    super(operand(LogicalTableScan.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalTableScan oldRel = call.rel(0);
    RelNode newRel =
        oldRel.getTable().toRel(
            RelOptUtil.getContext(oldRel.getCluster()));
    call.transformTo(newRel);
  }
}

// End TableScanRule.java
