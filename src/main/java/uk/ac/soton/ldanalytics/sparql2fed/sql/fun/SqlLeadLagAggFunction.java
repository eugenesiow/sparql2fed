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
package uk.ac.soton.ldanalytics.sparql2fed.sql.fun;

import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperatorBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SameOperandTypeChecker;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlReturnTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlSingleOperandTypeChecker;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeFamily;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeTransform;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeTransforms;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * <code>LEAD</code> and <code>LAG</code> aggregate functions
 * return the value of given expression evaluated at given offset.
 */
public class SqlLeadLagAggFunction extends SqlAggFunction {
  private static final SqlSingleOperandTypeChecker OPERAND_TYPES =
      OperandTypes.or(
          OperandTypes.ANY,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC),
          OperandTypes.and(
              OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.ANY),
              // Arguments 1 and 3 must have same type
              new SameOperandTypeChecker(3) {
                @Override protected List<Integer>
                getOperandList(int operandCount) {
                  return ImmutableList.of(0, 2);
                }
              }));

  private static final SqlReturnTypeInference RETURN_TYPE =
      ReturnTypes.cascade(ReturnTypes.ARG0, new SqlTypeTransform() {
        public RelDataType transformType(SqlOperatorBinding binding,
            RelDataType type) {
          // Result is NOT NULL if NOT NULL default value is provided
          SqlTypeTransform transform;
          if (binding.getOperandCount() < 3) {
            transform = SqlTypeTransforms.FORCE_NULLABLE;
          } else {
            RelDataType defValueType = binding.getOperandType(2);
            transform = defValueType.isNullable()
                ? SqlTypeTransforms.FORCE_NULLABLE
                : SqlTypeTransforms.TO_NOT_NULLABLE;
          }
          return transform.transformType(binding, type);
        }
      });

  public SqlLeadLagAggFunction(SqlKind kind) {
    super(kind.name(),
        null,
        kind,
        RETURN_TYPE,
        null,
        OPERAND_TYPES,
        SqlFunctionCategory.NUMERIC,
        false,
        true);
    Preconditions.checkArgument(kind == SqlKind.LEAD
        || kind == SqlKind.LAG);
  }

  @Deprecated // to be removed before 2.0
  public SqlLeadLagAggFunction(boolean isLead) {
    this(isLead ? SqlKind.LEAD : SqlKind.LAG);
  }

  @Override public boolean allowsFraming() {
    return false;
  }

}

// End SqlLeadLagAggFunction.java
