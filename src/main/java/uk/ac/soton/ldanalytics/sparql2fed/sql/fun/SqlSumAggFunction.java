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
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeFactory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSplittableAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * <code>Sum</code> is an aggregator which returns the sum of the values which
 * go into it. It has precisely one argument of numeric type (<code>int</code>,
 * <code>long</code>, <code>float</code>, <code>double</code>), and the result
 * is the same type.
 */
public class SqlSumAggFunction extends SqlAggFunction {

  //~ Instance fields --------------------------------------------------------

  @Deprecated // to be removed before 2.0
  private final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  public SqlSumAggFunction(RelDataType type) {
    super(
        "SUM",
        null,
        SqlKind.SUM,
        ReturnTypes.AGG_SUM,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false);
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  @Deprecated // to be removed before 2.0
  public RelDataType getType() {
    return type;
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return type;
  }

  @Override public <T> T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(SqlSplittableAggFunction.SumSplitter.INSTANCE);
    }
    return super.unwrap(clazz);
  }
}

// End SqlSumAggFunction.java
