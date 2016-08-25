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
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCall;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSplittableAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSyntax;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeName;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Definition of the SQL <code>COUNT</code> aggregation function.
 *
 * <p><code>COUNT</code> is an aggregator which returns the number of rows which
 * have gone into it. With one argument (or more), it returns the number of rows
 * for which that argument (or all) is not <code>null</code>.
 */
public class SqlCountAggFunction extends SqlAggFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCountAggFunction() {
    super(
        "COUNT",
        null,
        SqlKind.COUNT,
        ReturnTypes.BIGINT,
        null,
        SqlValidator.STRICT
            ? OperandTypes.ANY
            : OperandTypes.ONE_OR_MORE,
        SqlFunctionCategory.NUMERIC,
        false,
        false);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_STAR;
  }

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.ANY), true));
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.BIGINT);
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Check for COUNT(*) function.  If it is we don't
    // want to try and derive the "*"
    if (call.isCountStar()) {
      return validator.getTypeFactory().createSqlType(
          SqlTypeName.BIGINT);
    }
    return super.deriveType(validator, scope, call);
  }

  @Override public <T> T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE);
    }
    return super.unwrap(clazz);
  }
}

// End SqlCountAggFunction.java
