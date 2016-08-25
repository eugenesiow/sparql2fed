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
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperatorBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlReturnTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeTransforms;
import uk.ac.soton.ldanalytics.sparql2fed.sql.util.ReflectiveSqlOperatorTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Operator table that contains only Oracle-specific functions and operators.
 */
public class OracleSqlOperatorTable extends ReflectiveSqlOperatorTable {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The table of contains Oracle-specific operators.
   */
  private static OracleSqlOperatorTable instance;

  /** Return type inference for {@code DECODE}. */
  protected static final SqlReturnTypeInference DECODE_RETURN_TYPE =
      new SqlReturnTypeInference() {
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          final List<RelDataType> list = new ArrayList<>();
          for (int i = 1, n = opBinding.getOperandCount(); i < n; i++) {
            if (i < n - 1) {
              ++i;
            }
            list.add(opBinding.getOperandType(i));
          }
          final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
          RelDataType type = typeFactory.leastRestrictive(list);
          if (opBinding.getOperandCount() % 2 == 1) {
            type = typeFactory.createTypeWithNullability(type, true);
          }
          return type;
        }
      };

  /** The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function. */
  public static final SqlFunction DECODE =
      new SqlFunction("DECODE", SqlKind.DECODE, DECODE_RETURN_TYPE, null,
          OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "NVL(value, value)" function. */
  public static final SqlFunction NVL =
      new SqlFunction("NVL", SqlKind.NVL,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE_ALL),
          null, OperandTypes.SAME_SAME, SqlFunctionCategory.SYSTEM);

  /** The "LTRIM(string)" function. */
  public static final SqlFunction LTRIM =
      new SqlFunction("LTRIM", SqlKind.LTRIM,
          ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE,
              SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "RTRIM(string)" function. */
  public static final SqlFunction RTRIM =
      new SqlFunction("RTRIM", SqlKind.RTRIM,
          ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE,
              SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "GREATEST(value, value)" function. */
  public static final SqlFunction GREATEST =
      new SqlFunction("GREATEST", SqlKind.GREATEST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "LEAST(value, value)" function. */
  public static final SqlFunction LEAST =
      new SqlFunction("LEAST", SqlKind.LEAST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /**
   * The <code>TRANSLATE(<i>string_expr</i>, <i>search_chars</i>, <i>replacement_chars</i>)</code>
   * function returns <i>string_expr</i> with all occurrences of each character in
   * <i>search_chars</i> replaced by its corresponding character in <i>replacement_chars</i>.
   *
   * <p>It is not defined in the SQL standard, but occurs in Oracle and PostgreSQL.
   */
  public static final SqlFunction TRANSLATE3 = new SqlTranslate3Function();

  /**
   * Returns the Oracle operator table, creating it if necessary.
   */
  public static synchronized OracleSqlOperatorTable instance() {
    if (instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      instance = new OracleSqlOperatorTable();
      instance.init();
    }
    return instance;
  }
}

// End OracleSqlOperatorTable.java