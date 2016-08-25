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
package uk.ac.soton.ldanalytics.sparql2fed.sql;


import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeFactory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ArraySqlType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.MapSqlType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.MultisetSqlType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandCountRanges;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeName;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;


/**
 * The <code>UNNEST</code> operator.
 */
public class SqlUnnestOperator extends SqlFunctionalOperator {
  /** Whether {@code WITH ORDINALITY} was specified.
   *
   * <p>If so, the returned records include a column {@code ORDINALITY}. */
  public final boolean withOrdinality;

  public static final String ORDINALITY_COLUMN_NAME = "ORDINALITY";

  public static final String MAP_KEY_COLUMN_NAME = "KEY";

  public static final String MAP_VALUE_COLUMN_NAME = "VALUE";

  //~ Constructors -----------------------------------------------------------

  public SqlUnnestOperator(boolean withOrdinality) {
    super(
        "UNNEST",
        SqlKind.UNNEST,
        200,
        true,
        null,
        null,
        OperandTypes.repeat(SqlOperandCountRanges.from(1),
            OperandTypes.SCALAR_OR_RECORD_COLLECTION_OR_MAP));
    this.withOrdinality = withOrdinality;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        opBinding.getTypeFactory().builder();
    for (Integer operand : Util.range(opBinding.getOperandCount())) {
      RelDataType type = opBinding.getOperandType(operand);
      if (type.isStruct()) {
        type = type.getFieldList().get(0).getType();
      }
      assert type instanceof ArraySqlType || type instanceof MultisetSqlType
          || type instanceof MapSqlType;
      if (type instanceof MapSqlType) {
        builder.add(MAP_KEY_COLUMN_NAME, type.getKeyType());
        builder.add(MAP_VALUE_COLUMN_NAME, type.getValueType());
      } else {
        if (type.getComponentType().isStruct()) {
          builder.addAll(type.getComponentType().getFieldList());
        } else {
          builder.add(SqlUtil.deriveAliasFromOrdinal(operand),
              type.getComponentType());
        }
      }
    }
    if (withOrdinality) {
      builder.add(ORDINALITY_COLUMN_NAME, SqlTypeName.INTEGER);
    }
    return builder.build();
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    super.unparse(writer, call, leftPrec, rightPrec);
    if (withOrdinality) {
      writer.keyword("WITH ORDINALITY");
    }
  }

  public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }

}

// End SqlUnnestOperator.java
