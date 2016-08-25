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
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCall;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlIdentifier;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSpecialOperator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlWriter;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeName;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidatorScope;

import java.util.List;

/** Operator that returns the current or next value of a sequence. */
public class SqlSequenceValueOperator extends SqlSpecialOperator {
  /** Creates a SqlSequenceValueOperator. */
  SqlSequenceValueOperator(SqlKind kind) {
    super(kind.name(), kind, 100);
    assert kind == SqlKind.NEXT_VALUE || kind == SqlKind.CURRENT_VALUE;
  }

  @Override public boolean isDeterministic() {
    return false;
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    writer.sep(kind == SqlKind.NEXT_VALUE
        ? "NEXT VALUE FOR" : "CURRENT VALUE FOR");
    call.getOperandList().get(0).unparse(writer, 0, 0);
  }

  @Override public RelDataType deriveType(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), false);
  }

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    List<SqlNode> operands = call.getOperandList();
    assert operands.size() == 1;
    assert operands.get(0) instanceof SqlIdentifier;
    SqlIdentifier id = (SqlIdentifier) operands.get(0);
    validator.validateSequenceValue(scope, id);
  }
}

// End SqlSequenceValueOperator.java
