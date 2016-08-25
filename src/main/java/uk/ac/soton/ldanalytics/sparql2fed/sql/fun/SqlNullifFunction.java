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

import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCall;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlLiteral;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNodeList;
import uk.ac.soton.ldanalytics.sparql2fed.sql.parser.SqlParserPos;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidator;

import java.util.List;

/**
 * The <code>NULLIF</code> function.
 */
public class SqlNullifFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlNullifFunction() {
    // NOTE jvs 26-July-2006:  We fill in the type strategies here,
    // but normally they are not used because the validator invokes
    // rewriteCall to convert NULLIF into CASE early.  However,
    // validator rewrite can optionally be disabled, in which case these
    // strategies are used.
    super(
        "NULLIF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_FORCE_NULLABLE,
        null,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  // override SqlOperator
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    List<SqlNode> operands = call.getOperandList();
    SqlParserPos pos = call.getParserPosition();

    checkOperandCount(
        validator,
        getOperandTypeChecker(),
        call);
    assert operands.size() == 2;

    SqlNodeList whenList = new SqlNodeList(pos);
    SqlNodeList thenList = new SqlNodeList(pos);
    whenList.add(operands.get(1));
    thenList.add(SqlLiteral.createNull(SqlParserPos.ZERO));
    return SqlCase.createSwitched(
        pos, operands.get(0), whenList, thenList, operands.get(0).clone(
        operands.get(0).getParserPosition()));
  }
}

// End SqlNullifFunction.java
