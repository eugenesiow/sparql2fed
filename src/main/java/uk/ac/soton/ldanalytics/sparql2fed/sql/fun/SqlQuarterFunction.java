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

import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCallBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperandCountRange;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.InferTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandCountRanges;

/**
 * SqlQuarterFunction represents the SQL:1999 standard {@code QUARTER}
 * function. Determines Quarter (1,2,3,4) of a given date.
 */
public class SqlQuarterFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlQuarterFunction() {
    super("QUARTER",
        SqlKind.OTHER,
        ReturnTypes.BIGINT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.DATETIME,
        SqlFunctionCategory.TIMEDATE);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  public String getSignatureTemplate(int operandsCount) {
    assert 1 == operandsCount;
    return "{0}({1})";
  }

  public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return OperandTypes.DATETIME.checkSingleOperandType(callBinding,
        callBinding.operand(0), 0, throwOnFailure);
  }
}

// End SqlQuarterFunction.java
