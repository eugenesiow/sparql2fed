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

import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperatorBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSyntax;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlMonotonicity;

/**
 * Base class for functions such as "USER", "CURRENT_ROLE", and "CURRENT_PATH".
 */
public class SqlStringContextVariable extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  protected SqlStringContextVariable(String name) {
    super(
        name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_ID;
  }

  // All of the string constants are monotonic.
  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlMonotonicity.CONSTANT;
  }

  // Plans referencing context variables should never be cached
  public boolean isDynamicFunction() {
    return true;
  }
}

// End SqlStringContextVariable.java
