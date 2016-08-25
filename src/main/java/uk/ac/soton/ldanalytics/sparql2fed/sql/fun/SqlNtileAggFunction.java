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

import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlAggFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;

/**
 * <code>NTILE</code> aggregate function
 * return the value of given expression evaluated at given offset.
 */
public class SqlNtileAggFunction extends SqlAggFunction {
  public SqlNtileAggFunction() {
    super(
        "NTILE",
        null,
        SqlKind.NTILE,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.POSITIVE_INTEGER_LITERAL,
        SqlFunctionCategory.NUMERIC,
        false,
        true);
  }

}

// End SqlNtileAggFunction.java
