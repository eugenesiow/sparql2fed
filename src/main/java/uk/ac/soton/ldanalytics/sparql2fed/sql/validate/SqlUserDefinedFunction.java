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
package uk.ac.soton.ldanalytics.sparql2fed.sql.validate;

import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlIdentifier;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandTypeChecker;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlReturnTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.collect.Lists;

import java.util.List;

/**
* User-defined scalar function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.</p>
*/
public class SqlUserDefinedFunction extends SqlFunction {
  public final Function function;

  public SqlUserDefinedFunction(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      Function function) {
    super(Util.last(opName.names), opName, SqlKind.OTHER_FUNCTION,
        returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.function = function;
  }

  /**
   * Returns function that implements given operator call.
   * @return function that implements given operator call
   */
  public Function getFunction() {
    return function;
  }

  @Override public List<String> getParamNames() {
    return Lists.transform(function.getParameters(), FunctionParameter.NAME_FN);
  }
}

// End SqlUserDefinedFunction.java
