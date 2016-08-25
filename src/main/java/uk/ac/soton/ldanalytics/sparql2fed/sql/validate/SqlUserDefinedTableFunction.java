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
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TableFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlIdentifier;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandTypeChecker;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlReturnTypeInference;

import java.lang.reflect.Type;
import java.util.List;

/**
 * User-defined table function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
*/
public class SqlUserDefinedTableFunction extends SqlUserDefinedFunction {
  public SqlUserDefinedTableFunction(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      TableFunction function) {
    super(opName, returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, function);
  }

  /**
   * Returns function that implements given operator call.
   * @return function that implements given operator call
   */
  public TableFunction getFunction() {
    return (TableFunction) super.getFunction();
  }

  /**
   * Returns the record type of the table yielded by this function when
   * applied to given arguments. Only literal arguments are passed,
   * non-literal are replaced with default values (null, 0, false, etc).
   *
   * @param typeFactory Type factory
   * @param operandList arguments of a function call (only literal arguments
   *                    are passed, nulls for non-literal ones)
   * @return row type of the table
   */
  public RelDataType getRowType(RelDataTypeFactory typeFactory,
      List<SqlNode> operandList) {
    List<Object> arguments =
      SqlUserDefinedTableMacro.convertArguments(typeFactory, operandList,
        function, getNameAsId(), false);
    return getFunction().getRowType(typeFactory, arguments);
  }

  /**
   * Returns the row type of the table yielded by this function when
   * applied to given arguments. Only literal arguments are passed,
   * non-literal are replaced with default values (null, 0, false, etc).
   *
   * @param operandList arguments of a function call (only literal arguments
   *                    are passed, nulls for non-literal ones)
   * @return element type of the table (e.g. {@code Object[].class})
   */
  public Type getElementType(RelDataTypeFactory typeFactory,
      List<SqlNode> operandList) {
    List<Object> arguments =
      SqlUserDefinedTableMacro.convertArguments(typeFactory, operandList,
        function, getNameAsId(), false);
    return getFunction().getElementType(arguments);
  }
}

// End SqlUserDefinedTableFunction.java