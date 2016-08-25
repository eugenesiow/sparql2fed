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

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandTypeChecker;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlReturnTypeInference;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.validate.SqlValidatorScope;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static uk.ac.soton.ldanalytics.sparql2fed.util.Static.RESOURCE;

/**
 * A <code>SqlFunction</code> is a type of operator which has conventional
 * function-call syntax.
 */
public class SqlFunction extends SqlOperator {
  /** Function that generates "arg{n}" for the {@code n}th argument name. */
  private static final Function1<Integer, String> ARG_FN =
      new Function1<Integer, String>() {
        public String apply(Integer a0) {
          return "arg" + a0;
        }
      };

  //~ Instance fields --------------------------------------------------------

  private final SqlFunctionCategory category;

  private final SqlIdentifier sqlIdentifier;

  private final List<RelDataType> paramTypes;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new SqlFunction for a call to a builtin function.
   *
   * @param name                 Name of builtin function
   * @param kind                 kind of operator implemented by function
   * @param returnTypeInference  strategy to use for return type inference
   * @param operandTypeInference strategy to use for parameter type inference
   * @param operandTypeChecker   strategy to use for parameter type checking
   * @param category             categorization for function
   */
  public SqlFunction(
      String name,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    // We leave sqlIdentifier as null to indicate
    // that this is a builtin.  Same for paramTypes.
    this(name, null, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, null, category);

    assert !((category == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR)
        && (returnTypeInference == null));
  }

  /**
   * Creates a placeholder SqlFunction for an invocation of a function with a
   * possibly qualified name. This name must be resolved into either a builtin
   * function or a user-defined function.
   *
   * @param sqlIdentifier        possibly qualified identifier for function
   * @param returnTypeInference  strategy to use for return type inference
   * @param operandTypeInference strategy to use for parameter type inference
   * @param operandTypeChecker   strategy to use for parameter type checking
   * @param paramTypes           array of parameter types
   * @param funcType             function category
   */
  public SqlFunction(
      SqlIdentifier sqlIdentifier,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      SqlFunctionCategory funcType) {
    this(Util.last(sqlIdentifier.names), sqlIdentifier, SqlKind.OTHER_FUNCTION,
        returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, funcType);
  }

  /**
   * Internal constructor.
   */
  protected SqlFunction(
      String name,
      SqlIdentifier sqlIdentifier,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      SqlFunctionCategory category) {
    super(name, kind, 100, 100, returnTypeInference, operandTypeInference,
        operandTypeChecker);

    this.sqlIdentifier = sqlIdentifier;
    this.category = category;
    this.paramTypes =
        paramTypes == null ? null : ImmutableList.copyOf(paramTypes);
    assert category != null;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  /**
   * @return fully qualified name of function, or null for a builtin function
   */
  public SqlIdentifier getSqlIdentifier() {
    return sqlIdentifier;
  }

  @Override public SqlIdentifier getNameAsId() {
    if (sqlIdentifier != null) {
      return sqlIdentifier;
    }
    return super.getNameAsId();
  }

  /**
   * @return array of parameter types, or null for builtin function
   */
  public List<RelDataType> getParamTypes() {
    return paramTypes;
  }

  /**
   * Returns a list of parameter names.
   *
   * <p>The default implementation returns {@code [arg0, arg1, ..., argN]}.
   */
  public List<String> getParamNames() {
    return Functions.generate(paramTypes.size(), ARG_FN);
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
  }

  /**
   * @return function category
   */
  public SqlFunctionCategory getFunctionType() {
    return this.category;
  }

  /**
   * Returns whether this function allows a <code>DISTINCT</code> or <code>
   * ALL</code> quantifier. The default is <code>false</code>; some aggregate
   * functions return <code>true</code>.
   */
  public boolean isQuantifierAllowed() {
    return false;
  }

  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    // This implementation looks for the quantifier keywords DISTINCT or
    // ALL as the first operand in the list.  If found then the literal is
    // not called to validate itself.  Further the function is checked to
    // make sure that a quantifier is valid for that particular function.
    //
    // If the first operand does not appear to be a quantifier then the
    // parent ValidateCall is invoked to do normal function validation.

    super.validateCall(call, validator, scope, operandScope);
    validateQuantifier(validator, call);
  }

  /**
   * Throws a validation error if a DISTINCT or ALL quantifier is present but
   * not allowed.
   */
  protected void validateQuantifier(SqlValidator validator, SqlCall call) {
    if ((null != call.getFunctionQuantifier()) && !isQuantifierAllowed()) {
      throw validator.newValidationError(call.getFunctionQuantifier(),
          RESOURCE.functionQuantifierNotAllowed(call.getOperator().getName()));
    }
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    return deriveType(validator, scope, call, true);
  }

  private RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call,
      boolean convertRowArgToColumnList) {
    // Scope for operands. Usually the same as 'scope'.
    final SqlValidatorScope operandScope = scope.getOperandScope(call);

    // Indicate to the validator that we're validating a new function call
    validator.pushFunctionCall();

    final List<String> argNames = constructArgNameList(call);

    final List<SqlNode> args = constructOperandList(validator, call, argNames);

    final List<RelDataType> argTypes = constructArgTypeList(validator, scope,
        call, args, convertRowArgToColumnList);

    final SqlFunction function =
        (SqlFunction) SqlUtil.lookupRoutine(validator.getOperatorTable(),
            getNameAsId(), argTypes, argNames, getFunctionType(), SqlSyntax.FUNCTION, getKind()
        );
    try {
      // if we have a match on function name and parameter count, but
      // couldn't find a function with  a COLUMN_LIST type, retry, but
      // this time, don't convert the row argument to a COLUMN_LIST type;
      // if we did find a match, go back and re-validate the row operands
      // (corresponding to column references), now that we can set the
      // scope to that of the source cursor referenced by that ColumnList
      // type
      if (convertRowArgToColumnList && containsRowArg(args)) {
        if (function == null
            && SqlUtil.matchRoutinesByParameterCount(
                validator.getOperatorTable(), getNameAsId(), argTypes,
                getFunctionType())) {
          // remove the already validated node types corresponding to
          // row arguments before re-validating
          for (SqlNode operand : args) {
            if (operand.getKind() == SqlKind.ROW) {
              validator.removeValidatedNodeType(operand);
            }
          }
          return deriveType(validator, scope, call, false);
        } else if (function != null) {
          validator.validateColumnListParams(function, argTypes, args);
        }
      }

      if (getFunctionType() == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
        return validator.deriveConstructorType(scope, call, this, function,
            argTypes);
      }
      if (function == null) {
        throw validator.handleUnresolvedFunction(call, this, argTypes,
            argNames);
      }

      // REVIEW jvs 25-Mar-2005:  This is, in a sense, expanding
      // identifiers, but we ignore shouldExpandIdentifiers()
      // because otherwise later validation code will
      // choke on the unresolved function.
      ((SqlBasicCall) call).setOperator(function);
      return function.validateOperands(
          validator,
          operandScope,
          call);
    } finally {
      validator.popFunctionCall();
    }
  }

  private boolean containsRowArg(List<SqlNode> args) {
    for (SqlNode operand : args) {
      if (operand.getKind() == SqlKind.ROW) {
        return true;
      }
    }
    return false;
  }
}

// End SqlFunction.java
