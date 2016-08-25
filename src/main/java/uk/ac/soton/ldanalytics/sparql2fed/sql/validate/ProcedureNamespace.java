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
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCall;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCallBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeName;

/**
 * Namespace whose contents are defined by the result of a call to a
 * user-defined procedure.
 */
public class ProcedureNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlValidatorScope scope;

  private final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  ProcedureNamespace(
      SqlValidatorImpl validator,
      SqlValidatorScope scope,
      SqlCall call,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.scope = scope;
    this.call = call;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType validateImpl(RelDataType targetRowType) {
    validator.inferUnknownTypes(validator.unknownType, scope, call);
    final RelDataType type = validator.deriveTypeImpl(scope, call);
    final SqlOperator operator = call.getOperator();
    final SqlCallBinding callBinding =
        new SqlCallBinding(validator, scope, call);
    if (operator instanceof SqlUserDefinedTableFunction) {
      assert type.getSqlTypeName() == SqlTypeName.CURSOR
          : "User-defined table function should have CURSOR type, not " + type;
      final SqlUserDefinedTableFunction udf =
          (SqlUserDefinedTableFunction) operator;
      return udf.getRowType(validator.typeFactory, callBinding.operands());
    } else if (operator instanceof SqlUserDefinedTableMacro) {
      assert type.getSqlTypeName() == SqlTypeName.CURSOR
          : "User-defined table macro should have CURSOR type, not " + type;
      final SqlUserDefinedTableMacro udf =
          (SqlUserDefinedTableMacro) operator;
      return udf.getTable(validator.typeFactory, callBinding.operands())
          .getRowType(validator.typeFactory);
    }
    return type;
  }

  public SqlNode getNode() {
    return call;
  }
}

// End ProcedureNamespace.java