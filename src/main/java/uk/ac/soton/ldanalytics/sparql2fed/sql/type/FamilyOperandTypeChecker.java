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
package uk.ac.soton.ldanalytics.sparql2fed.sql.type;

import org.apache.calcite.linq4j.Ord;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCallBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperandCountRange;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlUtil;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static uk.ac.soton.ldanalytics.sparql2fed.util.Static.RESOURCE;

/**
 * Operand type-checking strategy which checks operands for inclusion in type
 * families.
 */
public class FamilyOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<SqlTypeFamily> families;
  protected final Predicate<Integer> optional;

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Create using {@link OperandTypes#family}.
   */
  FamilyOperandTypeChecker(List<SqlTypeFamily> families,
      Predicate<Integer> optional) {
    this.families = ImmutableList.copyOf(families);
    this.optional = optional;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean isOptional(int i) {
    return optional.apply(i);
  }

  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {
    SqlTypeFamily family = families.get(iFormalOperand);
    if (family == SqlTypeFamily.ANY) {
      // no need to check
      return true;
    }
    if (SqlUtil.isNullLiteral(node, false)) {
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(node,
            RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }
    RelDataType type =
        callBinding.getValidator().deriveType(
            callBinding.getScope(),
            node);
    SqlTypeName typeName = type.getSqlTypeName();

    // Pass type checking for operators if it's of type 'ANY'.
    if (typeName.getFamily() == SqlTypeFamily.ANY) {
      return true;
    }

    if (!family.getTypeNames().contains(typeName)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw
      return false;
    }

    for (Ord<SqlNode> op : Ord.zip(callBinding.operands())) {
      if (!checkSingleOperandType(
          callBinding,
          op.e,
          op.i,
          throwOnFailure)) {
        return false;
      }
    }
    return true;
  }

  public SqlOperandCountRange getOperandCountRange() {
    final int max = families.size();
    int min = max;
    while (min > 0 && optional.apply(min - 1)) {
      --min;
    }
    return SqlOperandCountRanges.between(min, max);
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName, families);
  }

  public Consistency getConsistency() {
    return Consistency.NONE;
  }
}

// End FamilyOperandTypeChecker.java
