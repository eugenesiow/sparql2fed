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
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlCallBinding;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlKind;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperandCountRange;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSpecialOperator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlWriter;
import uk.ac.soton.ldanalytics.sparql2fed.sql.parser.SqlParserPos;
import uk.ac.soton.ldanalytics.sparql2fed.sql.parser.SqlParserUtil;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.InferTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.OperandTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.ReturnTypes;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlOperandCountRanges;
import uk.ac.soton.ldanalytics.sparql2fed.sql.type.SqlTypeUtil;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import java.util.List;

/**
 * An operator describing the <code>LIKE</code> and <code>SIMILAR</code>
 * operators.
 *
 * <p>Syntax of the two operators:
 *
 * <ul>
 * <li><code>src-value [NOT] LIKE pattern-value [ESCAPE
 * escape-value]</code></li>
 * <li><code>src-value [NOT] SIMILAR pattern-value [ESCAPE
 * escape-value]</code></li>
 * </ul>
 *
 * <p><b>NOTE</b> If the <code>NOT</code> clause is present the
 * {@link uk.ac.soton.ldanalytics.sparql2fed.sql.parser.SqlParser parser} will generate a
 * equivalent to <code>NOT (src LIKE pattern ...)</code>
 */
public class SqlLikeOperator extends SqlSpecialOperator {
  //~ Instance fields --------------------------------------------------------

  private final boolean negated;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlLikeOperator.
   *
   * @param name    Operator name
   * @param kind    Kind
   * @param negated Whether this is 'NOT LIKE'
   */
  SqlLikeOperator(
      String name,
      SqlKind kind,
      boolean negated) {
    // LIKE is right-associative, because that makes it easier to capture
    // dangling ESCAPE clauses: "a like b like c escape d" becomes
    // "a like (b like c escape d)".
    super(
        name,
        kind,
        30,
        false,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_SAME_SAME_SAME);
    this.negated = negated;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns whether this is the 'NOT LIKE' operator.
   *
   * @return whether this is 'NOT LIKE'
   */
  public boolean isNegated() {
    return negated;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    switch (callBinding.getOperandCount()) {
    case 2:
      if (!OperandTypes.STRING_SAME_SAME.checkOperandTypes(
          callBinding,
          throwOnFailure)) {
        return false;
      }
      break;
    case 3:
      if (!OperandTypes.STRING_SAME_SAME_SAME.checkOperandTypes(
          callBinding,
          throwOnFailure)) {
        return false;
      }

      // calc implementation should
      // enforce the escape character length to be 1
      break;
    default:
      throw Util.newInternal(
          "unexpected number of args to " + callBinding.getCall());
    }

    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        callBinding.operands(),
        throwOnFailure);
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("", "");
    call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
    writer.sep(getName());

    call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
    if (call.operandCount() == 3) {
      writer.sep("ESCAPE");
      call.operand(2).unparse(writer, getLeftPrec(), getRightPrec());
    }
    writer.endList(frame);
  }

  public int reduceExpr(
      final int opOrdinal,
      List<Object> list) {
    // Example:
    //   a LIKE b || c ESCAPE d || e AND f
    // |  |    |      |      |      |
    //  exp0    exp1          exp2
    SqlNode exp0 = (SqlNode) list.get(opOrdinal - 1);
    SqlOperator op =
        ((SqlParserUtil.ToTreeListItem) list.get(opOrdinal)).getOperator();
    assert op instanceof SqlLikeOperator;
    SqlNode exp1 =
        SqlParserUtil.toTreeEx(
            list,
            opOrdinal + 1,
            getRightPrec(),
            SqlKind.ESCAPE);
    SqlNode exp2 = null;
    if ((opOrdinal + 2) < list.size()) {
      final Object o = list.get(opOrdinal + 2);
      if (o instanceof SqlParserUtil.ToTreeListItem) {
        final SqlOperator op2 =
            ((SqlParserUtil.ToTreeListItem) o).getOperator();
        if (op2.getKind() == SqlKind.ESCAPE) {
          exp2 =
              SqlParserUtil.toTreeEx(
                  list,
                  opOrdinal + 3,
                  getRightPrec(),
                  SqlKind.ESCAPE);
        }
      }
    }
    final SqlNode[] operands;
    int end;
    if (exp2 != null) {
      operands = new SqlNode[]{exp0, exp1, exp2};
      end = opOrdinal + 4;
    } else {
      operands = new SqlNode[]{exp0, exp1};
      end = opOrdinal + 2;
    }
    SqlCall call = createCall(SqlParserPos.ZERO, operands);
    SqlParserUtil.replaceSublist(list, opOrdinal - 1, end, call);
    return opOrdinal - 1;
  }
}

// End SqlLikeOperator.java