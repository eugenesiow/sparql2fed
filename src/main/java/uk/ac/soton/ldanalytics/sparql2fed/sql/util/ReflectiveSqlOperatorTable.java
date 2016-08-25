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
package uk.ac.soton.ldanalytics.sparql2fed.sql.util;

import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunction;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlFunctionCategory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlIdentifier;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperator;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlOperatorTable;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSyntax;
import uk.ac.soton.ldanalytics.sparql2fed.util.Pair;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable} interface
 * by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {
  public static final String IS_NAME = "INFORMATION_SCHEMA";

  //~ Instance fields --------------------------------------------------------

  private final Multimap<Key, SqlOperator> operators = HashMultimap.create();

  //~ Constructors -----------------------------------------------------------

  protected ReflectiveSqlOperatorTable() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Performs post-constructor initialization of an operator table. It can't
   * be part of the constructor, because the subclass constructor needs to
   * complete first.
   */
  public final void init() {
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null) {
            register(op);
          }
        } else if (
            SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          register(op);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  // implement SqlOperatorTable
  public void lookupOperatorOverloads(SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList) {
    // NOTE jvs 3-Mar-2005:  ignore category until someone cares

    String simpleName;
    if (opName.names.size() > 1) {
      if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {
        // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
        simpleName = Util.last(opName.names);
      } else {
        return;
      }
    } else {
      simpleName = opName.getSimple();
    }

    // Always look up built-in operators case-insensitively. Even in sessions
    // with unquotedCasing=UNCHANGED and caseSensitive=true.
    final Collection<SqlOperator> list =
        operators.get(new Key(simpleName, syntax));
    if (list.isEmpty()) {
      return;
    }
    for (SqlOperator op : list) {
      if (op.getSyntax() == syntax) {
        operatorList.add(op);
      } else if (syntax == SqlSyntax.FUNCTION
          && op instanceof SqlFunction) {
        // this special case is needed for operators like CAST,
        // which are treated as functions but have special syntax
        operatorList.add(op);
      }
    }

    // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
    // Shouldn't it be covered by search above?
    switch (syntax) {
    case BINARY:
    case PREFIX:
    case POSTFIX:
      for (SqlOperator extra : operators.get(new Key(simpleName, syntax))) {
        // REVIEW: should only search operators added during this method?
        if (extra != null && !operatorList.contains(extra)) {
          operatorList.add(extra);
        }
      }
      break;
    }
  }

  /**
   * Registers a function or operator in the table.
   */
  public void register(SqlOperator op) {
    operators.put(new Key(op.getName(), op.getSyntax()), op);
    if (op instanceof SqlFunction) {
      SqlFunction function = (SqlFunction) op;
      SqlFunctionCategory funcType = function.getFunctionType();
      assert funcType != null
          : "Function type for " + function.getName() + " not set";
    }
  }

  public List<SqlOperator> getOperatorList() {
    return ImmutableList.copyOf(operators.values());
  }

  /** Key for looking up operators. The name is stored in upper-case because we
   * store case-insensitively, even in a case-sensitive session. */
  private static class Key extends Pair<String, SqlSyntax> {
    Key(String name, SqlSyntax syntax) {
      super(name.toUpperCase(), normalize(syntax));
    }

    private static SqlSyntax normalize(SqlSyntax syntax) {
      switch (syntax) {
      case BINARY:
      case PREFIX:
      case POSTFIX:
        return syntax;
      default:
        return SqlSyntax.FUNCTION;
      }
    }
  }
}

// End ReflectiveSqlOperatorTable.java
