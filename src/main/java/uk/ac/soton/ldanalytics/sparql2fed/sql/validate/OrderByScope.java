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
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeField;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlIdentifier;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNodeList;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlSelect;

import java.util.List;

/**
 * Represents the name-resolution context for expressions in an ORDER BY clause.
 *
 * <p>In some dialects of SQL, the ORDER BY clause can reference column aliases
 * in the SELECT clause. For example, the query</p>
 *
 * <blockquote><code>SELECT empno AS x<br>
 * FROM emp<br>
 * ORDER BY x</code></blockquote>
 *
 * <p>is valid.</p>
 */
public class OrderByScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlNodeList orderList;
  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  OrderByScope(
      SqlValidatorScope parent,
      SqlNodeList orderList,
      SqlSelect select) {
    super(parent);
    this.orderList = orderList;
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return orderList;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
    final SqlValidatorNamespace ns = validator.getNamespace(select);
    addColumnNames(ns, result);
  }

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    // If it's a simple identifier, look for an alias.
    if (identifier.isSimple()
        && validator.getConformance().isSortByAlias()) {
      String name = identifier.names.get(0);
      final SqlValidatorNamespace selectNs =
          validator.getNamespace(select);
      final RelDataType rowType = selectNs.getRowType();

      final RelDataTypeField field = validator.catalogReader.field(rowType, name);
      if (field != null && !field.isDynamicStar()) {
        // if identifier is resolved to a dynamic star, use super.fullyQualify() for such case.
        return SqlQualified.create(this, 1, selectNs, identifier);
      }
    }
    return super.fullyQualify(identifier);
  }

  public RelDataType resolveColumn(String name, SqlNode ctx) {
    final SqlValidatorNamespace selectNs = validator.getNamespace(select);
    final RelDataType rowType = selectNs.getRowType();
    final RelDataTypeField field = validator.catalogReader.field(rowType, name);
    if (field != null) {
      return field.getType();
    }
    final SqlValidatorScope selectScope = validator.getSelectScope(select);
    return selectScope.resolveColumn(name, ctx);
  }

  public void validateExpr(SqlNode expr) {
    SqlNode expanded = validator.expandOrderExpr(select, expr);

    // expression needs to be valid in parent scope too
    parent.validateExpr(expanded);
  }
}

// End OrderByScope.java
