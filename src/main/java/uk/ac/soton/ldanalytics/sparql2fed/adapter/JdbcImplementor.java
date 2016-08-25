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
package uk.ac.soton.ldanalytics.sparql2fed.adapter;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.rel2sql.RelToSqlConverter;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlDialect;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import java.util.Collections;

/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor extends RelToSqlConverter {
  public JdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    super(dialect);
    Util.discard(typeFactory);
  }

  /** @see #dispatch */
  public Result visit(JdbcTableScan scan) {
    return result(scan.jdbcTable.tableName(),
        Collections.singletonList(Clause.FROM), scan);
  }

  public Result implement(RelNode node) {
    return dispatch(node);
  }
}

// End JdbcImplementor.java
