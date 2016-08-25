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
package uk.ac.soton.ldanalytics.sparql2fed.plan;

import org.apache.calcite.linq4j.tree.Expression;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelRoot;
import uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeField;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;

import java.util.List;

/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to
 * describe and implement itself.
 */
public interface RelOptTable {
  //~ Methods ----------------------------------------------------------------

  /**
   * Obtains an identifier for this table. The identifier must be unique with
   * respect to the Connection producing this table.
   *
   * @return qualified name
   */
  List<String> getQualifiedName();

  /**
   * Returns an estimate of the number of rows in the table.
   */
  double getRowCount();

  /**
   * Describes the type of rows returned by this table.
   */
  RelDataType getRowType();

  /**
   * Returns the {@link RelOptSchema} this table belongs to.
   */
  RelOptSchema getRelOptSchema();

  /**
   * Converts this table into a {@link RelNode relational expression}.
   *
   * <p>The {@link uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptPlanner planner} calls this
   * method to convert a table into an initial relational expression,
   * generally something abstract, such as a
   * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.logical.LogicalTableScan},
   * then optimizes this expression by
   * applying {@link uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptRule rules} to transform it
   * into more efficient access methods for this table.</p>
   */
  RelNode toRel(ToRelContext context);

  /**
   * Returns a description of the physical ordering (or orderings) of the rows
   * returned from this table.
   *
   * @see RelMetadataQuery#collations(RelNode)
   */
  List<RelCollation> getCollationList();

  /**
   * Returns a description of the physical distribution of the rows
   * in this table.
   *
   * @see RelMetadataQuery#distribution(RelNode)
   */
  RelDistribution getDistribution();

  /**
   * Returns whether the given columns are a key or a superset of a unique key
   * of this table.
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   */
  boolean isKey(ImmutableBitSet columns);

  /**
   * Finds an interface implemented by this table.
   */
  <T> T unwrap(Class<T> clazz);

  /**
   * Generates code for this table.
   *
   * @param clazz The desired collection class; for example {@code Queryable}.
   */
  Expression getExpression(Class clazz);

  /** Returns a table with the given extra fields. */
  RelOptTable extend(List<RelDataTypeField> extendedFields);

  /** Can expand a view into relational expressions. */
  interface ViewExpander {
    RelRoot expandView(RelDataType rowType, String queryString,
        List<String> schemaPath);
  }

  /** Contains the context needed to convert a a table into a relational
   * expression. */
  interface ToRelContext extends ViewExpander {
    RelOptCluster getCluster();
  }
}

// End RelOptTable.java
