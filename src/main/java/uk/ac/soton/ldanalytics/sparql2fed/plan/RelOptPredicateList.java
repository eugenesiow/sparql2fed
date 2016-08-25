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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.google.common.collect.ImmutableList;

/**
 * Predicates that are known to hold in the output of a particular relational
 * expression.
 *
 * <p><b>Pulled up predicates</b> (field {@link #pulledUpPredicates} are
 * predicates that apply to every row output by the relational expression. They
 * are inferred from the input relational expression(s) and the relational
 * operator.
 *
 * <p>For example, if you apply {@code Filter(x > 1)} to a relational
 * expression that has a predicate {@code y < 10} then the pulled up predicates
 * for the Filter are {@code [y < 10, x > ]}.
 *
 * <p><b>Inferred predicates</b> only apply to joins. If there there is a
 * predicate on the left input to a join, and that predicate is over columns
 * used in the join condition, then a predicate can be inferred on the right
 * input to the join. (And vice versa.)
 *
 * <p>For example, in the query
 * <blockquote>SELECT *<br>
 * FROM emp<br>
 * JOIN dept ON emp.deptno = dept.deptno
 * WHERE emp.gender = 'F' AND emp.deptno &lt; 10</blockquote>
 * we have
 * <ul>
 *   <li>left: {@code Filter(Scan(EMP), deptno < 10},
 *       predicates: {@code [deptno < 10]}
 *   <li>right: {@code Scan(DEPT)}, predicates: {@code []}
 *   <li>join: {@code Join(left, right, emp.deptno = dept.deptno},
 *      leftInferredPredicates: [],
 *      rightInferredPredicates: [deptno &lt; 10],
 *      pulledUpPredicates: [emp.gender = 'F', emp.deptno &lt; 10,
 *      emp.deptno = dept.deptno, dept.deptno &lt; 10]
 * </ul>
 *
 * <p>Note that the predicate from the left input appears in
 * {@code rightInferredPredicates}. Predicates from several sources appear in
 * {@code pulledUpPredicates}.
 */
public class RelOptPredicateList {
  private static final ImmutableList<RexNode> EMPTY_LIST = ImmutableList.of();
  public static final RelOptPredicateList EMPTY =
      new RelOptPredicateList(EMPTY_LIST, EMPTY_LIST, EMPTY_LIST);

  /** Predicates that can be pulled up from the relational expression and its
   * inputs. */
  public final ImmutableList<RexNode> pulledUpPredicates;

  /** Predicates that were inferred from the right input.
   * Empty if the relational expression is not a join. */
  public final ImmutableList<RexNode> leftInferredPredicates;

  /** Predicates that were inferred from the left input.
   * Empty if the relational expression is not a join. */
  public final ImmutableList<RexNode> rightInferredPredicates;

  private RelOptPredicateList(Iterable<RexNode> pulledUpPredicates,
      Iterable<RexNode> leftInferredPredicates,
      Iterable<RexNode> rightInferredPredicates) {
    this.pulledUpPredicates = ImmutableList.copyOf(pulledUpPredicates);
    this.leftInferredPredicates = ImmutableList.copyOf(leftInferredPredicates);
    this.rightInferredPredicates =
        ImmutableList.copyOf(rightInferredPredicates);
  }

  /** Creates a RelOptPredicateList with only pulled-up predicates, no inferred
   * predicates.
   *
   * <p>Use this for relational expressions other than joins.
   *
   * @param pulledUpPredicates Predicates that apply to the rows returned by the
   * relational expression
   */
  public static RelOptPredicateList of(Iterable<RexNode> pulledUpPredicates) {
    ImmutableList<RexNode> pulledUpPredicatesList =
        ImmutableList.copyOf(pulledUpPredicates);
    if (pulledUpPredicatesList.isEmpty()) {
      return EMPTY;
    }
    return new RelOptPredicateList(pulledUpPredicatesList, EMPTY_LIST,
        EMPTY_LIST);
  }

  /** Creates a RelOptPredicateList for a join.
   *
   * @param pulledUpPredicates Predicates that apply to the rows returned by the
   * relational expression
   * @param leftInferredPredicates Predicates that were inferred from the right
   *                               input
   * @param rightInferredPredicates Predicates that were inferred from the left
   *                                input
   */
  public static RelOptPredicateList of(Iterable<RexNode> pulledUpPredicates,
      Iterable<RexNode> leftInferredPredicates,
      Iterable<RexNode> rightInferredPredicates) {
    final ImmutableList<RexNode> pulledUpPredicatesList =
        ImmutableList.copyOf(pulledUpPredicates);
    final ImmutableList<RexNode> leftInferredPredicateList =
        ImmutableList.copyOf(leftInferredPredicates);
    final ImmutableList<RexNode> rightInferredPredicatesList =
        ImmutableList.copyOf(rightInferredPredicates);
    if (pulledUpPredicatesList.isEmpty()
        && leftInferredPredicateList.isEmpty()
        && rightInferredPredicatesList.isEmpty()) {
      return EMPTY;
    }
    return new RelOptPredicateList(pulledUpPredicatesList,
        leftInferredPredicateList, rightInferredPredicatesList);
  }

  public RelOptPredicateList union(RelOptPredicateList list) {
    return RelOptPredicateList.of(
        concat(pulledUpPredicates, list.pulledUpPredicates),
        concat(leftInferredPredicates, list.leftInferredPredicates),
        concat(rightInferredPredicates, list.rightInferredPredicates));
  }

  /** Concatenates two immutable lists, avoiding a copy it possible. */
  private static <E> ImmutableList<E> concat(ImmutableList<E> list1,
      ImmutableList<E> list2) {
    if (list1.isEmpty()) {
      return list2;
    } else if (list2.isEmpty()) {
      return list1;
    } else {
      return ImmutableList.<E>builder().addAll(list1).addAll(list2).build();
    }
  }

  public RelOptPredicateList shift(int offset) {
    return RelOptPredicateList.of(RexUtil.shift(pulledUpPredicates, offset),
        RexUtil.shift(leftInferredPredicates, offset),
        RexUtil.shift(rightInferredPredicates, offset));
  }
}

// End RelOptPredicateList.java