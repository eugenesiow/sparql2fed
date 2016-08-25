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
package uk.ac.soton.ldanalytics.sparql2fed.rel.metadata;

import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptCost;
import uk.ac.soton.ldanalytics.sparql2fed.plan.RelOptPredicateList;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelCollation;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelDistribution;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlExplainLevel;
import uk.ac.soton.ldanalytics.sparql2fed.util.BuiltInMethod;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

/**
 * Contains the interfaces for several common forms of metadata.
 */
public abstract class BuiltInMetadata {

  /** Metadata about the selectivity of a predicate. */
  public interface Selectivity extends Metadata {
    MetadataDef<Selectivity> DEF = MetadataDef.of(Selectivity.class,
        Selectivity.Handler.class, BuiltInMethod.SELECTIVITY.method);

    /**
     * Estimates the percentage of an expression's output rows which satisfy a
     * given predicate. Returns null to indicate that no reliable estimate can
     * be produced.
     *
     * @param predicate predicate whose selectivity is to be estimated against
     *                  rel's output
     * @return estimated selectivity (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    Double getSelectivity(RexNode predicate);

    /** Handler API. */
    interface Handler extends MetadataHandler<Selectivity> {
      Double getSelectivity(RelNode r, RelMetadataQuery mq, RexNode predicate);
    }
  }

  /** Metadata about which combinations of columns are unique identifiers. */
  public interface UniqueKeys extends Metadata {
    MetadataDef<UniqueKeys> DEF = MetadataDef.of(UniqueKeys.class,
        UniqueKeys.Handler.class, BuiltInMethod.UNIQUE_KEYS.method);

    /**
     * Determines the set of unique minimal keys for this expression. A key is
     * represented as an {@link uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableBitSet}, where
     * each bit position represents a 0-based output column ordinal.
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param ignoreNulls if true, ignore null values when determining
     *                    whether the keys are unique
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all)
     */
    Set<ImmutableBitSet> getUniqueKeys(boolean ignoreNulls);

    /** Handler API. */
    interface Handler extends MetadataHandler<UniqueKeys> {
      Set<ImmutableBitSet> getUniqueKeys(RelNode r, RelMetadataQuery mq,
          boolean ignoreNulls);
    }
  }

  /** Metadata about whether a set of columns uniquely identifies a row. */
  public interface ColumnUniqueness extends Metadata {
    MetadataDef<ColumnUniqueness> DEF = MetadataDef.of(ColumnUniqueness.class,
        ColumnUniqueness.Handler.class, BuiltInMethod.COLUMN_UNIQUENESS.method);

    /**
     * Determines whether a specified set of columns from a specified relational
     * expression are unique.
     *
     * <p>For example, if the relational expression is a {@code TableScan} to
     * T(A, B, C, D) whose key is (A, B), then:
     * <ul>
     * <li>{@code areColumnsUnique([0, 1])} yields true,
     * <li>{@code areColumnsUnique([0])} yields false,
     * <li>{@code areColumnsUnique([0, 2])} yields false.
     * </ul>
     *
     * <p>Nulls can be ignored if the relational expression has filtered out
     * null values.
     *
     * @param columns column mask representing the subset of columns for which
     *                uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column
     *                    uniqueness
     * @return whether the columns are unique, or
     * null if not enough information is available to make that determination
     */
    Boolean areColumnsUnique(ImmutableBitSet columns, boolean ignoreNulls);

    /** Handler API. */
    interface Handler extends MetadataHandler<ColumnUniqueness> {
      Boolean areColumnsUnique(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet columns, boolean ignoreNulls);
    }
  }

  /** Metadata about which columns are sorted. */
  public interface Collation extends Metadata {
    MetadataDef<Collation> DEF = MetadataDef.of(Collation.class,
        Collation.Handler.class, BuiltInMethod.COLLATIONS.method);

    /** Determines which columns are sorted. */
    ImmutableList<RelCollation> collations();

    /** Handler API. */
    interface Handler extends MetadataHandler<Collation> {
      ImmutableList<RelCollation> collations(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about how a relational expression is distributed.
   *
   * <p>If you are an operator consuming a relational expression, which subset
   * of the rows are you seeing? You might be seeing all of them (BROADCAST
   * or SINGLETON), only those whose key column values have a particular hash
   * code (HASH) or only those whose column values have particular values or
   * ranges of values (RANGE).
   *
   * <p>When a relational expression is partitioned, it is often partitioned
   * among nodes, but it may be partitioned among threads running on the same
   * node. */
  public interface Distribution extends Metadata {
    MetadataDef<Distribution> DEF = MetadataDef.of(Distribution.class,
        Distribution.Handler.class, BuiltInMethod.DISTRIBUTION.method);

    /** Determines how the rows are distributed. */
    RelDistribution distribution();

    /** Handler API. */
    interface Handler extends MetadataHandler<Distribution> {
      RelDistribution distribution(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the number of rows returned by a relational expression. */
  public interface RowCount extends Metadata {
    MetadataDef<RowCount> DEF = MetadataDef.of(RowCount.class,
        RowCount.Handler.class, BuiltInMethod.ROW_COUNT.method);

    /**
     * Estimates the number of rows which will be returned by a relational
     * expression. The default implementation for this query asks the rel itself
     * via {@link RelNode#estimateRowCount}, but metadata providers can override this
     * with their own cost models.
     *
     * @return estimated row count, or null if no reliable estimate can be
     * determined
     */
    Double getRowCount();

    /** Handler API. */
    interface Handler extends MetadataHandler<RowCount> {
      Double getRowCount(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the maximum number of rows returned by a relational
   * expression. */
  public interface MaxRowCount extends Metadata {
    MetadataDef<MaxRowCount> DEF = MetadataDef.of(MaxRowCount.class,
        MaxRowCount.Handler.class, BuiltInMethod.MAX_ROW_COUNT.method);

    /**
     * Estimates the max number of rows which will be returned by a relational
     * expression.
     *
     * <p>The default implementation for this query returns
     * {@link Double#POSITIVE_INFINITY},
     * but metadata providers can override this with their own cost models.
     *
     * @return upper bound on the number of rows returned
     */
    Double getMaxRowCount();

    /** Handler API. */
    interface Handler extends MetadataHandler<MaxRowCount> {
      Double getMaxRowCount(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the number of distinct rows returned by a set of columns
   * in a relational expression. */
  public interface DistinctRowCount extends Metadata {
    MetadataDef<DistinctRowCount> DEF = MetadataDef.of(DistinctRowCount.class,
        DistinctRowCount.Handler.class, BuiltInMethod.DISTINCT_ROW_COUNT.method);

    /**
     * Estimates the number of rows which would be produced by a GROUP BY on the
     * set of columns indicated by groupKey, where the input to the GROUP BY has
     * been pre-filtered by predicate. This quantity (leaving out predicate) is
     * often referred to as cardinality (as in gender being a "low-cardinality
     * column").
     *
     * @param groupKey  column mask representing group by columns
     * @param predicate pre-filtered predicates
     * @return distinct row count for groupKey, filtered by predicate, or null
     * if no reliable estimate can be determined
     */
    Double getDistinctRowCount(ImmutableBitSet groupKey, RexNode predicate);

    /** Handler API. */
    interface Handler extends MetadataHandler<DistinctRowCount> {
      Double getDistinctRowCount(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet groupKey, RexNode predicate);
    }
  }

  /** Metadata about the proportion of original rows that remain in a relational
   * expression. */
  public interface PercentageOriginalRows extends Metadata {
    MetadataDef<PercentageOriginalRows> DEF =
        MetadataDef.of(PercentageOriginalRows.class,
            PercentageOriginalRows.Handler.class,
            BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method);

    /**
     * Estimates the percentage of the number of rows actually produced by a
     * relational expression out of the number of rows it would produce if all
     * single-table filter conditions were removed.
     *
     * @return estimated percentage (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    Double getPercentageOriginalRows();

    /** Handler API. */
    interface Handler extends MetadataHandler<PercentageOriginalRows> {
      Double getPercentageOriginalRows(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the number of distinct values in the original source of a
   * column or set of columns. */
  public interface PopulationSize extends Metadata {
    MetadataDef<PopulationSize> DEF = MetadataDef.of(PopulationSize.class,
        PopulationSize.Handler.class, BuiltInMethod.POPULATION_SIZE.method);

    /**
     * Estimates the distinct row count in the original source for the given
     * {@code groupKey}, ignoring any filtering being applied by the expression.
     * Typically, "original source" means base table, but for derived columns,
     * the estimate may come from a non-leaf rel such as a LogicalProject.
     *
     * @param groupKey column mask representing the subset of columns for which
     *                 the row count will be determined
     * @return distinct row count for the given groupKey, or null if no reliable
     * estimate can be determined
     */
    Double getPopulationSize(ImmutableBitSet groupKey);

    /** Handler API. */
    interface Handler extends MetadataHandler<PopulationSize> {
      Double getPopulationSize(RelNode r, RelMetadataQuery mq,
          ImmutableBitSet groupKey);
    }
  }

  /** Metadata about the size of rows and columns. */
  public interface Size extends Metadata {
    MetadataDef<Size> DEF = MetadataDef.of(Size.class, Size.Handler.class,
        BuiltInMethod.AVERAGE_ROW_SIZE.method,
        BuiltInMethod.AVERAGE_COLUMN_SIZES.method);

    /**
     * Determines the average size (in bytes) of a row from this relational
     * expression.
     *
     * @return average size of a row, in bytes, or null if not known
     */
    Double averageRowSize();

    /**
     * Determines the average size (in bytes) of a value of a column in this
     * relational expression.
     *
     * <p>Null values are included (presumably they occupy close to 0 bytes).
     *
     * <p>It is left to the caller to decide whether the size is the compressed
     * size, the uncompressed size, or memory allocation when the value is
     * wrapped in an object in the Java heap. The uncompressed size is probably
     * a good compromise.
     *
     * @return an immutable list containing, for each column, the average size
     * of a column value, in bytes. Each value or the entire list may be null if
     * the metadata is not available
     */
    List<Double> averageColumnSizes();

    /** Handler API. */
    interface Handler extends MetadataHandler<Size> {
      Double averageRowSize(RelNode r, RelMetadataQuery mq);
      List<Double> averageColumnSizes(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the origins of columns. */
  public interface ColumnOrigin extends Metadata {
    MetadataDef<ColumnOrigin> DEF = MetadataDef.of(ColumnOrigin.class,
        ColumnOrigin.Handler.class, BuiltInMethod.COLUMN_ORIGIN.method);

    /**
     * For a given output column of an expression, determines all columns of
     * underlying tables which contribute to result values. An output column may
     * have more than one origin due to expressions such as Union and
     * LogicalProject. The optimizer may use this information for catalog access
     * (e.g. index availability).
     *
     * @param outputColumn 0-based ordinal for output column of interest
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates definitely no origin columns at
     * all)
     */
    Set<RelColumnOrigin> getColumnOrigins(int outputColumn);

    /** Handler API. */
    interface Handler extends MetadataHandler<ColumnOrigin> {
      Set<RelColumnOrigin> getColumnOrigins(RelNode r, RelMetadataQuery mq,
          int outputColumn);
    }
  }

  /** Metadata about the cost of evaluating a relational expression, including
   * all of its inputs. */
  public interface CumulativeCost extends Metadata {
    MetadataDef<CumulativeCost> DEF = MetadataDef.of(CumulativeCost.class,
        CumulativeCost.Handler.class, BuiltInMethod.CUMULATIVE_COST.method);

    /**
     * Estimates the cost of executing a relational expression, including the
     * cost of its inputs. The default implementation for this query adds
     * {@link NonCumulativeCost#getNonCumulativeCost} to the cumulative cost of
     * each input, but metadata providers can override this with their own cost
     * models, e.g. to take into account interactions between expressions.
     *
     * @return estimated cost, or null if no reliable estimate can be
     * determined
     */
    RelOptCost getCumulativeCost();

    /** Handler API. */
    interface Handler extends MetadataHandler<CumulativeCost> {
      RelOptCost getCumulativeCost(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the cost of evaluating a relational expression, not
   * including its inputs. */
  public interface NonCumulativeCost extends Metadata {
    MetadataDef<NonCumulativeCost> DEF = MetadataDef.of(NonCumulativeCost.class,
        NonCumulativeCost.Handler.class,
        BuiltInMethod.NON_CUMULATIVE_COST.method);

    /**
     * Estimates the cost of executing a relational expression, not counting the
     * cost of its inputs. (However, the non-cumulative cost is still usually
     * dependent on the row counts of the inputs.) The default implementation
     * for this query asks the rel itself via {@link RelNode#computeSelfCost},
     * but metadata providers can override this with their own cost models.
     *
     * @return estimated cost, or null if no reliable estimate can be
     * determined
     */
    RelOptCost getNonCumulativeCost();

    /** Handler API. */
    interface Handler extends MetadataHandler<NonCumulativeCost> {
      RelOptCost getNonCumulativeCost(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about whether a relational expression should appear in a plan. */
  public interface ExplainVisibility extends Metadata {
    MetadataDef<ExplainVisibility> DEF = MetadataDef.of(ExplainVisibility.class,
        ExplainVisibility.Handler.class,
        BuiltInMethod.EXPLAIN_VISIBILITY.method);

    /**
     * Determines whether a relational expression should be visible in EXPLAIN
     * PLAN output at a particular level of detail.
     *
     * @param explainLevel level of detail
     * @return true for visible, false for invisible
     */
    Boolean isVisibleInExplain(SqlExplainLevel explainLevel);

    /** Handler API. */
    interface Handler extends MetadataHandler<ExplainVisibility> {
      Boolean isVisibleInExplain(RelNode r, RelMetadataQuery mq,
          SqlExplainLevel explainLevel);
    }
  }

  /** Metadata about the predicates that hold in the rows emitted from a
   * relational expression. */
  public interface Predicates extends Metadata {
    MetadataDef<Predicates> DEF = MetadataDef.of(Predicates.class,
        Predicates.Handler.class, BuiltInMethod.PREDICATES.method);

    /**
     * Derives the predicates that hold on rows emitted from a relational
     * expression.
     *
     * @return Predicate list
     */
    RelOptPredicateList getPredicates();

    /** Handler API. */
    interface Handler extends MetadataHandler<Predicates> {
      RelOptPredicateList getPredicates(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the degree of parallelism of a relational expression, and
   * how its operators are assigned to processes with independent resource
   * pools. */
  public interface Parallelism extends Metadata {
    MetadataDef<Parallelism> DEF = MetadataDef.of(Parallelism.class,
        Parallelism.Handler.class, BuiltInMethod.IS_PHASE_TRANSITION.method,
        BuiltInMethod.SPLIT_COUNT.method);

    /** Returns whether each physical operator implementing this relational
     * expression belongs to a different process than its inputs.
     *
     * <p>A collection of operators processing all of the splits of a particular
     * stage in the query pipeline is called a "phase". A phase starts with
     * a leaf node such as a {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableScan},
     * or with a phase-change node such as an
     * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Exchange}. Hadoop's shuffle operator
     * (a form of sort-exchange) causes data to be sent across the network. */
    Boolean isPhaseTransition();

    /** Returns the number of distinct splits of the data.
     *
     * <p>Note that splits must be distinct. For broadcast, where each copy is
     * the same, returns 1.
     *
     * <p>Thus the split count is the <em>proportion</em> of the data seen by
     * each operator instance.
     */
    Integer splitCount();

    /** Handler API. */
    interface Handler extends MetadataHandler<Parallelism> {
      Boolean isPhaseTransition(RelNode r, RelMetadataQuery mq);
      Integer splitCount(RelNode r, RelMetadataQuery mq);
    }
  }

  /** Metadata about the memory use of an operator. */
  public interface Memory extends Metadata {
    MetadataDef<Memory> DEF = MetadataDef.of(Memory.class,
        Memory.Handler.class, BuiltInMethod.MEMORY.method,
        BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
        BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method);

    /** Returns the expected amount of memory, in bytes, required by a physical
     * operator implementing this relational expression, across all splits.
     *
     * <p>How much memory is used depends very much on the algorithm; for
     * example, an implementation of
     * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate} that loads all data into a
     * hash table requires approximately {@code rowCount * averageRowSize}
     * bytes, whereas an implementation that assumes that the input is sorted
     * requires only {@code averageRowSize} bytes to maintain a single
     * accumulator for each aggregate function.
     */
    Double memory();

    /** Returns the cumulative amount of memory, in bytes, required by the
     * physical operator implementing this relational expression, and all other
     * operators within the same phase, across all splits.
     *
     * @see Parallelism#splitCount()
     */
    Double cumulativeMemoryWithinPhase();

    /** Returns the expected cumulative amount of memory, in bytes, required by
     * the physical operator implementing this relational expression, and all
     * operators within the same phase, within each split.
     *
     * <p>Basic formula:
     *
     * <blockquote>cumulativeMemoryWithinPhaseSplit
     *     = cumulativeMemoryWithinPhase / Parallelism.splitCount</blockquote>
     */
    Double cumulativeMemoryWithinPhaseSplit();

    /** Handler API. */
    interface Handler extends MetadataHandler<Memory> {
      Double memory(RelNode r, RelMetadataQuery mq);
      Double cumulativeMemoryWithinPhase(RelNode r, RelMetadataQuery mq);
      Double cumulativeMemoryWithinPhaseSplit(RelNode r, RelMetadataQuery mq);
    }
  }

  /** The built-in forms of metadata. */
  interface All extends Selectivity, UniqueKeys, RowCount, DistinctRowCount,
      PercentageOriginalRows, ColumnUniqueness, ColumnOrigin, Predicates,
      Collation, Distribution, Size, Parallelism, Memory {
  }
}

// End BuiltInMetadata.java
