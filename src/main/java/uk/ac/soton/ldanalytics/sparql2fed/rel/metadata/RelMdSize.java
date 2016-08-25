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

import org.apache.calcite.avatica.util.ByteString;
import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Aggregate;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.AggregateCall;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Exchange;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Filter;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Intersect;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Join;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Minus;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Project;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.SemiJoin;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Sort;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableScan;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Union;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Values;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataType;
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import uk.ac.soton.ldanalytics.sparql2fed.util.BuiltInMethod;
import uk.ac.soton.ldanalytics.sparql2fed.util.ImmutableNullableList;
import uk.ac.soton.ldanalytics.sparql2fed.util.NlsString;
import uk.ac.soton.ldanalytics.sparql2fed.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Default implementations of the
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.BuiltInMetadata.Size}
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery#getAverageRowSize
 * @see RelMetadataQuery#getAverageColumnSizes
 * @see RelMetadataQuery#getAverageColumnSizesNotNull
 */
public class RelMdSize implements MetadataHandler<BuiltInMetadata.Size> {
  /** Source for
   * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.BuiltInMetadata.Size}. */
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new RelMdSize(),
          BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
          BuiltInMethod.AVERAGE_ROW_SIZE.method);

  /** Bytes per character (2). */
  public static final int BYTES_PER_CHARACTER = Character.SIZE / Byte.SIZE;

  //~ Constructors -----------------------------------------------------------

  protected RelMdSize() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.Size> getDef() {
    return BuiltInMetadata.Size.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Size#averageRowSize()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#getAverageRowSize
   */
  public Double averageRowSize(RelNode rel, RelMetadataQuery mq) {
    final List<Double> averageColumnSizes = mq.getAverageColumnSizes(rel);
    if (averageColumnSizes == null) {
      return null;
    }
    Double d = 0d;
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    for (Pair<Double, RelDataTypeField> p
        : Pair.zip(averageColumnSizes, fields)) {
      if (p.left == null) {
        d += averageFieldValueSize(p.right);
      } else {
        d += p.left;
      }
    }
    return d;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Size#averageColumnSizes()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#getAverageColumnSizes
   */
  public List<Double> averageColumnSizes(RelNode rel, RelMetadataQuery mq) {
    return null; // absolutely no idea
  }

  public List<Double> averageColumnSizes(Filter rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public List<Double> averageColumnSizes(Sort rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public List<Double> averageColumnSizes(Exchange rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput());
  }

  public List<Double> averageColumnSizes(Project rel, RelMetadataQuery mq) {
    final List<Double> inputColumnSizes =
        mq.getAverageColumnSizesNotNull(rel.getInput());
    final ImmutableNullableList.Builder<Double> sizes =
        ImmutableNullableList.builder();
    for (RexNode project : rel.getProjects()) {
      sizes.add(averageRexSize(project, inputColumnSizes));
    }
    return sizes.build();
  }

  public List<Double> averageColumnSizes(Values rel, RelMetadataQuery mq) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final ImmutableList.Builder<Double> list = ImmutableList.builder();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      double d;
      if (rel.getTuples().isEmpty()) {
        d = averageTypeValueSize(field.getType());
      } else {
        d = 0;
        for (ImmutableList<RexLiteral> literals : rel.getTuples()) {
          d += typeValueSize(field.getType(), literals.get(i).getValue());
        }
        d /= rel.getTuples().size();
      }
      list.add(d);
    }
    return list.build();
  }

  public List<Double> averageColumnSizes(TableScan rel, RelMetadataQuery mq) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final ImmutableList.Builder<Double> list = ImmutableList.builder();
    for (RelDataTypeField field : fields) {
      list.add(averageTypeValueSize(field.getType()));
    }
    return list.build();
  }

  public List<Double> averageColumnSizes(Aggregate rel, RelMetadataQuery mq) {
    final List<Double> inputColumnSizes =
        mq.getAverageColumnSizesNotNull(rel.getInput());
    final ImmutableList.Builder<Double> list = ImmutableList.builder();
    for (int key : rel.getGroupSet()) {
      list.add(inputColumnSizes.get(key));
    }
    for (AggregateCall aggregateCall : rel.getAggCallList()) {
      list.add(averageTypeValueSize(aggregateCall.type));
    }
    return list.build();
  }

  public List<Double> averageColumnSizes(SemiJoin rel, RelMetadataQuery mq) {
    return averageJoinColumnSizes(rel, mq, true);
  }

  public List<Double> averageColumnSizes(Join rel, RelMetadataQuery mq) {
    return averageJoinColumnSizes(rel, mq, false);
  }

  private List<Double> averageJoinColumnSizes(Join rel, RelMetadataQuery mq,
      boolean semijoin) {
    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();
    final List<Double> lefts = mq.getAverageColumnSizes(left);
    final List<Double> rights =
        semijoin ? null : mq.getAverageColumnSizes(right);
    if (lefts == null && rights == null) {
      return null;
    }
    final int fieldCount = rel.getRowType().getFieldCount();
    Double[] sizes = new Double[fieldCount];
    if (lefts != null) {
      lefts.toArray(sizes);
    }
    if (rights != null) {
      final int leftCount = left.getRowType().getFieldCount();
      for (int i = 0; i < rights.size(); i++) {
        sizes[leftCount + i] = rights.get(i);
      }
    }
    return ImmutableNullableList.copyOf(sizes);
  }

  public List<Double> averageColumnSizes(Intersect rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput(0));
  }

  public List<Double> averageColumnSizes(Minus rel, RelMetadataQuery mq) {
    return mq.getAverageColumnSizes(rel.getInput(0));
  }

  public List<Double> averageColumnSizes(Union rel, RelMetadataQuery mq) {
    final int fieldCount = rel.getRowType().getFieldCount();
    List<List<Double>> inputColumnSizeList = Lists.newArrayList();
    for (RelNode input : rel.getInputs()) {
      final List<Double> inputSizes = mq.getAverageColumnSizes(input);
      if (inputSizes != null) {
        inputColumnSizeList.add(inputSizes);
      }
    }
    switch (inputColumnSizeList.size()) {
    case 0:
      return null; // all were null
    case 1:
      return inputColumnSizeList.get(0); // all but one were null
    }
    final ImmutableNullableList.Builder<Double> sizes =
        ImmutableNullableList.builder();
    int nn = 0;
    for (int i = 0; i < fieldCount; i++) {
      double d = 0d;
      int n = 0;
      for (List<Double> inputColumnSizes : inputColumnSizeList) {
        Double d2 = inputColumnSizes.get(i);
        if (d2 != null) {
          d += d2;
          ++n;
          ++nn;
        }
      }
      sizes.add(n > 0 ? d / n : null);
    }
    if (nn == 0) {
      return null; // all columns are null
    }
    return sizes.build();
  }

  /** Estimates the average size (in bytes) of a value of a field, knowing
   * nothing more than its type.
   *
   * <p>We assume that the proportion of nulls is negligible, even if the field
   * is nullable.
   */
  protected Double averageFieldValueSize(RelDataTypeField field) {
    return averageTypeValueSize(field.getType());
  }

  /** Estimates the average size (in bytes) of a value of a type.
   *
   * <p>We assume that the proportion of nulls is negligible, even if the type
   * is nullable.
   */
  public Double averageTypeValueSize(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case REAL:
    case DECIMAL:
    case DATE:
    case TIME:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case FLOAT: // sic
    case TIMESTAMP:
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return 8d;
    case BINARY:
      return (double) type.getPrecision();
    case VARBINARY:
      return Math.min((double) type.getPrecision(), 100d);
    case CHAR:
      return (double) type.getPrecision() * BYTES_PER_CHARACTER;
    case VARCHAR:
      // Even in large (say VARCHAR(2000)) columns most strings are small
      return Math.min((double) type.getPrecision() * BYTES_PER_CHARACTER, 100d);
    case ROW:
      Double average = 0.0;
      for (RelDataTypeField field : type.getFieldList()) {
        average += averageTypeValueSize(field.getType());
      }
      return average;
    default:
      return null;
    }
  }

  /** Estimates the average size (in bytes) of a value of a type.
   *
   * <p>Nulls count as 1 byte.
   */
  public double typeValueSize(RelDataType type, Comparable value) {
    if (value == null) {
      return 1d;
    }
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case FLOAT:
    case REAL:
    case DATE:
    case TIME:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case TIMESTAMP:
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return 8d;
    case BINARY:
    case VARBINARY:
      return ((ByteString) value).length();
    case CHAR:
    case VARCHAR:
      return ((NlsString) value).getValue().length() * BYTES_PER_CHARACTER;
    default:
      return 32;
    }
  }

  public Double averageRexSize(RexNode node, List<Double> inputColumnSizes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return inputColumnSizes.get(((RexInputRef) node).getIndex());
    case LITERAL:
      return typeValueSize(node.getType(), ((RexLiteral) node).getValue());
    default:
      if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        for (RexNode operand : call.getOperands()) {
          // It's a reasonable assumption that a function's result will have
          // similar size to its argument of a similar type. For example,
          // UPPER(c) has the same average size as c.
          if (operand.getType().getSqlTypeName()
              == node.getType().getSqlTypeName()) {
            return averageRexSize(operand, inputColumnSizes);
          }
        }
      }
      return averageTypeValueSize(node.getType());
    }
  }
}

// End RelMdSize.java
