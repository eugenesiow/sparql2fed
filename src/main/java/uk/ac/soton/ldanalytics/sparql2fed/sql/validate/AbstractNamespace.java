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
import uk.ac.soton.ldanalytics.sparql2fed.rel.type.RelDataTypeFactory;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlNode;
import uk.ac.soton.ldanalytics.sparql2fed.util.Pair;
import uk.ac.soton.ldanalytics.sparql2fed.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Abstract implementation of {@link SqlValidatorNamespace}.
 */
abstract class AbstractNamespace implements SqlValidatorNamespace {
  //~ Instance fields --------------------------------------------------------

  protected final SqlValidatorImpl validator;

  /**
   * Whether this scope is currently being validated. Used to check for
   * cycles.
   */
  private SqlValidatorImpl.Status status =
      SqlValidatorImpl.Status.UNVALIDATED;

  /**
   * Type of the output row, which comprises the name and type of each output
   * column. Set on validate.
   */
  protected RelDataType rowType;

  /** As {@link #rowType}, but not necessarily a struct. */
  protected RelDataType type;

  private boolean forceNullable;

  protected final SqlNode enclosingNode;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AbstractNamespace.
   *
   * @param validator     Validator
   * @param enclosingNode Enclosing node
   */
  AbstractNamespace(
      SqlValidatorImpl validator,
      SqlNode enclosingNode) {
    this.validator = validator;
    this.enclosingNode = enclosingNode;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlValidator getValidator() {
    return validator;
  }

  public final void validate(RelDataType targetRowType) {
    switch (status) {
    case UNVALIDATED:
      try {
        status = SqlValidatorImpl.Status.IN_PROGRESS;
        Util.permAssert(
            rowType == null,
            "Namespace.rowType must be null before validate has been called");
        RelDataType type = validateImpl(targetRowType);
        Util.permAssert(
            type != null,
            "validateImpl() returned null");
        if (forceNullable) {
          // REVIEW jvs 10-Oct-2005: This may not be quite right
          // if it means that nullability will be forced in the
          // ON clause where it doesn't belong.
          type =
              validator.getTypeFactory().createTypeWithNullability(
                  type,
                  true);
        }
        setType(type);
      } finally {
        status = SqlValidatorImpl.Status.VALID;
      }
      break;
    case IN_PROGRESS:
      throw Util.newInternal("todo: Cycle detected during type-checking");
    case VALID:
      break;
    default:
      throw Util.unexpected(status);
    }
  }

  /**
   * Validates this scope and returns the type of the records it returns.
   * External users should call {@link #validate}, which uses the
   * {@link #status} field to protect against cycles.
   *
   * @return record data type, never null
   *
   * @param targetRowType Desired row type, must not be null, may be the data
   *                      type 'unknown'.
   */
  protected abstract RelDataType validateImpl(RelDataType targetRowType);

  public RelDataType getRowType() {
    if (rowType == null) {
      validator.validateNamespace(this, validator.unknownType);
      Util.permAssert(rowType != null, "validate must set rowType");
    }
    return rowType;
  }

  public RelDataType getRowTypeSansSystemColumns() {
    return getRowType();
  }

  public RelDataType getType() {
    Util.discard(getRowType());
    return type;
  }

  public void setType(RelDataType type) {
    this.type = type;
    this.rowType = convertToStruct(type);
  }

  public SqlNode getEnclosingNode() {
    return enclosingNode;
  }

  public SqlValidatorTable getTable() {
    return null;
  }

  public SqlValidatorNamespace lookupChild(String name) {
    return validator.lookupFieldNamespace(
        getRowType(),
        name);
  }

  public boolean fieldExists(String name) {
    final RelDataType rowType = getRowType();
    return validator.catalogReader.field(rowType, name) != null;
  }

  public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs() {
    return ImmutableList.of();
  }

  public SqlMonotonicity getMonotonicity(String columnName) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  public void makeNullable() {
    forceNullable = true;
  }

  public String translate(String name) {
    return name;
  }

  public SqlValidatorNamespace resolve() {
    return this;
  }

  public boolean supportsModality(SqlModality modality) {
    return true;
  }

  public <T> T unwrap(Class<T> clazz) {
    return clazz.cast(this);
  }

  public boolean isWrapperFor(Class<?> clazz) {
    return clazz.isInstance(this);
  }

  protected RelDataType convertToStruct(RelDataType type) {
    // "MULTISET [<expr>, ...]" needs to be wrapped in a record if
    // <expr> has a scalar type.
    // For example, "MULTISET [8, 9]" has type
    // "RECORD(INTEGER EXPR$0 NOT NULL) NOT NULL MULTISET NOT NULL".
    final RelDataType componentType = type.getComponentType();
    if (componentType == null || componentType.isStruct()) {
      return type;
    }
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final RelDataType structType = toStruct(componentType, getNode());
    final RelDataType collectionType;
    switch (type.getSqlTypeName()) {
    case ARRAY:
      collectionType = typeFactory.createArrayType(structType, -1);
      break;
    case MULTISET:
      collectionType = typeFactory.createMultisetType(structType, -1);
      break;
    default:
      throw new AssertionError(type);
    }
    return typeFactory.createTypeWithNullability(collectionType,
        type.isNullable());
  }

  /** Converts a type to a struct if it is not already. */
  protected RelDataType toStruct(RelDataType type, SqlNode unnest) {
    if (type.isStruct()) {
      return type;
    }
    return validator.getTypeFactory().builder()
        .add(validator.deriveAlias(unnest, 0), type)
        .build();
  }
}

// End AbstractNamespace.java
