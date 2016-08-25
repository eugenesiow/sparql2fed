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

import uk.ac.soton.ldanalytics.sparql2fed.rel.RelNode;
import uk.ac.soton.ldanalytics.sparql2fed.sql.SqlExplainLevel;
import uk.ac.soton.ldanalytics.sparql2fed.util.BuiltInMethod;

/**
 * RelMdExplainVisibility supplies a default implementation of
 * {@link RelMetadataQuery#isVisibleInExplain} for the standard logical algebra.
 */
public class RelMdExplainVisibility
    implements MetadataHandler<BuiltInMetadata.ExplainVisibility> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.EXPLAIN_VISIBILITY.method,
          new RelMdExplainVisibility());

  //~ Constructors -----------------------------------------------------------

  private RelMdExplainVisibility() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.ExplainVisibility> getDef() {
    return BuiltInMetadata.ExplainVisibility.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.ExplainVisibility#isVisibleInExplain(SqlExplainLevel)},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#isVisibleInExplain(RelNode, SqlExplainLevel)
   */
  public Boolean isVisibleInExplain(RelNode rel, RelMetadataQuery mq,
      SqlExplainLevel explainLevel) {
    // no information available
    return null;
  }
}

// End RelMdExplainVisibility.java
