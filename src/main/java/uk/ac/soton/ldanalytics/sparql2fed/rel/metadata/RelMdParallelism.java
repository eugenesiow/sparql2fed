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
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Exchange;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.TableScan;
import uk.ac.soton.ldanalytics.sparql2fed.rel.core.Values;
import uk.ac.soton.ldanalytics.sparql2fed.util.BuiltInMethod;

/**
 * Default implementations of the
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.BuiltInMetadata.Parallelism}
 * metadata provider for the standard logical algebra.
 *
 * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#isPhaseTransition
 * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#splitCount
 */
public class RelMdParallelism
    implements MetadataHandler<BuiltInMetadata.Parallelism> {
  /** Source for
   * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.BuiltInMetadata.Parallelism}. */
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new RelMdParallelism(),
          BuiltInMethod.IS_PHASE_TRANSITION.method,
          BuiltInMethod.SPLIT_COUNT.method);

  //~ Constructors -----------------------------------------------------------

  protected RelMdParallelism() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.Parallelism> getDef() {
    return BuiltInMetadata.Parallelism.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Parallelism#isPhaseTransition()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#isPhaseTransition
   */
  public Boolean isPhaseTransition(RelNode rel, RelMetadataQuery mq) {
    return false;
  }

  public Boolean isPhaseTransition(TableScan rel, RelMetadataQuery mq) {
    return true;
  }

  public Boolean isPhaseTransition(Values rel, RelMetadataQuery mq) {
    return true;
  }

  public Boolean isPhaseTransition(Exchange rel, RelMetadataQuery mq) {
    return true;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Parallelism#splitCount()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#splitCount
   */
  public Integer splitCount(RelNode rel, RelMetadataQuery mq) {
    return 1;
  }
}

// End RelMdParallelism.java
