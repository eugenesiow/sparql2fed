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
import uk.ac.soton.ldanalytics.sparql2fed.util.BuiltInMethod;

/**
 * Default implementations of the
 * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.BuiltInMetadata.Memory}
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery#isPhaseTransition
 * @see RelMetadataQuery#splitCount
 */
public class RelMdMemory implements MetadataHandler<BuiltInMetadata.Memory> {
  /** Source for
   * {@link uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.BuiltInMetadata.Memory}. */
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new RelMdMemory(),
          BuiltInMethod.MEMORY.method,
          BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
          BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method);

  //~ Constructors -----------------------------------------------------------

  protected RelMdMemory() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.Memory> getDef() {
    return BuiltInMetadata.Memory.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Memory#memory()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#memory
   */
  public Double memory(RelNode rel, RelMetadataQuery mq) {
    return null;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhase()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#memory
   */
  public Double cumulativeMemoryWithinPhase(RelNode rel, RelMetadataQuery mq) {
    Double nullable = mq.memory(rel);
    if (nullable == null) {
      return null;
    }
    Boolean isPhaseTransition = mq.isPhaseTransition(rel);
    if (isPhaseTransition == null) {
      return null;
    }
    double d = nullable;
    if (!isPhaseTransition) {
      for (RelNode input : rel.getInputs()) {
        nullable = mq.cumulativeMemoryWithinPhase(input);
        if (nullable == null) {
          return null;
        }
        d += nullable;
      }
    }
    return d;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhaseSplit()},
   * invoked using reflection.
   *
   * @see uk.ac.soton.ldanalytics.sparql2fed.rel.metadata.RelMetadataQuery#cumulativeMemoryWithinPhaseSplit
   */
  public Double cumulativeMemoryWithinPhaseSplit(RelNode rel,
      RelMetadataQuery mq) {
    final Double memoryWithinPhase = mq.cumulativeMemoryWithinPhase(rel);
    final Integer splitCount = mq.splitCount(rel);
    if (memoryWithinPhase == null || splitCount == null) {
      return null;
    }
    return memoryWithinPhase / splitCount;
  }
}

// End RelMdMemory.java
