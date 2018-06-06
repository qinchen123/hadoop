/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

@Private
@Unstable
public class GPUResourceCalculator extends ResourceCalculator {

  @Override
  public int compare(Resource unused, Resource lhs, Resource rhs, boolean singleType) {
    // Only consider GPU
    return lhs.getGPUs() - rhs.getGPUs();
  }

  @Override
  public long computeAvailableContainers(Resource available, Resource required) {
    // Only consider GPU
    if(!isInvalidDivisor(required)) {
        return available.getGPUs() / required.getGPUs();
    }
    else {
        return available.getGPUs();
    }
  }

  @Override
  public float divide(Resource unused,
      Resource numerator, Resource denominator) {
    return ratio(numerator, denominator);
  }

  public boolean isInvalidDivisor(Resource r) {
    if (r.getGPUs() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
      if(!isInvalidDivisor(b)) {
        return (float)a.getGPUs() / b.getGPUs();
      }
      else {
          return (float)a.getGPUs();
      }
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return divideAndCeil(numerator, (float)denominator);
  }

  @Override
  public Resource divideAndCeil(Resource numerator, float denominator) {
    return Resources.createResource(
        numerator.getMemorySize(),
        numerator.getVirtualCores(),
        divideAndCeil(numerator.getGPUs(), denominator),
        numerator.getGPUAttribute()
    );
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    int normalizedGPU = Math.min(
        roundUp(
            Math.max(r.getGPUs(), minimumResource.getGPUs()),
            stepFactor.getGPUs()),
            maximumResource.getGPUs());
    return Resources.createResource(
            r.getMemorySize(),
            r.getVirtualCores(),
            normalizedGPU,
            r.getGPUAttribute()
            );
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        r.getMemorySize(),
        r.getVirtualCores(),
        roundUp(r.getGPUs(), stepFactor.getGPUs()),
        r.getGPUAttribute()
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
         r.getMemorySize(),
         r.getVirtualCores(),
         roundDown(r.getGPUs(), stepFactor.getGPUs()),
         r.getGPUAttribute()
        );
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        r.getMemorySize(),
        r.getVirtualCores(),
        roundUp((int)(r.getGPUs() * by + 0.5), stepFactor.getGPUs()),
        r.getGPUAttribute()
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        r.getMemorySize(),
        r.getVirtualCores(),
        roundDown(
            (int)(r.getGPUs() * by),
            stepFactor.getGPUs()
            ),
        r.getGPUAttribute()
        );
  }

  @Override
  public boolean fitsIn(Resource cluster,
      Resource smaller, Resource bigger) {

    boolean fitsIn = smaller.getMemorySize() <= bigger.getMemorySize() &&
        smaller.getVirtualCores() <= bigger.getVirtualCores() &&
        smaller.getGPUs() <= bigger.getGPUs();
    if (fitsIn) {
      if((smaller.getGPUAttribute() & bigger.getGPUAttribute()) != smaller.getGPUAttribute()) {
        fitsIn = false;
      }
      if (fitsIn) {
        if (smaller.getPorts() != null && !(smaller.getPorts().isLessOrEqual(bigger.getPorts()))) {
          fitsIn = false;
        }
      }
    }
    return fitsIn;
  }

  @Override
  public boolean isAnyMajorResourceZero(Resource resource) {
    return resource.getGPUs() == 0;
  }

}
