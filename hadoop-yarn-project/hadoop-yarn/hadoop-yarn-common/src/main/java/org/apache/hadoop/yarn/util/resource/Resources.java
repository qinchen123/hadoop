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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;
import sun.awt.SunHints;

import java.util.List;
import java.util.ArrayList;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
@Unstable
public class Resources {

  private static final Log LOG = LogFactory
      .getLog(Resources.class);
  // Java doesn't have const :(
  private static final Resource NONE = new Resource() {

    @Override
    @SuppressWarnings("deprecation")
    public int getMemory() {
      return 0;
    }

    @Override
    public long getMemorySize() {
      return 0;
    }

    @Override
    public void setMemorySize(long memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return 0;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getGPUs() {
      return 0;
    }

    @Override
    public void setGPUs(int GPUs) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public long getGPUAttribute() {
      return 0;
    }

    @Override
    public void setGPUAttribute(long GPUAttribute) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    public ValueRanges getPorts() {
      return null;
    }

    @Override
    public void setPorts(ValueRanges port) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      long diff = 0 - o.getMemorySize();
      if (diff == 0) {
        diff = 0 - o.getVirtualCores();
        if (diff == 0) {
          diff = 0 - o.getGPUs();
        }
      }
      return Long.signum(diff);
    }
    
  };
  
  private static final Resource UNBOUNDED = new Resource() {

    @Override
    @SuppressWarnings("deprecation")
    public int getMemory() {
      return Integer.MAX_VALUE;
    }

    @Override
    public long getMemorySize() {
      return Long.MAX_VALUE;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setMemory(int memory) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public void setMemorySize(long memory) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public int getVirtualCores() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setVirtualCores(int cores) {
      throw new RuntimeException("UNBOUNDED cannot be modified!");
    }

    @Override
    public int getGPUs() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void setGPUs(int GPUs) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public long getGPUAttribute() {
      return Long.MAX_VALUE;
    }

    @Override
    public void setGPUAttribute(long GPUAttribute) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public ValueRanges getPorts() {
      return null;
    }

    @Override
    public void setPorts(ValueRanges port) {
      throw new RuntimeException("NONE cannot be modified!");
    }


    @Override
    public int compareTo(Resource o) {
      long diff = Long.MAX_VALUE - o.getMemorySize();
      if (diff == 0) {
        diff = Integer.MAX_VALUE - o.getVirtualCores();
        if (diff == 0) {
          diff = 0 - o.getGPUs();
        }
      }
      return Long.signum(diff);
    }
  };

  public static Resource createResource(int memory) {
    return createResource(memory, (memory > 0) ? 1 : 0, 0);
  }

  public static Resource createResource(int memory, int cores) {
    return Resource.newInstance(memory, cores);
  }

  public static Resource createResource(long memory) {
    return createResource(memory, (memory > 0) ? 1 : 0, 0);
  }

  public static Resource createResource(long memory, int cores) {
    return Resource.newInstance(memory, cores);
  }

  public static Resource createResource(long memory, int cores, int GPUs) {
    return createResource(memory, cores, GPUs, 0);
  }

  public static Resource createResource(long memory, int cores, int GPUs, long GPUAttribute) {
    return createResource(memory, cores, GPUs, GPUAttribute, null);
  }

  public static Resource createResource(long memory, int cores, int GPUs, long GPUAttribute, ValueRanges ports) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemorySize(memory);
    resource.setVirtualCores(cores);
    resource.setGPUs(GPUs);
    resource.setGPUAttribute(GPUAttribute);
    resource.setPorts(ports);
    return resource;
  }


  public static Resource none() {
    return NONE;
  }

  /**
   * Check whether a resource object is empty (0 memory and 0 virtual cores).
   * @param other The resource to check
   * @return {@code true} if {@code other} has 0 memory and 0 virtual cores,
   * {@code false} otherwise
   */
  public static boolean isNone(Resource other) {
    return NONE.equals(other);
  }
  
  public static Resource unbounded() {
    return UNBOUNDED;
  }  

  public static Resource clone(Resource res) {
    return createResource(res.getMemorySize(), res.getVirtualCores(), res.getGPUs(), res.getGPUAttribute(), res.getPorts());
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    lhs.setMemorySize(lhs.getMemorySize() + rhs.getMemorySize());
    lhs.setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores());
    lhs.setGPUs(lhs.getGPUs() + rhs.getGPUs());

    if ( (lhs.getGPUAttribute() & rhs.getGPUAttribute()) != 0) {
      //LOG.warn("Resource.addTo: lhs GPU attribute is " +
      //    lhs.getGPUAttribute() + "; rhs GPU attribute is " + rhs.getGPUAttribute());
    } else {
      lhs.setGPUAttribute(lhs.getGPUAttribute() | rhs.getGPUAttribute());
    }

    if (lhs.getPorts() != null) {
      lhs.setPorts(lhs.getPorts().addSelf(rhs.getPorts()));
    } else {
      lhs.setPorts(rhs.getPorts());
    }
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    lhs.setMemorySize(lhs.getMemorySize() - rhs.getMemorySize());
    lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
    lhs.setGPUs(lhs.getGPUs() - rhs.getGPUs());

    if ( (lhs.getGPUAttribute() | rhs.getGPUAttribute()) != lhs.getGPUAttribute()) {
      //LOG.warn("Resource.subtractFrom: lhs GPU attribute is " +
      //   lhs.getGPUAttribute() + "; rhs GPU attribute is " + rhs.getGPUAttribute());
    } else {
      lhs.setGPUAttribute(lhs.getGPUAttribute() & ~rhs.getGPUAttribute());
    }

    if (lhs.getPorts() != null) {
      lhs.setPorts(lhs.getPorts().minusSelf(rhs.getPorts()));
    }
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  /**
   * Subtract <code>rhs</code> from <code>lhs</code> and reset any negative
   * values to zero.
   * @param lhs {@link Resource} to subtract from
   * @param rhs {@link Resource} to subtract
   * @return the value of lhs after subtraction
   */
  public static Resource subtractFromNonNegative(Resource lhs, Resource rhs) {
    subtractFrom(lhs, rhs);
    if (lhs.getMemorySize() < 0) {
      lhs.setMemorySize(0);
    }
    if (lhs.getVirtualCores() < 0) {
      lhs.setVirtualCores(0);
    }

    if (lhs.getGPUs() < 0) {
      lhs.setGPUs(0);
    }

    return lhs;
  }

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, double by) {
    lhs.setMemorySize((long)(lhs.getMemorySize() * by));
    lhs.setVirtualCores((int)(lhs.getVirtualCores() * by));
    lhs.setGPUs((int)(lhs.getGPUs() * by));
    return lhs;
  }

  public static Resource multiply(Resource lhs, double by) {
    return multiplyTo(clone(lhs), by);
  }

  /**
   * Multiply {@code rhs} by {@code by}, and add the result to {@code lhs}
   * without creating any new {@link Resource} object
   */
  public static Resource multiplyAndAddTo(
      Resource lhs, Resource rhs, double by) {
    lhs.setMemorySize(lhs.getMemorySize() + (long)(rhs.getMemorySize() * by));
    lhs.setVirtualCores(lhs.getVirtualCores()
        + (int)(rhs.getVirtualCores() * by));
    lhs.setGPUs(lhs.getGPUs()
        + (int)(rhs.getGPUs() * by));
    return lhs;
  }

  public static Resource multiplyAndNormalizeUp(
      ResourceCalculator calculator, Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }

  public static Resource multiplyAndNormalizeDown(
      ResourceCalculator calculator,Resource lhs, double by, Resource factor) {
    return calculator.multiplyAndNormalizeDown(lhs, by, factor);
  }
  
  public static Resource multiplyAndRoundDown(Resource lhs, double by) {
    Resource out = clone(lhs);
    out.setMemorySize((long)(lhs.getMemorySize() * by));
    out.setVirtualCores((int)(lhs.getVirtualCores() * by));
    out.setGPUs((int)(lhs.getGPUs() * by));
    return out;
  }

  public static Resource multiplyAndRoundUp(Resource lhs, double by) {
    Resource out = clone(lhs);
    out.setMemorySize((long)Math.ceil(lhs.getMemorySize() * by));
    out.setVirtualCores((int)Math.ceil(lhs.getVirtualCores() * by));
    out.setGPUs((int)Math.ceil(lhs.getGPUs() * by));
    return out;
  }
  
  public static Resource normalize(
      ResourceCalculator calculator, Resource lhs, Resource min,
      Resource max, Resource increment) {
    return calculator.normalize(lhs, min, max, increment);
  }
  
  public static Resource roundUp(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundUp(lhs, factor);
  }
  
  public static Resource roundDown(
      ResourceCalculator calculator, Resource lhs, Resource factor) {
    return calculator.roundDown(lhs, factor);
  }
  
  public static boolean isInvalidDivisor(
      ResourceCalculator resourceCalculator, Resource divisor) {
    return resourceCalculator.isInvalidDivisor(divisor);
  }

  public static float ratio(
      ResourceCalculator resourceCalculator, Resource lhs, Resource rhs) {
    return resourceCalculator.ratio(lhs, rhs);
  }
  
  public static float divide(
      ResourceCalculator resourceCalculator,
      Resource clusterResource, Resource lhs, Resource rhs) {
    return resourceCalculator.divide(clusterResource, lhs, rhs);
  }
  
  public static Resource divideAndCeil(
      ResourceCalculator resourceCalculator, Resource lhs, int rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }

  public static Resource divideAndCeil(
      ResourceCalculator resourceCalculator, Resource lhs, float rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }
  
  public static boolean equals(Resource lhs, Resource rhs) {
    return lhs.equals(rhs);
  }

  public static boolean lessThan(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) < 0);
  }

  public static boolean lessThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) <= 0);
  }

  public static boolean greaterThan(
      ResourceCalculator resourceCalculator,
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) > 0;
  }

  public static boolean greaterThanOrEqual(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0;
  }
  
  public static Resource min(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) <= 0 ? lhs : rhs;
  }

  public static Resource max(
      ResourceCalculator resourceCalculator, 
      Resource clusterResource,
      Resource lhs, Resource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
  }

  public static boolean fitsIn(ResourceCalculator rc, Resource cluster,
      Resource smaller, Resource bigger) {
    return rc.fitsIn(cluster, smaller, bigger);
  }

  public static boolean fitsIn(Resource smaller, Resource bigger) {
    boolean fitsIn = smaller.getMemorySize() <= bigger.getMemorySize() &&
        smaller.getVirtualCores() <= bigger.getVirtualCores() &&
        smaller.getGPUs() <= bigger.getGPUs();
    return fitsIn;
  }

  public static boolean fitsInWithAttribute(Resource smaller, Resource bigger) {
      boolean fitsIn = fitsIn(smaller, bigger);
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


  public static Resource componentwiseMin(Resource lhs, Resource rhs) {
    return createResource(Math.min(lhs.getMemorySize(), rhs.getMemorySize()),
                          Math.min(lhs.getVirtualCores(), rhs.getVirtualCores()),
                          Math.min(lhs.getGPUs(), rhs.getGPUs()));
  }
  
  public static Resource componentwiseMax(Resource lhs, Resource rhs) {
    return createResource(Math.max(lhs.getMemorySize(), rhs.getMemorySize()),
                          Math.max(lhs.getVirtualCores(), rhs.getVirtualCores()),
                          Math.max(lhs.getGPUs(), rhs.getGPUs()));
  }


  // Calculate the candidate GPUs from bigger resource.
  // If the request contains the GPU information, allocate according the request gpu attribute. 
  // If the request does't contains the GPU information, sequencing allocate the free GPUs.
   
  public static long allocateGPUs(Resource smaller, Resource bigger) {
    if (smaller.getGPUAttribute() > 0) {        
         if((smaller.getGPUAttribute() & bigger.getGPUAttribute()) == smaller.getGPUAttribute()){
             return smaller.getGPUAttribute();
         }
         else {
             return 0;
         }
    }
    else {
        return allocateGPUsByCount(smaller.getGPUs(), bigger.getGPUAttribute());
    }
  }

  //Sequencing allocate the free GPUs.
  private static long allocateGPUsByCount(int requestCount, long available)
  {
    int availableCount = Long.bitCount(available);
    if(availableCount >= requestCount) {
      long result = available;
      while (availableCount-- > requestCount) {
        result &= (result - 1);
      }
      return result;
    } else {
      return 0;
    }
  }

  //Sequencing allocate the free GPUs.
  private static ValueRanges allocatePortsByCount(int requestCount, ValueRanges ports) {
    List<ValueRange> rangeList = ports.getRangesList();
    int needAllocateCount = requestCount;

    for (ValueRange range : rangeList) {
      if (range.getEnd() - range.getBegin() >= needAllocateCount - 1) {
        ValueRange vr = ValueRange.newInstance(range.getBegin(), range.getBegin() + needAllocateCount - 1);
        rangeList.add(vr);
        break;
      } else {
        ValueRange vr = ValueRange.newInstance(range.getBegin(), range.getEnd());
        rangeList.add(vr);
        needAllocateCount -= (range.getEnd() - range.getBegin() + 1);
      }
    }
    ValueRanges valueRanges = ValueRanges.newInstance();
    valueRanges.setRangesList(rangeList);
    return valueRanges;
  }

  public static boolean isAnyMajorResourceZero(ResourceCalculator rc,
      Resource resource) {
    return rc.isAnyMajorResourceZero(resource);
  }
}
