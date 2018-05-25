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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * Contains various scheduling metrics to be reported by UI and CLI.
 */
@Public
@Stable
public abstract class ApplicationResourceUsageReport {

  @Private
  @Unstable
  public static ApplicationResourceUsageReport newInstance(
      int numUsedContainers, int numReservedContainers, Resource usedResources,
      Resource reservedResources, Resource neededResources, long memorySeconds,
      long vcoreSeconds, long GPUSeconds, float queueUsagePerc, float clusterUsagePerc,
      long preemptedMemorySeconds, long preemptedVcoresSeconds) {

    ApplicationResourceUsageReport report =
        Records.newRecord(ApplicationResourceUsageReport.class);
    report.setNumUsedContainers(numUsedContainers);
    report.setNumReservedContainers(numReservedContainers);
    report.setUsedResources(usedResources);
    report.setReservedResources(reservedResources);
    report.setNeededResources(neededResources);
    report.setMemorySeconds(memorySeconds);
    report.setVcoreSeconds(vcoreSeconds);
    report.setGPUSeconds(GPUSeconds);
    report.setQueueUsagePercentage(queueUsagePerc);
    report.setClusterUsagePercentage(clusterUsagePerc);
    report.setPreemptedMemorySeconds(preemptedMemorySeconds);
    report.setPreemptedVcoreSeconds(preemptedVcoresSeconds);
    return report;
  }

  /**
   * Get the number of used containers.  -1 for invalid/inaccessible reports.
   * @return the number of used containers
   */
  @Public
  @Stable
  public abstract int getNumUsedContainers();

  /**
   * Set the number of used containers
   * @param num_containers the number of used containers
   */
  @Private
  @Unstable
  public abstract void setNumUsedContainers(int num_containers);

  /**
   * Get the number of reserved containers.  -1 for invalid/inaccessible reports.
   * @return the number of reserved containers
   */
  @Private
  @Unstable
  public abstract int getNumReservedContainers();

  /**
   * Set the number of reserved containers
   * @param num_reserved_containers the number of reserved containers
   */
  @Private
  @Unstable
  public abstract void setNumReservedContainers(int num_reserved_containers);

  /**
   * Get the used <code>Resource</code>.  -1 for invalid/inaccessible reports.
   * @return the used <code>Resource</code>
   */
  @Public
  @Stable
  public abstract Resource getUsedResources();

  @Private
  @Unstable
  public abstract void setUsedResources(Resource resources);

  /**
   * Get the reserved <code>Resource</code>.  -1 for invalid/inaccessible reports.
   * @return the reserved <code>Resource</code>
   */
  @Public
  @Stable
  public abstract Resource getReservedResources();

  @Private
  @Unstable
  public abstract void setReservedResources(Resource reserved_resources);

  /**
   * Get the needed <code>Resource</code>.  -1 for invalid/inaccessible reports.
   * @return the needed <code>Resource</code>
   */
  @Public
  @Stable
  public abstract Resource getNeededResources();

  @Private
  @Unstable
  public abstract void setNeededResources(Resource needed_resources);

  /**
   * Set the aggregated amount of memory (in megabytes) the application has
   * allocated times the number of seconds the application has been running.
   * @param memory_seconds the aggregated amount of memory seconds
   */
  @Private
  @Unstable
  public abstract void setMemorySeconds(long memory_seconds);

  /**
   * Get the aggregated amount of memory (in megabytes) the application has
   * allocated times the number of seconds the application has been running.
   * @return the aggregated amount of memory seconds
   */
  @Public
  @Unstable
  public abstract long getMemorySeconds();

  /**
   * Set the aggregated number of vcores that the application has allocated
   * times the number of seconds the application has been running.
   * @param vcore_seconds the aggregated number of vcore seconds
   */
  @Private
  @Unstable
  public abstract void setVcoreSeconds(long vcore_seconds);

  /**
   * Get the aggregated number of vcores that the application has allocated
   * times the number of seconds the application has been running.
   * @return the aggregated number of vcore seconds
   */
  @Public
  @Unstable
  public abstract long getVcoreSeconds();

  /**
   * Get the percentage of resources of the queue that the app is using.
   * @return the percentage of resources of the queue that the app is using.
   */
  @Public
  @Stable
  public abstract float getQueueUsagePercentage();

  /**
   * Set the percentage of resources of the queue that the app is using.
   * @param queueUsagePerc the percentage of resources of the queue that
   *                       the app is using.
   */
  @Private
  @Unstable
  public abstract void setQueueUsagePercentage(float queueUsagePerc);

  /**
   * Get the percentage of resources of the cluster that the app is using.
   * @return the percentage of resources of the cluster that the app is using.
   */
  @Public
  @Stable
  public abstract float getClusterUsagePercentage();

  /**
   * Set the percentage of resources of the cluster that the app is using.
   * @param clusterUsagePerc the percentage of resources of the cluster that
   *                         the app is using.
   */
  @Private
  @Unstable
  public abstract void setClusterUsagePercentage(float clusterUsagePerc);

  /**
   * Set the aggregated amount of memory preempted (in megabytes)
   * the application has allocated times the number of seconds
   * the application has been running.
   * @param memorySeconds the aggregated amount of memory seconds
   */
  @Private
  @Unstable
  public abstract void setPreemptedMemorySeconds(long memorySeconds);

  /**
   * Get the aggregated amount of memory preempted(in megabytes)
   * the application has allocated times the number of
   * seconds the application has been running.
   * @return the aggregated amount of memory seconds
   */
  @Public
  @Unstable
  public abstract long getPreemptedMemorySeconds();

  /**
   * Set the aggregated number of vcores preempted that the application has
   * allocated times the number of seconds the application has been running.
   * @param vcoreSeconds the aggregated number of vcore seconds
   */
  @Private
  @Unstable
  public abstract void setPreemptedVcoreSeconds(long vcoreSeconds);

  /**
   * Get the aggregated number of vcores preempted that the application has
   * allocated times the number of seconds the application has been running.
   * @return the aggregated number of vcore seconds
   */
  @Public
  @Unstable
  public abstract long getPreemptedVcoreSeconds();

  /**
   * Set the aggregated number of GPUs that the application has allocated
   * times the number of seconds the application has been running.
   * @param GPUSeconds the aggregated number of GPU seconds
   */
  @Private
  @Unstable
  public abstract void setGPUSeconds(long gpuSeconds);

  /**
   * Get the aggregated number of GPUs that the application has allocated
   * times the number of seconds the application has been running.
   * @return the aggregated number of GPU seconds
   */
  @Public
  @Unstable
  public abstract long getGPUSeconds();
}
