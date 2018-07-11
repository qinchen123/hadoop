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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ValueRanges;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

public class NodeResourceMonitorImpl extends AbstractService implements
    NodeResourceMonitor {

  /** Logging infrastructure. */
  final static Log LOG = LogFactory
      .getLog(NodeResourceMonitorImpl.class);

  /** Interval to monitor the node resource utilization. */
  private long monitoringInterval = 60 * 1000;
  /** Thread to monitor the node resource utilization. */
  private MonitoringThread monitoringThread;

  /** Resource calculator. */
  private ResourceCalculatorPlugin resourceCalculatorPlugin;

  private long lastUpdateTime = -1;
  /** Current <em>resource utilization</em> of the node. */
  private long gpuAttribute = 0;

  // Exclude the Gpus are being used by un-know program.
  // Usually, the Gpu memory status is non-zero, but the process of this GPU is empty.
  private boolean excludeOwnerlessUsingGpus;
  private int gpuNotReadyMemoryThreshold;

  /**
   * Initialize the node resource monitor.
   */
  public NodeResourceMonitorImpl() {
    super(NodeResourceMonitorImpl.class.getName());
    this.monitoringThread = new MonitoringThread();
  }

  /**
   * Initialize the service with the proper parameters.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, null);

    this.excludeOwnerlessUsingGpus =
        conf.getBoolean(YarnConfiguration.GPU_EXCLUDE_OWNERLESS_GPUS,
            YarnConfiguration.DEFAULT_GPU_EXCLUDE_OWNERLESS_GPUS);

    this.gpuNotReadyMemoryThreshold =
        conf.getInt(YarnConfiguration.GPU_NOT_READY_MEMORY_THRESHOLD,
            YarnConfiguration.DEFAULT_GPU_NOT_READY_MEMORY_THRESHOLD);

    LOG.info("NodeResourceMonitorImpl: Using ResourceCalculatorPlugin : "
        + this.resourceCalculatorPlugin);
    this.gpuAttribute = 0;
    lastUpdateTime = System.currentTimeMillis();
  }

  /**
   * Check if we should be monitoring.
   * @return <em>true</em> if we can monitor the node resource utilization.
   */
  private boolean isEnabled() {
    if (resourceCalculatorPlugin == null) {
      LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    return true;
  }

  /**
   * Start the thread that does the node resource utilization monitoring.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  /**
   * Stop the thread that does the node resource utilization monitoring.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Could not wait for the thread to join");
      }
    }
    super.serviceStop();
  }

  /**
   * Thread that monitors the resource utilization of this node.
   */
  private class MonitoringThread extends Thread {
    /**
     * Initialize the node resource monitoring thread.
     */
    public MonitoringThread() {
      super("Node Resource Monitor");
      this.setDaemon(true);
    }

    /**
     * Periodically monitor the resource utilization of the node.
     */
    @Override
    public void run() {

      int count = 0;
      LOG.info("Start NodeResourceMonitorImpl thread:" + Thread.currentThread().getName());
      while (true) {
        // Get node utilization and save it into the health status
        long gpus = resourceCalculatorPlugin.getGpuAttributeCapacity(excludeOwnerlessUsingGpus, gpuNotReadyMemoryThreshold);
        // Check if the reading is invalid
        if (gpus <= 0) {
          LOG.error("Cannot get gpu information, set it to 0");
          gpuAttribute = 0;
        } else {
          gpuAttribute = gpus;
        }
        if(count++ % 20 == 0) {
          count = 0;
          LOG.info("get GPU attribute:" + Long.toBinaryString(gpus));
        }
        lastUpdateTime = System.currentTimeMillis();
        try {
            Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(NodeResourceMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }

  /**
   * Get the <em>gpu utilization</em> of the node.
   * @return <em>gpu utilization</em> of the node.
   */
  @Override
  public long getGpuAttribute() {
    long now = System.currentTimeMillis();

    if(now > lastUpdateTime + monitoringInterval * 10) {
      LOG.warn(NodeResourceMonitorImpl.class.getName()
          + " Too long to get the GPU information, set GPU Capacity to 0. LastUpdateTime=" + lastUpdateTime + "nowTime=" + now);
     return 0L;
    }
    return this.gpuAttribute;
  }

  /**
   * Get the <em>Ports utilization</em> of the node.
   * @return <em>Ports utilization</em> of the node.
   */
  @Override
  public String getUsedPorts() {
    return resourceCalculatorPlugin.getPortsUsage();
  }
}
