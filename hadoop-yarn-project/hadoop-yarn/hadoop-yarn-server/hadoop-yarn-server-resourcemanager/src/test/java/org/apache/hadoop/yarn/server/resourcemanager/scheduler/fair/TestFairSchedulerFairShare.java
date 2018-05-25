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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFairSchedulerFairShare extends FairSchedulerTestBase {
  private final static String ALLOC_FILE = new File(TEST_DIR,
      TestFairSchedulerFairShare.class.getName() + ".xml").getAbsolutePath();

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);
  }

  @After
  public void teardown() {
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    conf = null;
  }

  private void createClusterWithQueuesAndOneNode(int mem, int vCores, int GPUs,
      String policy) throws IOException {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\" >");
    out.println("   <queue name=\"parentA\" >");
    out.println("       <weight>8</weight>");
    out.println("       <queue name=\"childA1\" />");
    out.println("       <queue name=\"childA2\" />");
    out.println("       <queue name=\"childA3\" />");
    out.println("       <queue name=\"childA4\" />");
    out.println("   </queue>");
    out.println("   <queue name=\"parentB\" >");
    out.println("       <weight>1</weight>");
    out.println("       <queue name=\"childB1\" />");
    out.println("       <queue name=\"childB2\" />");
    out.println("   </queue>");
    out.println("</queue>");
    out.println("<defaultQueueSchedulingPolicy>" + policy
        + "</defaultQueueSchedulingPolicy>");
    out.println("</allocations>");
    out.close();

    resourceManager = new MockRM(conf);
    resourceManager.start();
    scheduler = (FairScheduler) resourceManager.getResourceScheduler();

    RMNode node1 = MockNodes.newNodeInfo(1,
        Resources.createResource(mem, vCores, GPUs), 1, "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
  }

  @Test
  public void testFairShareNoAppsRunning() throws IOException {
    int nodeCapacity = 16;
    createClusterWithQueuesAndOneNode(nodeCapacity * 1024, nodeCapacity, nodeCapacity, "fair");

    scheduler.update();
    // No apps are running in the cluster,verify if fair share is zero
    // for all queues under parentA and parentB.
    Collection<FSLeafQueue> leafQueues = scheduler.getQueueManager()
        .getLeafQueues();

    for (FSLeafQueue leaf : leafQueues) {
      if (leaf.getName().startsWith("root.parentA")) {
        assertEquals(0, (double) leaf.getFairShare().getGPUs() / nodeCapacity,
            0);
      } else if (leaf.getName().startsWith("root.parentB")) {
        assertEquals(0, (double) leaf.getFairShare().getGPUs() / nodeCapacity,
            0);
      }
    }

    verifySteadyFairShareGPUs(leafQueues, nodeCapacity);
  }

  @Test
  public void testFairShareOneAppRunning() throws IOException {
    int nodeCapacity = 16;
    createClusterWithQueuesAndOneNode(nodeCapacity * 1024, nodeCapacity, nodeCapacity, "fair");

    // Run a app in a childA1. Verify whether fair share is 100% in childA1,
    // since it is the only active queue.
    // Also verify if fair share is 0 for childA2. since no app is
    // running in it.
    createSchedulingRequest(2 * 1024, 2, 2, "root.parentA.childA1", "user1");

    scheduler.update();

    assertEquals(
        100,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentA.childA1", false).getFairShare()
            .getGPUs() / nodeCapacity * 100, 0.1);
    assertEquals(
        0,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentA.childA2", false).getFairShare()
            .getGPUs() / nodeCapacity, 0.1);

    verifySteadyFairShareGPUs(scheduler.getQueueManager().getLeafQueues(),
        nodeCapacity);
  }

  @Test
  public void testFairShareMultipleActiveQueuesUnderSameParent()
      throws IOException {
    int nodeCapacity = 16;
    createClusterWithQueuesAndOneNode(nodeCapacity * 1024, nodeCapacity, nodeCapacity, "fair");

    // Run apps in childA1,childA2,childA3
    createSchedulingRequest(2 * 1024, 2, 2, "root.parentA.childA1", "user1");
    createSchedulingRequest(2 * 1024, 2, 2, "root.parentA.childA2", "user2");
    createSchedulingRequest(2 * 1024, 2, 2, "root.parentA.childA3", "user3");

    scheduler.update();

    // Verify fair share:
    //     16 GPUs / 3 = 5.33 => 6 GPUs
    //     For each child, 6 / 16 * 100 = 37.5
    for (int i = 1; i <= 3; i++) {
      assertEquals(
          37.5,
          (double) scheduler.getQueueManager()
              .getLeafQueue("root.parentA.childA" + i, false).getFairShare()
              .getGPUs()
              / nodeCapacity * 100, .9);
    }

    verifySteadyFairShareGPUs(scheduler.getQueueManager().getLeafQueues(),
        nodeCapacity);
  }

  @Test
  public void testFairShareMultipleActiveQueuesUnderDifferentParent()
      throws IOException {
    int nodeCapacity = 16;
    createClusterWithQueuesAndOneNode(nodeCapacity * 1024, nodeCapacity, nodeCapacity, "fair");

    // Run apps in childA1,childA2 which are under parentA
    createSchedulingRequest(2 * 1024, 2, 2, "root.parentA.childA1", "user1");
    createSchedulingRequest(3 * 1024, 3, 3, "root.parentA.childA2", "user2");

    // Run app in childB1 which is under parentB
    createSchedulingRequest(1 * 1024, 1, 1, "root.parentB.childB1", "user3");

    // Run app in root.default queue
    createSchedulingRequest(1 * 1024, 1, 1, "root.default", "user4");

    scheduler.update();

    // The two active child queues under parentA would
    // get fair share of 80/2=40%, but in GPU case:
    //     16 GPUs * 0.8 / 2 = 6.4 => 7 GPUs
    //     For each child, 7 / 16 * 100 = 43.75
    for (int i = 1; i <= 2; i++) {
      assertEquals(
          43.75,
          (double) scheduler.getQueueManager()
              .getLeafQueue("root.parentA.childA" + i, false).getFairShare()
              .getGPUs()
              / nodeCapacity * 100, .9);
    }

    // The child queue under parentB would get a fair share of 10%,
    // basically all of parentB's fair share, but in GPU case:
    //     16 GPUs * 0.1 = 1.6, where this child can't get 2 GPUs
    //     as two childAs already got 7 GPUs each. So, 1 GPU is assigned.
    assertEquals(
        6.25,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentB.childB1", false).getFairShare()
            .getGPUs()
            / nodeCapacity * 100, .9);
    assertEquals(
        6.25,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.default", false).getFairShare()
            .getGPUs()
            / nodeCapacity * 100, .9);

    verifySteadyFairShareGPUs(scheduler.getQueueManager().getLeafQueues(),
        nodeCapacity);
  }

  @Test
  public void testFairShareResetsToZeroWhenAppsComplete() throws IOException {
    int nodeCapacity = 16;
    createClusterWithQueuesAndOneNode(nodeCapacity * 1024, nodeCapacity, nodeCapacity, "fair");

    // Run apps in childA1,childA2 which are under parentA
    ApplicationAttemptId app1 = createSchedulingRequest(2 * 1024, 2, 2,
        "root.parentA.childA1", "user1");
    ApplicationAttemptId app2 = createSchedulingRequest(3 * 1024, 3, 3,
        "root.parentA.childA2", "user2");

    scheduler.update();

    // Verify if both the active queues under parentA get 50% fair
    // share
    for (int i = 1; i <= 2; i++) {
      assertEquals(
          50,
          (double) scheduler.getQueueManager()
              .getLeafQueue("root.parentA.childA" + i, false).getFairShare()
              .getGPUs()
              / nodeCapacity * 100, .9);
    }
    // Let app under childA1 complete. This should cause the fair share
    // of queue childA1 to be reset to zero,since the queue has no apps running.
    // Queue childA2's fair share would increase to 100% since its the only
    // active queue.
    AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent(
        app1, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();

    assertEquals(
        0,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentA.childA1", false).getFairShare()
            .getGPUs()
            / nodeCapacity * 100, 0);
    assertEquals(
        100,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentA.childA2", false).getFairShare()
            .getGPUs()
            / nodeCapacity * 100, 0.1);

    verifySteadyFairShareGPUs(scheduler.getQueueManager().getLeafQueues(),
        nodeCapacity);
  }

  @Test
  public void testFairShareWithDRFMultipleActiveQueuesUnderDifferentParent()
      throws IOException {
    int nodeMem = 16 * 1024;
    int nodeVCores = 10;
    int nodeGPUs = 10;
    createClusterWithQueuesAndOneNode(nodeMem, nodeVCores, nodeGPUs, "drf");

    // Run apps in childA1,childA2 which are under parentA
    createSchedulingRequest(2 * 1024, "root.parentA.childA1", "user1");
    createSchedulingRequest(3 * 1024, "root.parentA.childA2", "user2");

    // Run app in childB1 which is under parentB
    createSchedulingRequest(1 * 1024, "root.parentB.childB1", "user3");

    // Run app in root.default queue
    createSchedulingRequest(1 * 1024, "root.default", "user4");

    scheduler.update();

    // The two active child queues under parentA would
    // get 80/2=40% memory and vcores
    for (int i = 1; i <= 2; i++) {
      assertEquals(
          40,
          (double) scheduler.getQueueManager()
              .getLeafQueue("root.parentA.childA" + i, false).getFairShare()
              .getMemory()
              / nodeMem * 100, .9);
      assertEquals(
          40,
          (double) scheduler.getQueueManager()
              .getLeafQueue("root.parentA.childA" + i, false).getFairShare()
              .getVirtualCores()
              / nodeVCores * 100, .9);
    }

    // The only active child queue under parentB would get 10% memory and vcores
    assertEquals(
        10,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentB.childB1", false).getFairShare()
            .getMemory()
            / nodeMem * 100, .9);
    assertEquals(
        10,
        (double) scheduler.getQueueManager()
            .getLeafQueue("root.parentB.childB1", false).getFairShare()
            .getVirtualCores()
            / nodeVCores * 100, .9);
    Collection<FSLeafQueue> leafQueues = scheduler.getQueueManager()
        .getLeafQueues();

    for (FSLeafQueue leaf : leafQueues) {
      if (leaf.getName().startsWith("root.parentA")) {
        assertEquals(0.2,
            (double) leaf.getSteadyFairShare().getMemory() / nodeMem, 0.001);
        assertEquals(0.2,
            (double) leaf.getSteadyFairShare().getVirtualCores() / nodeVCores,
            0.001);
      } else if (leaf.getName().startsWith("root.parentB")) {
        assertEquals(0.05,
            (double) leaf.getSteadyFairShare().getMemory() / nodeMem, 0.001);
        assertEquals(0.1,
            (double) leaf.getSteadyFairShare().getVirtualCores() / nodeVCores,
            0.001);
      }
    }
  }

  /**
   * Verify whether steady fair shares for all leaf queues still follow
   * their weight, not related to active/inactive status.
   *
   * @param leafQueues
   * @param nodeCapacity
   */
  private void verifySteadyFairShareGPUs(Collection<FSLeafQueue> leafQueues,
      int nodeCapacity) {
    for (FSLeafQueue leaf : leafQueues) {
      if (leaf.getName().startsWith("root.parentA")) {
        assertEquals(0.25,
            (double) leaf.getSteadyFairShare().getGPUs() / nodeCapacity,
            0.001);
      } else if (leaf.getName().startsWith("root.parentB")) {
        assertEquals(0.0625,
            (double) leaf.getSteadyFairShare().getGPUs() / nodeCapacity,
            0.001);
      }
    }
  }
}
