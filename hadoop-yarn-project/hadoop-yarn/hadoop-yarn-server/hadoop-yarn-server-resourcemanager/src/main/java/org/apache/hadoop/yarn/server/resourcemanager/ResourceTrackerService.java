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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.VersionUtil;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.*;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.utils.YarnServerBuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import com.google.common.annotations.VisibleForTesting;

public class ResourceTrackerService extends AbstractService implements
    ResourceTracker {

  private static final Log LOG = LogFactory.getLog(ResourceTrackerService.class);

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  private final NMLivelinessMonitor nmLivelinessMonitor;
  private final RMContainerTokenSecretManager containerTokenSecretManager;
  private final NMTokenSecretManagerInRM nmTokenSecretManager;

  private long nextHeartBeatInterval;
  private Server server;
  private InetSocketAddress resourceTrackerAddress;
  private String minimumNodeManagerVersion;

  private static final NodeHeartbeatResponse resync = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);
  private static final NodeHeartbeatResponse shutDown = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);

  private int minAllocMb;
  private int minAllocVcores;
  private int minAllocGPUs;

  private boolean enablePortsAsResource;
  private boolean enablePortsBitSetStore;

  static {
    resync.setNodeAction(NodeAction.RESYNC);

    shutDown.setNodeAction(NodeAction.SHUTDOWN);
  }

  public ResourceTrackerService(RMContext rmContext,
                                NodesListManager nodesListManager,
                                NMLivelinessMonitor nmLivelinessMonitor,
                                RMContainerTokenSecretManager containerTokenSecretManager,
                                NMTokenSecretManagerInRM nmTokenSecretManager) {
    super(ResourceTrackerService.class.getName());
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.nmLivelinessMonitor = nmLivelinessMonitor;
    this.containerTokenSecretManager = containerTokenSecretManager;
    this.nmTokenSecretManager = nmTokenSecretManager;

  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    resourceTrackerAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    RackResolver.init(conf);
    nextHeartBeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (nextHeartBeatInterval <= 0) {
      throw new YarnRuntimeException("Invalid Configuration. "
          + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS
          + " should be larger than 0.");
    }

    minAllocMb = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    minAllocVcores = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    minAllocGPUs = conf.getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_GPUS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_GPUS);

    minimumNodeManagerVersion = conf.get(
        YarnConfiguration.RM_NODEMANAGER_MINIMUM_VERSION,
        YarnConfiguration.DEFAULT_RM_NODEMANAGER_MINIMUM_VERSION);

    enablePortsAsResource =
        conf.getBoolean(YarnConfiguration.PORTS_AS_RESOURCE_ENABLE,
            YarnConfiguration.DEFAULT_PORTS_AS_RESOURCE_ENABLE);
    enablePortsBitSetStore =
        conf.getBoolean(YarnConfiguration.PORTS_BITSET_STORE_ENABLE,
            YarnConfiguration.DEFAULT_PORTS_BITSET_STORE_ENABLE);


    LOG.info("serviceInit with config: {minAllocMb" + minAllocMb + " minAllocVcores:" + minAllocVcores + " minAllocGPUs:" + minAllocGPUs
      + " minimumNodeManagerVersion:" + minimumNodeManagerVersion + " enablePortsAsResource:" + enablePortsAsResource + " enablePortsBitSetStore" + enablePortsBitSetStore);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // ResourceTrackerServer authenticates NodeManager via Kerberos if
    // security is enabled, so no secretManager.
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
        rpc.getServer(ResourceTracker.class, this, resourceTrackerAddress,
            conf, null,
            conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT,
                YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT));

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
        server.getListenerAddress());
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }

  /**
   * Helper method to handle received ContainerStatus. If this corresponds to
   * the completion of a master-container of a managed AM,
   * we call the handler for RMAppAttemptContainerFinishedEvent.
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  void handleNMContainerStatus(NMContainerStatus containerStatus, NodeId nodeId) {
    ApplicationAttemptId appAttemptId =
        containerStatus.getContainerId().getApplicationAttemptId();
    RMApp rmApp =
        rmContext.getRMApps().get(appAttemptId.getApplicationId());
    if (rmApp == null) {
      LOG.error("Received finished container : "
          + containerStatus.getContainerId()
          + " for unknown application " + appAttemptId.getApplicationId()
          + " Skipping.");
      return;
    }

    if (rmApp.getApplicationSubmissionContext().getUnmanagedAM()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring container completion status for unmanaged AM "
            + rmApp.getApplicationId());
      }
      return;
    }

    RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptId);
    Container masterContainer = rmAppAttempt.getMasterContainer();
    if (masterContainer.getId().equals(containerStatus.getContainerId())
        && containerStatus.getContainerState() == ContainerState.COMPLETE) {
      ContainerStatus status =
          ContainerStatus.newInstance(containerStatus.getContainerId(),
              containerStatus.getContainerState(), containerStatus.getDiagnostics(),
              containerStatus.getContainerExitStatus());
      // sending master container finished event.
      RMAppAttemptContainerFinishedEvent evt =
          new RMAppAttemptContainerFinishedEvent(appAttemptId, status,
              nodeId);
      rmContext.getDispatcher().getEventHandler().handle(evt);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException,
      IOException {
    NodeId nodeId = request.getNodeId();
    String host = nodeId.getHost();
    int cmPort = nodeId.getPort();
    int httpPort = request.getHttpPort();
    Resource capability = request.getResource();
    String nodeManagerVersion = request.getNMVersion();

    RegisterNodeManagerResponse response = recordFactory
        .newRecordInstance(RegisterNodeManagerResponse.class);

    LOG.info("registerNodeManager: nodeId=" + host + " witch totalCapacity=" + capability);

    if (!minimumNodeManagerVersion.equals("NONE")) {
      if (minimumNodeManagerVersion.equals("EqualToRM")) {
        minimumNodeManagerVersion = YarnVersionInfo.getVersion();
      }

      if ((nodeManagerVersion == null) ||
          (VersionUtil.compareVersions(nodeManagerVersion, minimumNodeManagerVersion)) < 0) {
        String message =
            "Disallowed NodeManager Version " + nodeManagerVersion
                + ", is less than the minimum version "
                + minimumNodeManagerVersion + " sending SHUTDOWN signal to "
                + "NodeManager.";
        LOG.info(message);
        response.setDiagnosticsMessage(message);
        response.setNodeAction(NodeAction.SHUTDOWN);
        return response;
      }
    }

    // Check if this node is a 'valid' node
    if (!this.nodesListManager.isValidNode(host)) {
      String message =
          "Disallowed NodeManager from  " + host
              + ", Sending SHUTDOWN signal to the NodeManager.";
      LOG.info(message);
      response.setDiagnosticsMessage(message);
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    // Check if this node has minimum allocations
    if (capability.getMemory() < minAllocMb
        || capability.getVirtualCores() < minAllocVcores
        || capability.getGPUs() < minAllocGPUs) {
      String message =
          "NodeManager from  " + host
              + " doesn't satisfy minimum allocations, Sending SHUTDOWN"
              + " signal to the NodeManager.";
      LOG.info(message);
      response.setDiagnosticsMessage(message);
      response.setNodeAction(NodeAction.SHUTDOWN);
      return response;
    }

    // reset illegal resource report
    if (!this.enablePortsAsResource) {
      capability.setPorts(null);
    }

    response.setContainerTokenMasterKey(containerTokenSecretManager
        .getCurrentKey());
    response.setNMTokenMasterKey(nmTokenSecretManager
        .getCurrentKey());


    ValueRanges localUsedPorts = null;
    if (this.enablePortsAsResource) {
      localUsedPorts = request.getLocalUsedPortsSnapshot();
      if (this.enablePortsBitSetStore
          && request.getLocalUsedPortsSnapshot() != null) {
        localUsedPorts =
            ValueRanges.convertToBitSet(request.getLocalUsedPortsSnapshot());
      }
    }
    RMNode rmNode = new RMNodeImpl(nodeId, rmContext, host, cmPort, httpPort,
        resolve(host), capability, nodeManagerVersion, localUsedPorts);
    if (this.enablePortsAsResource && this.enablePortsBitSetStore) {
      if (rmNode.getTotalCapability().getPorts() != null) {
        ValueRanges totalPorts =
            ValueRanges.convertToBitSet(rmNode.getTotalCapability().getPorts());
        rmNode.getTotalCapability().setPorts(totalPorts);
      }
      if (rmNode.getContainerAllocatedPorts() == null) {
        rmNode.setContainerAllocatedPorts(ValueRanges.newInstance());
        rmNode.getContainerAllocatedPorts().setByteStoreEnable(true);
      }
      ValueRanges containerAllocatedPorts =
          ValueRanges.convertToBitSet(rmNode.getContainerAllocatedPorts());
      rmNode.setContainerAllocatedPorts(containerAllocatedPorts);

      if (rmNode.getLocalUsedPortsSnapshot() != null) {
        ValueRanges localUsedPortsSnapshot =
            ValueRanges.convertToBitSet(rmNode.getLocalUsedPortsSnapshot());
        rmNode.setLocalUsedPortsSnapshot(localUsedPortsSnapshot);
      }
    }

    if (this.enablePortsAsResource) {
      rmNode.setAvailablePorts(
          getAvailablePorts(
              rmNode.getTotalCapability().getPorts(),
              rmNode.getContainerAllocatedPorts(),
              rmNode.getLocalUsedPortsSnapshot()));
      if (this.enablePortsBitSetStore && rmNode.getAvailablePorts() != null) {
        rmNode.getAvailablePorts().setByteStoreEnable(true);
        ValueRanges availablePorts =
            ValueRanges.convertToBitSet(rmNode.getAvailablePorts());
        rmNode.setAvailablePorts(availablePorts);
      }
    }

    RMNode oldNode = this.rmContext.getRMNodes().putIfAbsent(nodeId, rmNode);
    if (oldNode == null) {
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeStartedEvent(nodeId, request.getNMContainerStatuses(),
              request.getRunningApplications()));
    } else {
      LOG.info("Reconnect from the node at: " + host);
      this.nmLivelinessMonitor.unregister(nodeId);
      // Reset heartbeat ID since node just restarted.
      oldNode.resetLastNodeHeartBeatResponse();
      this.rmContext
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMNodeReconnectEvent(nodeId, rmNode, request
                  .getRunningApplications(), request.getNMContainerStatuses()));
    }
    // On every node manager register we will be clearing NMToken keys if
    // present for any running application.
    this.nmTokenSecretManager.removeNodeKey(nodeId);
    this.nmLivelinessMonitor.register(nodeId);

    // Handle received container status, this should be processed after new
    // RMNode inserted
    if (!rmContext.isWorkPreservingRecoveryEnabled()) {
      if (!request.getNMContainerStatuses().isEmpty()) {
        LOG.info("received container statuses on node manager register :"
            + request.getNMContainerStatuses());
        for (NMContainerStatus status : request.getNMContainerStatuses()) {
          handleNMContainerStatus(status, nodeId);
        }
      }
    }

    String message =
        "NodeManager from node " + host + "(cmPort: " + cmPort + " httpPort: "
            + httpPort + ") " + "registered with capability: " + capability
            + ", assigned nodeId " + nodeId +  ", localUsedPorts: " + rmNode.getLocalUsedPortsSnapshot() + " , available Port:"+ rmNode.getAvailablePorts() + ", containerAllocated Port:" + rmNode.getContainerAllocatedPorts();
    LOG.info(message);
    response.setNodeAction(NodeAction.NORMAL);
    response.setRMIdentifier(ResourceManager.getClusterTimeStamp());
    response.setRMVersion(YarnVersionInfo.getVersion());
    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException {

    NodeStatus remoteNodeStatus = request.getNodeStatus();
    /**
     * Here is the node heartbeat sequence...
     * 1. Check if it's a valid (i.e. not excluded) node
     * 2. Check if it's a registered node
     * 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
     * 4. Send healthStatus to RMNode
     */

    NodeId nodeId = remoteNodeStatus.getNodeId();

    // 1. Check if it's a valid (i.e. not excluded) node
    if (!this.nodesListManager.isValidNode(nodeId.getHost())) {
      String message =
          "Disallowed NodeManager nodeId: " + nodeId + " hostname: "
              + nodeId.getHost();
      LOG.info(message);
      shutDown.setDiagnosticsMessage(message);
      return shutDown;
    }

    // 2. Check if it's a registered node
    RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
    if (rmNode == null) {
      /* node does not exist */
      String message = "Node not found resyncing " + remoteNodeStatus.getNodeId();
      LOG.info(message);
      resync.setDiagnosticsMessage(message);
      return resync;
    }

    // Send ping
    this.nmLivelinessMonitor.receivedPing(nodeId);

    // 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
    NodeHeartbeatResponse lastNodeHeartbeatResponse = rmNode.getLastNodeHeartBeatResponse();
    if (remoteNodeStatus.getResponseId() + 1 == lastNodeHeartbeatResponse
        .getResponseId()) {
      LOG.info("Received duplicate heartbeat from node "
          + rmNode.getNodeAddress() + " responseId=" + remoteNodeStatus.getResponseId());
      return lastNodeHeartbeatResponse;
    } else if (remoteNodeStatus.getResponseId() + 1 < lastNodeHeartbeatResponse
        .getResponseId()) {
      String message =
          "Too far behind rm response id:"
              + lastNodeHeartbeatResponse.getResponseId() + " nm response id:"
              + remoteNodeStatus.getResponseId();
      LOG.info(message);
      resync.setDiagnosticsMessage(message);
      // TODO: Just sending reboot is not enough. Think more.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
      return resync;
    }

    // Heartbeat response
    NodeHeartbeatResponse nodeHeartBeatResponse = YarnServerBuilderUtils
        .newNodeHeartbeatResponse(lastNodeHeartbeatResponse.
                getResponseId() + 1, NodeAction.NORMAL, null, null, null, null,
            nextHeartBeatInterval);
    rmNode.updateNodeHeartbeatResponseForCleanup(nodeHeartBeatResponse);

    populateKeys(request, nodeHeartBeatResponse);

    ConcurrentMap<ApplicationId, ByteBuffer> systemCredentials =
        rmContext.getSystemCredentialsForApps();
    if (!systemCredentials.isEmpty()) {
      nodeHeartBeatResponse.setSystemCredentialsForApps(systemCredentials);
    }

    // 4. Send status to RMNode, saving the latest response.
    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMNodeStatusEvent(nodeId, remoteNodeStatus.getNodeHealthStatus(),
            remoteNodeStatus.getContainersStatuses(),
            remoteNodeStatus.getKeepAliveApplications(), nodeHeartBeatResponse));

    // 5. Update the local used ports snapshot
    if (this.enablePortsAsResource) {
      ValueRanges ports = remoteNodeStatus.getLocalUsedPortsSnapshot();
      if (ports != null) {
        rmNode.setLocalUsedPortsSnapshot(ports);
        if (this.enablePortsBitSetStore) {
          ValueRanges LocalUsedPorts =
              ValueRanges.convertToBitSet(rmNode.getLocalUsedPortsSnapshot());
          rmNode.setLocalUsedPortsSnapshot(LocalUsedPorts);
        }
        ValueRanges availablePorts = null;
        if (rmNode.getTotalCapability().getPorts() != null) {
          availablePorts =
              getAvailablePorts(rmNode.getTotalCapability().getPorts(),
                  rmNode.getContainerAllocatedPorts(),
                  rmNode.getLocalUsedPortsSnapshot());
        }
        rmNode.setAvailablePorts(availablePorts);
      }
    }

    // 6. Send new totalCapacity to RMNode if totalCpacity changed;
    if(!rmNode.getTotalCapability().equalsWithGPUAttribute(remoteNodeStatus.getResource())) {
    Resource newTotalCapacity = Resource.newInstance(remoteNodeStatus.getResource().getMemory(),
        remoteNodeStatus.getResource().getVirtualCores(), remoteNodeStatus.getResource().getGPUs(), remoteNodeStatus.getResource().getGPUAttribute());
        ValueRanges newCapacityPorts = ValueRanges.add(rmNode.getAvailablePorts(), rmNode.getContainerAllocatedPorts());
    newTotalCapacity.setPorts(newCapacityPorts);

      ResourceOption newResourceOption = ResourceOption.newInstance(newTotalCapacity, 1000);
      this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMNodeResourceUpdateEvent(nodeId, newResourceOption));
    }

    if(LOG.isDebugEnabled()) {
      String message =
          "NodeManager heartbeat from node " + rmNode.getHostName() + " with newTotalCapacity: " + remoteNodeStatus.getResource().toNoPortsString();
      LOG.debug(message);

    }

    return nodeHeartBeatResponse;
  }

  private static ValueRanges getAvailablePorts(ValueRanges total,
        ValueRanges allocated, ValueRanges localUsed) {
      if (total == null) {
        return null;
      }
      return total.minusSelf(allocated).minusSelf(localUsed);
    }

    private void populateKeys(NodeHeartbeatRequest request,
      NodeHeartbeatResponse nodeHeartBeatResponse) {

    // Check if node's masterKey needs to be updated and if the currentKey has
    // roller over, send it across

    // ContainerTokenMasterKey

    MasterKey nextMasterKeyForNode =
        this.containerTokenSecretManager.getNextKey();
    if (nextMasterKeyForNode != null
        && (request.getLastKnownContainerTokenMasterKey().getKeyId()
            != nextMasterKeyForNode.getKeyId())) {
      nodeHeartBeatResponse.setContainerTokenMasterKey(nextMasterKeyForNode);
    }

    // NMTokenMasterKey

    nextMasterKeyForNode = this.nmTokenSecretManager.getNextKey();
    if (nextMasterKeyForNode != null
        && (request.getLastKnownNMTokenMasterKey().getKeyId() 
            != nextMasterKeyForNode.getKeyId())) {
      nodeHeartBeatResponse.setNMTokenMasterKey(nextMasterKeyForNode);
    }
  }

  /**
   * resolving the network topology.
   * @param hostName the hostname of this node.
   * @return the resolved {@link Node} for this nodemanager.
   */
  public static Node resolve(String hostName) {
    return RackResolver.resolve(hostName);
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
}
