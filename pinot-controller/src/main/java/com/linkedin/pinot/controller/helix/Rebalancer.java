/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.controller.helix.core.UAutoRebalancer;
import org.apache.helix.*;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.*;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * In this class , we rebuild many env parameters for UAutoRebalancer to calculate new idealstate
 */

public class Rebalancer {

    private static Logger logger = Logger.getLogger(Rebalancer.class);

    private static ZkClient _zkClient;
    private static UAutoRebalancer _rebalancer;

    public Rebalancer(String zkAddress){
         _zkClient = new ZkClient(zkAddress, 30 * 1000);
         _zkClient.setZkSerializer(new ZNRecordSerializer());
         _zkClient.waitUntilConnected(30, TimeUnit.SECONDS);
         _rebalancer = new UAutoRebalancer();
        // Here no need to init especially helixmanager
    }

    public void rebalance(String clusterName, String resourceName, int replica) {
        rebalance(clusterName, resourceName, replica, resourceName, "");
    }

    public void rebalance(String clusterName, String resourceName, int replica, String keyPrefix,
                          String group) {
        List<String> instanceNames = new LinkedList<String>();
        if (keyPrefix == null || keyPrefix.length() == 0) {
            keyPrefix = resourceName;
        }
        if (group != null && group.length() > 0) {
            instanceNames = getInstancesInClusterWithTag(clusterName, group);
        }
        if (instanceNames.size() == 0) {
            logger.info("No tags found for resource " + resourceName + ", use all instances");
            instanceNames = getInstancesInCluster(clusterName);
            group = "";
        } else {
            logger.info("Found instances with tag for " + resourceName + " " + instanceNames);
        }
        rebalance(clusterName, resourceName, replica, keyPrefix, instanceNames, group);
    }

    public void rebalance(String clusterName, String resourceName, int replica, List<String> instances) {
        rebalance(clusterName, resourceName, replica, resourceName, instances, "");
    }

    void rebalance(String clusterName, String resourceName, int replica, String keyPrefix,
                   List<String> instanceNames, String groupId) {
        // ensure we get the same idealState with the same set of instances
        Collections.sort(instanceNames);


        IdealState idealState = getResourceIdealState(clusterName, resourceName);
        if (idealState == null) {
            throw new HelixException("Resource: " + resourceName + " has NOT been added yet");
        }
//        logger.info("###ideal state is : "+idealState.toString());
        if (groupId != null && groupId.length() > 0) {
            idealState.setInstanceGroupTag(groupId);
        }
        idealState.setReplicas(Integer.toString(replica));
        int partitions = idealState.getNumPartitions();
        String stateModelName = idealState.getStateModelDefRef();
        StateModelDefinition stateModDef = getStateModelDef(clusterName, stateModelName);

        if (stateModDef == null) {
            throw new HelixException("cannot find state model: " + stateModelName);
        }
        // StateModelDefinition def = new StateModelDefinition(stateModDef);

        List<String> statePriorityList = stateModDef.getStatesPriorityList();

        String masterStateValue = null;
        String slaveStateValue = null;
        replica--;

        for (String state : statePriorityList) {
            String count = stateModDef.getNumInstancesPerState(state);
            if (count.equals("1")) {
                if (masterStateValue != null) {
                    throw new HelixException("Invalid or unsupported state model definition");
                }
                masterStateValue = state;
            } else if (count.equalsIgnoreCase("R")) {
                if (slaveStateValue != null) {
                    throw new HelixException("Invalid or unsupported state model definition");
                }
                slaveStateValue = state;
            } else if (count.equalsIgnoreCase("N")) {
                if (!(masterStateValue == null && slaveStateValue == null)) {
                    throw new HelixException("Invalid or unsupported state model definition");
                }
                replica = instanceNames.size() - 1;
                masterStateValue = slaveStateValue = state;
            }
        }
        if (masterStateValue == null && slaveStateValue == null) {
            throw new HelixException("Invalid or unsupported state model definition");
        }

        if (masterStateValue == null) {
            masterStateValue = slaveStateValue;
        }
        IdealState newIdealState = null;
        if (idealState.getRebalanceMode() != IdealState.RebalanceMode.FULL_AUTO
                && idealState.getRebalanceMode() != IdealState.RebalanceMode.USER_DEFINED) {
//            logger.info("#### Into rebalance mode");
//            logger.info("get live instance :"+getLiveInstances(clusterName));
//            logger.info("get instance config map :"+getInstanceConfigMap(clusterName));
//            logger.info("get current state output "+ computeCurrentStateOutput(clusterName));
             newIdealState = _rebalancer.computeNewIdealState(resourceName,idealState,stateModDef,
                     getLiveInstances(clusterName),getInstanceConfigMap(clusterName),computeCurrentStateOutput(clusterName));

        } else {
            for (int i = 0; i < partitions; i++) {
                String partitionName = keyPrefix + "_" + i;
                newIdealState.getRecord().setMapField(partitionName, new HashMap<String, String>());
                newIdealState.getRecord().setListField(partitionName, new ArrayList<String>());
            }
        }
//        logger.info("new ideal state is : "+ newIdealState.toString());
        setResourceIdealState(clusterName, resourceName, newIdealState);
    }

    /**
     * Get LiveInstances by clusterName
     * @param clusterName
     * @return
     */
    public Map<String,LiveInstance>  getLiveInstances(String clusterName){
        HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName,new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();
        return accessor.getChildValuesMap(keyBuilder.liveInstances());
    }

    /**
     * Get Instance Configs
     * @param clusterName
     * @return
     */
    public Map<String,InstanceConfig> getInstanceConfigMap(String clusterName){
        HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName,new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();
        return accessor.getChildValuesMap(keyBuilder.instanceConfigs());
    }

    /**
     * Get IdealState
     * @param clusterName
     * @return
     */
    public Map<String,IdealState> getIdealStates(String clusterName){
        HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName,new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();
        return accessor.getChildValuesMap(keyBuilder.idealStates());
    }

    /**
     * Get Resource IdealState
     * @param clusterName
     * @param resourceName
     * @return
     */
    public IdealState getResourceIdealState(String clusterName, String resourceName) {
        HelixDataAccessor accessor =
                new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        return accessor.getProperty(keyBuilder.idealStates(resourceName));
    }

    /**
     * Set Resource IdealState
     * @param clusterName
     * @param resourceName
     * @param idealState
     */
    public void setResourceIdealState(String clusterName, String resourceName, IdealState idealState) {
        HelixDataAccessor accessor =
                new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }

    /**
     * Get State model Def
     * @param clusterName
     * @param stateModelName
     * @return
     */
    public StateModelDefinition getStateModelDef(String clusterName, String stateModelName) {
        HelixDataAccessor accessor =
                new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        return accessor.getProperty(keyBuilder.stateModelDef(stateModelName));
    }

    /**
     * Get Instances with Tag
     * @param clusterName
     * @param tag
     * @return
     */
    public List<String> getInstancesInClusterWithTag(String clusterName, String tag) {
        String memberInstancesPath = HelixUtil.getMemberInstancesPath(clusterName);
        List<String> instances = _zkClient.getChildren(memberInstancesPath);
        List<String> result = new ArrayList<String>();

        HelixDataAccessor accessor =
                new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        for (String instanceName : instances) {
            InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
            if (config.containsTag(tag)) {
                result.add(instanceName);
            }
        }
        return result;
    }

    /**
     * Get all instances
     * @param clusterName
     * @return
     */
    public List<String> getInstancesInCluster(String clusterName) {
        String memberInstancesPath = HelixUtil.getMemberInstancesPath(clusterName);
        return _zkClient.getChildren(memberInstancesPath);
    }


    /**
     * Get Instance Message
     * @param clusterName
     * @param instanceName
     * @return
     */
    public Map<String, Message> getInstanceMessage (String clusterName,String instanceName){
        HelixDataAccessor accessor =
                new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        return accessor.getChildValuesMap(keyBuilder.messages(instanceName));
    }


    /**
     * Get Instance CurrentState
     * @param clusterName
     * @param instanceName
     * @param clientSessionId
     * @return
     */
    public Map<String, CurrentState> getCurrentState (String clusterName,String instanceName,String clientSessionId){
        HelixDataAccessor accessor =
                new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        return accessor.getChildValuesMap(keyBuilder.currentStates(instanceName, clientSessionId));
    }

    /**
     * Get Resource Map
     * @param clusterName
     * @return
     */
    public Map<String,Resource> getResourceMap(String clusterName){
        Map<String, IdealState> idealStates = getIdealStates(clusterName);

        Map<String, Resource> resourceMap = new LinkedHashMap<String, Resource>();

        if (idealStates != null && idealStates.size() > 0) {
            for (IdealState idealState : idealStates.values()) {
                Set<String> partitionSet = idealState.getPartitionSet();
                String resourceName = idealState.getResourceName();
                if (!resourceMap.containsKey(resourceName)) {
                    Resource resource = new Resource(resourceName);
                    resourceMap.put(resourceName, resource);
                    resource.setStateModelDefRef(idealState.getStateModelDefRef());
                    resource.setStateModelFactoryName(idealState.getStateModelFactoryName());
                    resource.setBucketSize(idealState.getBucketSize());
                    resource.setBatchMessageMode(idealState.getBatchMessageMode());
                }

                for (String partition : partitionSet) {
                    addPartition(partition, resourceName, resourceMap);
                }
            }
        }

        Map<String, LiveInstance> availableInstances = getLiveInstances(clusterName);

        if (availableInstances != null && availableInstances.size() > 0) {
            for (LiveInstance instance : availableInstances.values()) {
                String instanceName = instance.getInstanceName();
                String clientSessionId = instance.getSessionId();

                Map<String, CurrentState> currentStateMap =
                        getCurrentState(clusterName, instanceName, clientSessionId);
                if (currentStateMap == null || currentStateMap.size() == 0) {
                    continue;
                }
                for (CurrentState currentState : currentStateMap.values()) {

                    String resourceName = currentState.getResourceName();
                    Map<String, String> resourceStateMap = currentState.getPartitionStateMap();

                    if (resourceStateMap.keySet().isEmpty()) {
                        // don't include empty current state for dropped resource
                        continue;
                    }

                    // don't overwrite ideal state settings
                    if (!resourceMap.containsKey(resourceName)) {
                        addResource(resourceName, resourceMap);
                        Resource resource = resourceMap.get(resourceName);
                        resource.setStateModelDefRef(currentState.getStateModelDefRef());
                        resource.setStateModelFactoryName(currentState.getStateModelFactoryName());
                        resource.setBucketSize(currentState.getBucketSize());
                        resource.setBatchMessageMode(currentState.getBatchMessageMode());
                    }

                    if (currentState.getStateModelDefRef() == null) {
                        logger.error("state model def is null." + "resource:" + currentState.getResourceName()
                                + ", partitions: " + currentState.getPartitionStateMap().keySet() + ", states: "
                                + currentState.getPartitionStateMap().values());
                        try {
                            throw new StageException("State model def is null for resource:"
                                    + currentState.getResourceName());
                        } catch (StageException e) {
                            e.printStackTrace();
                        }
                    }

                    for (String partition : resourceStateMap.keySet()) {
                        addPartition(partition, resourceName, resourceMap);
                    }
                }
            }
        }
        return resourceMap;
    }

    private void addResource(String resource, Map<String, Resource> resourceMap) {
        if (resource == null || resourceMap == null) {
            return;
        }
        if (!resourceMap.containsKey(resource)) {
            resourceMap.put(resource, new Resource(resource));
        }
    }

    private void addPartition(String partition, String resourceName, Map<String, Resource> resourceMap) {
        if (resourceName == null || partition == null || resourceMap == null) {
            return;
        }
        if (!resourceMap.containsKey(resourceName)) {
            resourceMap.put(resourceName, new Resource(resourceName));
        }
        Resource resource = resourceMap.get(resourceName);
        resource.addPartition(partition);

    }

    /**
     * Compute CurrentStateOutput , the same as CurrentStateComputationStage.java
     * @param clusterName
     * @return
     */
    public CurrentStateOutput computeCurrentStateOutput(String clusterName){

        Map<String, Resource> resourceMap = getResourceMap(clusterName);
        Map<String, LiveInstance> liveInstances = getLiveInstances(clusterName);
        CurrentStateOutput currentStateOutput = new CurrentStateOutput();

        for (LiveInstance instance : liveInstances.values()) {
            String instanceName = instance.getInstanceName();
            Map<String, Message> instanceMessages = getInstanceMessage(clusterName,instanceName);
            for (Message message : instanceMessages.values()) {
                if (!Message.MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())) {
                    continue;
                }
                if (!instance.getSessionId().equals(message.getTgtSessionId())) {
                    continue;
                }
                String resourceName = message.getResourceName();
                    Resource resource = resourceMap.get(resourceName);
                if (resource == null) {
                    continue;
                }
                if (!message.getBatchMessageMode()) {
                    String partitionName = message.getPartitionName();
                    Partition partition = resource.getPartition(partitionName);
                    if (partition != null) {
                        currentStateOutput.setPendingState(resourceName, partition, instanceName, message);
                    } else {
                           // log
                    }
                } else {
                    List<String> partitionNames = message.getPartitionNames();
                    if (!partitionNames.isEmpty()) {
                        for (String partitionName : partitionNames) {
                            Partition partition = resource.getPartition(partitionName);
                            if (partition != null) {
                                currentStateOutput.setPendingState(resourceName, partition, instanceName, message);
                            } else {
                                   // log
                            }
                        }
                    }
                }
            }
        }
        for (LiveInstance instance : liveInstances.values()) {
            String instanceName = instance.getInstanceName();
            String clientSessionId = instance.getSessionId();
            Map<String, CurrentState> currentStateMap =getCurrentState(clusterName,instanceName,clientSessionId);
            for (CurrentState currentState : currentStateMap.values()) {

                if (!instance.getSessionId().equals(currentState.getSessionId())) {
                    continue;
                }
                String resourceName = currentState.getResourceName();
                String stateModelDefName = currentState.getStateModelDefRef();
                Resource resource = resourceMap.get(resourceName);
                if (resource == null) {
                    continue;
                }
                if (stateModelDefName != null) {
                    currentStateOutput.setResourceStateModelDef(resourceName, stateModelDefName);
                }
                currentStateOutput.setBucketSize(resourceName, currentState.getBucketSize());
                Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
                for (String partitionName : partitionStateMap.keySet()) {
                    Partition partition = resource.getPartition(partitionName);
                    if (partition != null) {
                        currentStateOutput.setCurrentState(resourceName, partition, instanceName,
                                currentState.getState(partitionName));
                        currentStateOutput.setRequestedState(resourceName, partition, instanceName,
                                currentState.getRequestedState(partitionName));
                        currentStateOutput.setInfo(resourceName, partition, instanceName,
                                currentState.getInfo(partitionName));
                    }
                }
            }
        }
        return currentStateOutput;
    }
}


