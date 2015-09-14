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
package com.linkedin.pinot.tools.admin.command;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.*;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.*;

public class ShowClusterInfoCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShowClusterInfoCommand.class.getName());

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster clusterName.")
  private String _clusterName = DEFAULT_CLUSTER_NAME;

  @Option(name = "-tables", required = false, metaVar = "<String>", usage = "Comma separated table names.")
  private String _tables = "";

  @Option(name = "-tags", required = false, metaVar = "<String>", usage = "Commaa separated tag names.")
  private String _tags = "";

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean execute() throws Exception {
    Set<String> includeTableSet = new HashSet<>();
    String[] includeTables = _tables.split(",");
    for (String includeTable : includeTables) {
      String name = stripTypeFromName(includeTable.trim());
      if (name.length() > 0) {
        includeTableSet.add(name);
      }
    }
    Set<String> includeTagSet = new HashSet<>();
    String[] includeTags = _tags.split(",");
    for (String includeTag : includeTags) {
      String name = stripTypeFromName(includeTag.trim());
      if (name.length() > 0) {
        includeTagSet.add(name);
      }
    }

    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.clusterName = _clusterName;

    ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(_zkAddress);
    List<String> instancesInCluster = zkHelixAdmin.getInstancesInCluster(_clusterName);

    List<String> tables = zkHelixAdmin.getResourcesInCluster(_clusterName);
    ZkClient zkClient = new ZkClient(_zkAddress);
    zkClient.setZkSerializer(new ZNRecordStreamingSerializer());
    LOGGER.info("Connecting to Zookeeper at: {}", _zkAddress);
    zkClient.waitUntilConnected();
    ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    ZKHelixDataAccessor zkHelixDataAccessor = new ZKHelixDataAccessor(_clusterName, baseDataAccessor);
    PropertyKey property = zkHelixDataAccessor.keyBuilder().liveInstances();
    List<String> liveInstances = zkHelixDataAccessor.getChildNames(property);

    PropertyKey controllerLeaderKey = zkHelixDataAccessor.keyBuilder().controllerLeader();
    LiveInstance controllerLeaderLiveInstance = zkHelixDataAccessor.getProperty(controllerLeaderKey);
    ControllerInfo controllerInfo = new ControllerInfo();
    controllerInfo.leaderName = controllerLeaderLiveInstance.getId();
    clusterInfo.controllerInfo = controllerInfo;
    for (String server : instancesInCluster) {
      if (server.startsWith("Server")) {
        ServerInfo serverInfo = new ServerInfo();
        serverInfo.name = server;
        serverInfo.state = (liveInstances.contains(server)) ? "ONLINE" : "OFFLINE";
        InstanceConfig config = zkHelixAdmin.getInstanceConfig(_clusterName, server);
        serverInfo.tags = config.getRecord().getListField("TAG_LIST");
        clusterInfo.addServerInfo(serverInfo);
      }
      if (server.startsWith("Broker")) {
        BrokerInfo brokerInfo = new BrokerInfo();
        brokerInfo.name = server;
        brokerInfo.state = (liveInstances.contains(server)) ? "ONLINE" : "OFFLINE";
        InstanceConfig config = zkHelixAdmin.getInstanceConfig(_clusterName, server);
        brokerInfo.tags = config.getRecord().getListField("TAG_LIST");
        clusterInfo.addBrokerInfo(brokerInfo);
      }
    }
    for (String table : tables) {
      if ("brokerResource".equalsIgnoreCase(table)) {
        continue;
      }

      TableInfo tableInfo = new TableInfo();
      IdealState idealState = zkHelixAdmin.getResourceIdealState(_clusterName, table);
      ExternalView externalView = zkHelixAdmin.getResourceExternalView(_clusterName, table);
      Set<String> segmentsFromIdealState = idealState.getPartitionSet();

      tableInfo.tableName = table;
      tableInfo.tag = idealState.getRecord().getSimpleField("INSTANCE_GROUP_TAG");
      String rawTableName = stripTypeFromName(tableInfo.tableName);
      String rawTagName = stripTypeFromName(tableInfo.tag);

      if (!includeTableSet.isEmpty() && !includeTableSet.contains(rawTableName)) {
        continue;
      }

      if (!includeTagSet.isEmpty() && !includeTagSet.contains(rawTagName)) {
        continue;
      }
      for (String segment : segmentsFromIdealState) {
        SegmentInfo segmentInfo = new SegmentInfo();
        segmentInfo.name = segment;
        Map<String, String> serverStateMapFromIS = idealState.getInstanceStateMap(segment);
        Map<String, String> serverStateMapFromEV = externalView.getStateMap(segment);
        for (String serverName : serverStateMapFromIS.keySet()) {
          segmentInfo.segmentStateMap.put(serverName, serverStateMapFromEV.get(serverName));
        }
        tableInfo.addSegmentInfo(segmentInfo);
      }
      clusterInfo.addTableInfo(tableInfo);
    }
    Yaml yaml = new Yaml();
    StringWriter sw = new StringWriter();
    yaml.dump(clusterInfo, sw);
    LOGGER.info(sw.toString());
    return false;
  }

  private String stripTypeFromName(String tableName) {
    return tableName.replace("_OFFLINE", "").replace("_REALTIME", "");
  }

  @Override
  public String description() {
    return "Show Pinot Cluster information.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    return ("ShowClusterInfo -clusterName " + _clusterName + " -zkAddress " + _zkAddress
        + " -tables " + _tables + " -tags " + _tags);
  }

  class SegmentInfo {
    public String name;
    public Map<String, String> segmentStateMap = new HashMap<String, String>();

  }

  class TableInfo {
    public String tableName;
    public String tag;
    public List<SegmentInfo> segmentInfoList = new ArrayList<SegmentInfo>();

    public void addSegmentInfo(SegmentInfo segmentInfo) {
      segmentInfoList.add(segmentInfo);
    }
  }

  class ServerInfo {
    public String name;
    public List<String> tags;
    public String state;
  }

  class BrokerInfo {
    public String name;
    public List<String> tags;
    public String state;

  }

  class ControllerInfo {

    public String leaderName;

  }

  class ClusterInfo {
    public ControllerInfo controllerInfo;
    public List<BrokerInfo> brokerInfoList = new ArrayList<>();
    public List<ServerInfo> serverInfoList = new ArrayList<>();
    public List<TableInfo> tableInfoList = new ArrayList<>();

    public String clusterName;

    public void addServerInfo(ServerInfo serverInfo) {
      serverInfoList.add(serverInfo);
    }

    public void addTableInfo(TableInfo tableInfo) {
      tableInfoList.add(tableInfo);
    }

    public void addBrokerInfo(BrokerInfo brokerInfo) {
      brokerInfoList.add(brokerInfo);
    }
  }
}
