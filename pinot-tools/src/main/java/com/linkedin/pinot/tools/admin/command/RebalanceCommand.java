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


import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.controller.helix.Rebalancer;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */

public class RebalanceCommand extends AbstractBaseCommand implements Command {

    private static final Logger LOGGER = LoggerFactory.getLogger(RebalanceCommand.class);


    @Option(name = "-zkAddress", required = true , metaVar = "<http>", usage = "HTTP address of Zookeeper.")
    private String _zkAddress = DEFAULT_ZK_ADDRESS;


    @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Pinot cluster name.")
    private String _clusterName = "PinotCluster";


    @Option(name = "-tableName", required = true, metaVar = "<String>", usage = "Offline Table Name.")
    private String _resourceName = "";


    @Option(name = "-replicas", required = false, metaVar = "<int>", usage = "Replica Number of Resource Partitions.")
    private int _replicas = 1;


    @Option(name = "-partitionPrefix", required = true, metaVar = "<String>", usage = "Partition Prefix of Resource")
    private String _partitionPrefix = "";



    @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
            usage = "Print this message.")
    private boolean _help = false;


    public RebalanceCommand setZkAddress(String zkAddress){
        _zkAddress = zkAddress;
        return this;
    }

    public RebalanceCommand setClusterName(String clusterName){
        _clusterName = clusterName;
        return this;
    }

    public RebalanceCommand setTableName(String tableName){
        _resourceName = tableName;
        return this;
    }

    public RebalanceCommand setPartitionPrefix(String partitionPrefix){
        _partitionPrefix = partitionPrefix;
        return this;
    }

    public RebalanceCommand setReplicas(int replicas){
        _replicas = replicas;
        return this;
    }


    @Override
    public boolean execute() throws Exception {
        Rebalancer rebalancer = new Rebalancer(_zkAddress);
        final String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(_resourceName);
        rebalancer.rebalance(_clusterName,offlineTableName,_replicas,_partitionPrefix,"");
        return true;
    }

    @Override
    public String getName(){
        return "Rebalance";
    }

    @Override
    public String description() {
        return "Balance Resources By CommandLine";
    }

    @Override
    public boolean getHelp() {
        return _help;
    }

    @Override
    public String toString(){
        return "Start Rebalance Resource -zkAddress "+ _zkAddress + " -clusterName " + _clusterName + " -tableName "+_resourceName+" -replicas "+ _replicas + " -partitionPrefix "+ _partitionPrefix ;
    }
}
