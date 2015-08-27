package com.linkedin.pinot.tools.admin.command;


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



        return false;
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
