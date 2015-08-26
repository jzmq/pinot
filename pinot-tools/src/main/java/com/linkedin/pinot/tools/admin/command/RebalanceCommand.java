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


    @Option(name = "-replicas", required = true, metaVar = "<int>", usage = "Replica Number of Resource Partitions.")
    private int _replicas = 1;


    @Option(name = "-partitionPrefix", required = true, metaVar = "<String>", usage = "Partition Prefix of Resource")
    private String _partitionPrefix = "";



    @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
            usage = "Print this message.")
    private boolean _help = false;

    @Override
    public boolean execute() throws Exception {
        return false;
    }

    @Override
    public void printUsage() {

    }

    @Override
    public String description() {
        return null;
    }



    @Override
    public boolean getHelp() {
        return _help;
    }
}
