package com.gecko;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

/**
 * Created by hlieu on 08/22/16.
 */
public class AdvancedTopologyRunner {

    private static String TOPOLOGY_NAME = "Advanced-Topology";

    private static int FIVE_SECS = 5 * 1000;
    private static int ONE_MIN = 60 * 1000;
    private static int SLEEP_CYCLE = ONE_MIN;

    public static void main(String[] args) {

        Config config = new Config();
        config.put("inputFile", "address.txt");

        StormTopology topology = AdvancedTopologyBuilder.build();


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( TOPOLOGY_NAME, config, topology);
        Utils.sleep(SLEEP_CYCLE);

        cluster.killTopology( TOPOLOGY_NAME );
        cluster.shutdown();
    }
}
