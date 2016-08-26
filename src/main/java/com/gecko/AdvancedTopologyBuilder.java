package com.gecko;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gecko.bolt.AdvancedBolt;
import com.gecko.spouts.AdvancedSpout;

/**
 * Created by hlieu on 08/22/16.
 */
public class AdvancedTopologyBuilder {

    private static TopologyBuilder BUILDER;

    // my topology
    static {
        BUILDER = new TopologyBuilder();
        BUILDER.setSpout("advanced-spout", new AdvancedSpout());
        BUILDER.setBolt("advanced-bolt", new AdvancedBolt()).fieldsGrouping("advanced-spout", new Fields("some-value"));
    }

    public static StormTopology build() {
        return BUILDER.createTopology();
    }

}

