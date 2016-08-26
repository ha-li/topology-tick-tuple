package com.gecko.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by hlieu on 08/22/16.
 */
public class AdvancedBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return config;
    }

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }



    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();

        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    public void execute(Tuple tuple) {
        if(isTickTuple(tuple)) {
            System.out.println("Tick Tuple detected.");
        } else {
            System.out.println("Processing tuple.");
        }
    }
}
