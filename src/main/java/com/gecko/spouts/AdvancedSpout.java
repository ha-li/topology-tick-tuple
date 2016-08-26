package com.gecko.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * Created by hlieu on 08/22/16.
 */
public class AdvancedSpout implements IRichSpout {
    private BufferedReader bf = null;
    private SpoutOutputCollector collector = null;

    public void activate() {}
    public void deactivate() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("some-value"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void close() {}
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        URL url = ClassLoader.getSystemResource((String)config.get("inputFile"));

        try {
            if (url == null) {
                throw new FileNotFoundException("The input file could not be found.");
            }

            bf = new BufferedReader(new FileReader(url.getFile()));

        } catch (FileNotFoundException fnf) {
            fnf.printStackTrace();
        }

    }

    public void nextTuple() {

        String line = null;
        try {
            while ((line = bf.readLine()) != null) {
                System.out.println("line : " + line);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void ack(Object msgId) {}
    public void fail(Object msgId) {}
}
