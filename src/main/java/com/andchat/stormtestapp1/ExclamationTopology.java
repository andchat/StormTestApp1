/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.andchat.stormtestapp1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 *
 * @author s0902901
 */
public class ExclamationTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("1", new TestWordSpout(), 10);
        builder.setBolt("2", new ExclamationBolt(), 3)
                .shuffleGrouping("1");
        builder.setBolt("3", new ExclamationBolt(), 2)
                .shuffleGrouping("2");
        
        // Create a local cluster
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }    
}
