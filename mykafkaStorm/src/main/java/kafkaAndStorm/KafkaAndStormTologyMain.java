package kafkaAndStorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by xiaoquan on 2017/2/22.
 */
public class KafkaAndStormTologyMain {


    public  static  void main(String[]args)
    {

        TopologyBuilder topologyBuilder=new TopologyBuilder();
        //设置SPout的实例，并且并发度设置为1,里面的KafkaSpout是Storm提供了整合Kafka的jar 包
        //配置好Spout的并发度，和zookeeper
        topologyBuilder.setSpout("kafkaSpout",
                new KafkaSpout(new SpoutConfig(new ZkHosts("namenode:2181,shizhan2.com:2181,shizhan3.com:2181"),"orderMq","/myKafka","kafkaSpout")),

                1);
      topologyBuilder.setBolt("mybolt1",new MyKafkaBolt(),1).shuffleGrouping("kafkaSpout");

      Config config=new Config();
      config.setNumWorkers(1);
      //  LocalCluster localCluster=new LocalCluster();
       // localCluster.submitTopology("mywordcoutn",config,topologyBuilder.createTopology());
//根据传入的参数来决定运行本地模式还是集群模式。
        if (args.length>0) {
            try {
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("storm2kafka", config, topologyBuilder.createTopology());
        }

        }
}
