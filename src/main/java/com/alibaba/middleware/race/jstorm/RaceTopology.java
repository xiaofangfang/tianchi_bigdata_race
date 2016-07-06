package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.
 * RaceTopology； 所以这个主类路径一定要正确
 */
public class RaceTopology {

	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

	public static void main(String[] args) throws Exception {

		// Properties properties = new Properties();
		// properties.load(new
		// FileInputStream("./src/main/resources/metaspout.yaml"));
		// Map<String, String> conf = new HashMap<String, String>((Map)
		// properties);
//		int spout_Parallelism_hint = 1;
//		int split_Parallelism_hint = 2;
//		int count_Parallelism_hint = 1;

		Config conf = new Config();
		conf.setDebug(false);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tb_spout", new RacePayMentSpout(RaceConfig.MqTaobaoTradeTopic), 1);
		builder.setSpout("tm_spout", new RacePayMentSpout(RaceConfig.MqTmallTradeTopic), 1);
		builder.setSpout("pay_spout", new RacePayMentSpout(RaceConfig.MqPayTopic), 1);
		
		builder.setBolt("tb_bolt", new OrderIdBolt(), 2).shuffleGrouping("tb_spout");
		builder.setBolt("tm_bolt", new OrderIdBolt(), 2).shuffleGrouping("tm_spout");
		
		builder.setBolt("py_1", new PayMentBolt(), 2).fieldsGrouping("pay_spout", new Fields("payment"));;
		builder.setBolt("py_2", new PlatFormMegerBolt(), 1).shuffleGrouping("py_1",RaceConfig.PAY_STREAM_ID);
		builder.setBolt("py_3", new OrderSourceCountBolt(), 2).shuffleGrouping("tb_bolt").shuffleGrouping("tm_bolt").shuffleGrouping("py_1",RaceConfig.ORDERID_STREAM_ID);
		builder.setBolt("count", new CountAllBolt(), 1).fieldsGrouping("py_3",new Fields("orderSource"));
		String topologyName = RaceConfig.JstormTopologyName;

		try {
			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology(topologyName, conf, builder.createTopology());
			Utils.sleep(10000000);
			cluster.killTopology(topologyName);
			cluster.shutdown();
			// StormSubmitter.submitTopology(topologyName, conf,
			// builder.createTopology());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}