package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.aloha.meta.MetaTuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderIds;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TMOrderIdBolt implements IRichBolt {

	private static final long serialVersionUID = 154654564654654L;
	OutputCollector collector;
	//aprivate final long div = 5000;
	private Map<Long, List<Long>> tm = new HashMap<Long, List<Long>>();

	@Override
	public void execute(Tuple tuple) {
		MetaTuple metas = (MetaTuple) tuple.getValue(0);
		if (metas == null)
			return;
		for (MessageExt msg : metas.getMsgList()) {
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				tm.put(RaceConfig.orderId_end, null);
				continue;
			}
			OrderMessage paymentMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
			long id = paymentMessage.getOrderId();
			long l = id % RaceConfig.div;
			List<Long> list = tm.get(l);
			if (list == null)
				list = new ArrayList<Long>();
			list.add(id);
			tm.put(l, list);
		}
		OrderIds.add(tm, RaceConfig.TM_flag);
		collector.ack(tuple);
		tm.clear();
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("tm", "orderid"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// tairOperator = new TairOperatorImpl();
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean writeData(String pt, Map<Long, List<Long>> tm2) {
		// BillCount bill = new BillCount();
		// bill.setOrderIds(tm2);
		// String key = RaceConfig.teamcode;
		// RaceUtils.method1_WriteText(key + ":" + pt);
		// return tairOperator.write(key, bill);
		return true;

	}

}