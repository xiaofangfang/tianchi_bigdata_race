package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.aloha.meta.MetaTuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.PlatFromCount;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PayMentBolt implements IRichBolt {

	private static final long serialVersionUID = 146464324798431321L;
	private static Logger LOG = LoggerFactory.getLogger(PayMentBolt.class);
	OutputCollector collector;

	// 该时间段内的PaymentMessage
	List<PaymentMessage> orderIdlists = new ArrayList<PaymentMessage>();
	Map<String, PlatFromCount> plats = new HashMap<String, PlatFromCount>();
	private List<String> payInfolist = new ArrayList<String>();
	// private static final String end_flag = "end";

	@Override
	public void execute(Tuple tuple) {
		MetaTuple metas = (MetaTuple) tuple.getValue(0);
		if (metas == null)
			return;
		for (MessageExt msg : metas.getMsgList()) {
			String str = null;
			byte[] body = msg.getBody();
			if (body.length == 0)
				continue;
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				str = RaceConfig.pay_end;
				//RaceUtils.method1_WriteText("--- payment rev endflag---" + str);
				payInfolist.add(str);
				continue;
			}
			PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
			str = paymentMessage.getPayPlatform() + ":" + paymentMessage.getCreateTime() + ":"
					+ paymentMessage.getPayAmount() + ":" + paymentMessage.getOrderId();
			payInfolist.add(str);
		}
		//RaceUtils.method1_WriteText("----send paylist---size--"+payInfolist.size());
		collector.emit(RaceConfig.PAY_STREAM_ID, tuple, new Values(new ArrayList(payInfolist)));
		if (payInfolist.contains(RaceConfig.pay_end))
			payInfolist.remove(RaceConfig.pay_end);
		//RaceUtils.method1_WriteText("-----send--list -size------" +payInfolist.size());
		collector.emit(RaceConfig.ORDERID_STREAM_ID, tuple, new Values(new ArrayList(payInfolist)));
		collector.ack(tuple);
		payInfolist.clear();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(RaceConfig.PAY_STREAM_ID, new Fields("payInfolist"));
		declarer.declareStream(RaceConfig.ORDERID_STREAM_ID, new Fields("payInfolist"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
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
}
