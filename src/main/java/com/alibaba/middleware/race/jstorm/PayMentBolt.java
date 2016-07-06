package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Date;
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
import com.alibaba.middleware.race.model.TradeCountAll;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class PayMentBolt implements IRichBolt {

	private static final long serialVersionUID = 146464324798431321L;
	private static Logger LOG = LoggerFactory.getLogger(PayMentBolt.class);
	OutputCollector collector;

	// 该时间段内的PaymentMessage
	List<PaymentMessage> orderIdlists = new ArrayList<PaymentMessage>();
	Map<String, PlatFromCount> plats = new HashMap<String, PlatFromCount>();
	final int millSec = 1000;
	Long begintime;
	boolean firtTime = false;
	// String value = "value";
	private static final String s1 = "1";
	private static final String s0 = "0";

	boolean orderTimeinit = false;
	long orderBegintime;

	private static final long oneMinut = 60000;

	private void calPays(Long createTime, PaymentMessage pay) {
		short platInfo = pay.getPayPlatform();
		PlatFromCount p = plats.get(platInfo + "");
		if (p == null) {
			p = new PlatFromCount();
			p.setTimeStamp(createTime);
			p.setMiddleStamp(createTime - oneMinut);
			p.setLastStmap(createTime - 2 * oneMinut);
			p.setPlatFrom(platInfo);
			p.setTotalprice(pay.getPayAmount());
			p.setLastTotalPrice(0);
			p.setMiddleTotalPrice(0);
		}
		if (createTime > p.getTimeStamp()) {
			collector.emit(RaceConfig.PAY_STREAM_ID, new Values(p));
			p = new PlatFromCount();
			p.setTimeStamp(createTime);
			p.setMiddleStamp(createTime - oneMinut);
			p.setLastStmap(createTime - 2 * oneMinut);
			p.setPlatFrom(platInfo);
			p.setTotalprice(pay.getPayAmount());
			p.setLastTotalPrice(0);
			p.setMiddleTotalPrice(0);
		}
		if(createTime < p.getTimeStamp())
		{
			if(createTime==p.getMiddleStamp())
			{
				p.setMiddleTotalPrice(p.getMiddleTotalPrice()+pay.getPayAmount());
			}
			if(createTime==p.getLastStmap())
			{
				p.setLastTotalPrice(p.getLastTotalPrice()+pay.getPayAmount());
			}
			plats.put(platInfo + "", p);
			return;
			
		}
		p.setTotalprice(p.getTotalprice() + pay.getPayAmount());
		plats.put(platInfo + "", p);

	}

	public void emitObject(Tuple tuple, PaymentMessage pay) {

		// double py = pay.getPayAmount();
		long crateTime = pay.getCreateTime();
		// 求该时间的整分时间
		long time = (crateTime / millSec) * millSec;
		Date d = new Date(time);
		time = time - d.getSeconds() * 1000;
		if (!firtTime) {
			begintime = time;
			firtTime = true;
		}
		// 无线和PC交易量计算
		calPays(time, pay);

		if (!orderTimeinit) {
			orderTimeinit = true;
			orderBegintime = time;
		}
		if (time > orderBegintime) {

			orderBegintime = time;
			orderIdlists.clear();
		}

	}

	@Override
	public void execute(Tuple tuple) {

		MetaTuple metas = (MetaTuple) tuple.getValue(0);
		for (MessageExt msg : metas.getMsgList()) {
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				continue;
			}
			PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
			collector.emit(RaceConfig.ORDERID_STREAM_ID, tuple, new Values(paymentMessage));
			emitObject(tuple, paymentMessage);

		}
		collector.emit(RaceConfig.PAY_STREAM_ID, tuple, new Values(plats.get(s1)));
		collector.emit(RaceConfig.PAY_STREAM_ID, tuple, new Values(plats.get(s0)));
		plats.remove(s1);
		plats.remove(s0);
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(RaceConfig.PAY_STREAM_ID, new Fields("platFrom"));
		declarer.declareStream(RaceConfig.ORDERID_STREAM_ID, new Fields("paySource"));
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
