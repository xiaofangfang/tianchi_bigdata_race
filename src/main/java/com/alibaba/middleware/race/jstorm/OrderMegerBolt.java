package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderIds;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderMegerBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 15678978978978L;
	OutputCollector collector;
	private static Logger LOG = LoggerFactory.getLogger(OrderMegerBolt.class);
	private transient TairOperatorImpl tairOperator;
	private static final String seq = ":";
	private List<String> tmpOrders = new ArrayList<String>();
	private Map<Long, Double> tm_total = new HashMap<Long, Double>();
	private Map<Long, Double> tb_total = new HashMap<Long, Double>();

	private long getChouTime(String str) {
		long time = Long.parseLong(str);
		long createtime = (time / 1000) * 1000;
		Date d = new Date(createtime);
		time = createtime - d.getSeconds() * 1000;
		time = time / 1000;
		return time;
	}

	@Override
	public void execute(Tuple tuple) {
		String streamid = tuple.getSourceStreamId();
		List<String> lists = (List<String>) tuple.getValue(0);
		if (lists == null)
			return;
		if (tmpOrders.size() > 0) {
			lists.addAll(tmpOrders);
			tmpOrders.clear();
		}
		String[] arr = (String[]) lists.toArray(new String[lists.size()]);
		if (RaceConfig.ORDERID_STREAM_ID.equals(streamid)) {
			List<String> noprce = new ArrayList<String>();
			for (String str : arr) {
				String data[] = str.split(seq);
				// 获取金额
				Double price = Double.parseDouble(data[2]);
				// 获取时间
				long time = getChouTime(data[1]);
				// 获取订单
				long orderid = Long.parseLong(data[3]);
				// 判断订单的来源
				String value = OrderIds.containOrderId(orderid);
				if (RaceConfig.TM_flag.equals(value)) {
					Double _tmpResult = tm_total.get(time);
					if (_tmpResult == null)
						tm_total.put(time, price);
					else
						tm_total.put(time, _tmpResult + price);
				}
				if (RaceConfig.TB_flag.equals(value)) {
					Double _tmpResult = tb_total.get(time);
					if (_tmpResult == null)
						tb_total.put(time, price);
					else
						tb_total.put(time, _tmpResult + price);
					// Log.info("----------tb----------" + tm_total);
				}
				// 如果该订单暂时在内存中 未找到，保存起来，下次使用
				if (value == null) {
					noprce.add(str);
					LOG.error("------get orderid=null from orderids  orderid=" + orderid + ":" + value);
				}

			}
			tmpOrders.addAll(noprce);
		}
		collector.emit(tuple, new Values(RaceConfig.TM_flag, new HashMap(tm_total)));
		collector.emit(tuple, new Values(RaceConfig.TB_flag, new HashMap(tb_total)));
		collector.ack(tuple);
		tm_total.clear();
		tb_total.clear();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pt", "payInfolist"));
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

//	public boolean writeData(long timestamp, Number str, String pt) {
//		if (tairOperator == null)
//			tairOperator = new TairOperatorImpl();
//		String key = null;
//		if ("tm".equals(pt))
//			key = RaceConfig.prex_tmall + RaceConfig.teamcode + timestamp;
//		else
//			key = RaceConfig.prex_taobao + RaceConfig.teamcode + timestamp;
//		RaceUtils.method1_WriteText(key + ":" + str);
//		return tairOperator.write(key, str);
//
//	}

}