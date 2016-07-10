package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PlatFormMegerBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1557894564564L;
	OutputCollector collector;
	Map<Long, String> tp0 = new HashMap<Long, String>();
	Map<Long, String> tp1 = new HashMap<Long, String>();
	List<String> tmplist = new ArrayList<String>();
	private static final String sep = ":";

	@Override
	public void execute(Tuple tuple) {

		List<String> lists = (List<String>) tuple.getValue(0);
		if (lists == null)
			return;
		tmplist.clear();
		tp0.clear();
		tp1.clear();
		String[] arr = (String[]) lists.toArray(new String[lists.size()]);
		if (arr.length == 0)
			return;
		for (String str : arr) {
			if (RaceConfig.pay_end.equals(str)) {
				tmplist.add(str);
				// RaceUtils.method1_WriteText("----PlatFormMeger--rev-----" +
				// str);
				continue;
			}
			String data[] = str.split(sep);
			String pt = data[0];
			String time = data[1];
			long createtime = (Long.parseLong(time) / 1000) * 1000;
			Date d = new Date(createtime);
			long timeStamp = createtime - d.getSeconds() * 1000;
			timeStamp = timeStamp / 1000;
			double totalPrice = Double.parseDouble(data[2]);
			String v0 = tp0.get(timeStamp);
			String v1 = tp1.get(timeStamp);
			// 平台相同才可以进行金额的加减
			if (v0 == null && "0".equals(pt))
				tp0.put(timeStamp, pt + sep + totalPrice);
			else if (v1 == null && "1".equals(pt))
				tp1.put(timeStamp, pt + sep + totalPrice);
			else {
				if ("0".equals(pt)) {
					String values[] = v0.split(sep);
					double total = totalPrice + Double.parseDouble(values[1]);
					tp0.put(timeStamp, values[0] + sep + total);

				}
				if ("1".equals(pt)) {
					String values[] = v1.split(sep);
					double total = totalPrice + Double.parseDouble(values[1]);
					tp1.put(timeStamp, values[0] + sep + total);

				}

			}

		}

		for (Long l : tp0.keySet()) {
			tmplist.add(l + ":" + tp0.get(l));
		}
		collector.emit(tuple, new Values(new ArrayList(tmplist)));

		tmplist.clear();

		for (Long l : tp1.keySet()) {
			tmplist.add(l + ":" + tp1.get(l));
		}
		collector.emit(tuple, new Values(new ArrayList(tmplist)));
		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tmplist"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// tairOperator = new TairOperatorImpl();
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

	// public boolean writeData(long timestamp, Number str) {
	// String key = RaceConfig.prex_ratio + RaceConfig.teamcode + timestamp;
	// RaceUtils.method1_WriteText(key + ":" + str);
	// return tairOperator.put(key, str);
	//
	// }
}