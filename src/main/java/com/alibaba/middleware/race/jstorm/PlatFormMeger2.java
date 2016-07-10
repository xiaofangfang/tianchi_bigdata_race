package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.emory.mathcs.backport.java.util.Collections;

public class PlatFormMeger2 implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 187273497298374823L;
	OutputCollector collector;

	private static Logger LOG = LoggerFactory.getLogger(PlatFormMeger2.class);
	private static final String sep = ":";

	//private transient TairOperatorImpl tairOperator;
	/**
	 * 此处不计算，只合并相同时间的数据
	 */
	private Map<Long, Double> pt0maps = new HashMap<Long, Double>();
	private Map<Long, Double> pt1maps = new HashMap<Long, Double>();
	boolean isend = false;

	private int getIndex(List<Long> list, long time) {
		list.add(time);
		Collections.sort(list);
		int index = list.indexOf(time);
		return index;
	}
	@Override
	public void execute(Tuple tuple) {
		List<String> list = (List<String>) tuple.getValue(0);
		if (list == null || list.size() == 0) {
			// storeCountMessage();
			return;
		}
		String[] arr = (String[]) list.toArray(new String[list.size()]);
		for (String str : arr) {
			if (RaceConfig.pay_end.equals(str)) {
				isend = true;
				continue;
			}
			String data[] = str.split(sep);
			long time = Long.parseLong(data[0]);
			String pt = data[1];
			double price = Double.parseDouble(data[2]);
			Double tmprice0 = pt0maps.get(time);
			Double tmprice1 = pt1maps.get(time);
			if (tmprice0 == null && "0".equals(pt)) {
				pt0maps.put(time, price);
			} else if (tmprice1 == null && "1".equals(pt)) {
				pt1maps.put(time, price);
			} else {
				if ("1".equals(pt))
					pt1maps.put(time, price + tmprice1);
				else
					pt0maps.put(time, price + tmprice0);
			}
		}
		collector.emit(tuple, new Values("1", new HashMap(pt0maps)));
		collector.emit(tuple, new Values("0", new HashMap(pt1maps)));
		if (isend) {
			collector.emit(tuple, new Values("end",null));
			isend = false;
		}
		pt0maps.clear();
		pt1maps.clear();
		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("id", "time"));
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

//	public boolean writeData(long timestamp, Number str) {
//		if (tairOperator == null)
//			tairOperator = new TairOperatorImpl();
//		String key = RaceConfig.prex_ratio + RaceConfig.teamcode + timestamp;
//		RaceUtils.method1_WriteText(key + ":" + str);
//		return tairOperator.write(key, str);
//
//	}
}