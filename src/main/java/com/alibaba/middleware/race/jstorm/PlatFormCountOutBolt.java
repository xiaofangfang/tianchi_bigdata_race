package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import edu.emory.mathcs.backport.java.util.Collections;

public class PlatFormCountOutBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 187273497298374823L;
	OutputCollector collector;

	//private static Logger LOG = LoggerFactory.getLogger(PlatFormCountOutBolt.class);
	private static final String sep = ":";

	private transient TairOperatorImpl tairOperator;
	private Map<Long, Double> pt0maps;
	private Map<Long, Double> pt1maps;
	boolean isend = false;

	private int getIndex(List<Long> list, long time) {
		list.add(time);
		Collections.sort(list);
		int index = list.indexOf(time);
		return index;
	}

	public synchronized void aclPrice(Map<Long, Double> p, long time, double price) {
		List<Long> tmpList = new ArrayList<Long>(p.keySet());
		Collections.sort(tmpList);
		//RaceUtils.method1_WriteText("----tmplist--" + tmpList + "----" + time);
		int index = getIndex(tmpList, time);
		int length = tmpList.size();
		if (index == 0 && tmpList.size() != 0) {
			for (int i = 1; i < length; i++) {
				p.put(tmpList.get(i), p.get(tmpList.get(i)) + price);
			}
		}
		double total = 0;
		if (index > 0 && index < length - 1) {

			for (int i = 0; i < index; i++) {
				total = total + p.get(tmpList.get(i));
			}
			p.put(time, price + total);
			for (int i = index + 1; i < length - 1; i++) {
				p.put(tmpList.get(i), p.get(tmpList.get(i) + price));
			}
		}

		if (index + 1 == length) {
			for (int i = 0; i < index; i++) {
				total = total + p.get(tmpList.get(i));
			}
			p.put(time, price + total);
		}
		//RaceUtils.method1_WriteText("----acl-index-total--" + index + ":" + length + ":" + pt0maps);

	}

	public void combineMap(Map<Long, Double> map1, Map<Long, Double> map2) {
		for (Long l : map2.keySet()) {
			Double price = map1.get(l);
			if (price == null)
				map1.put(l, map2.get(l));
			else
				map1.put(l, map2.get(l) + map1.get(l));
		}

	}

	public synchronized void aclValue(final Map<Long, Double> pmap1, final Map<Long, Double> pmap0) {
		// String key = RaceConfig.prex_ratio + RaceConfig.teamcode;

		List<Long> tmpList = new ArrayList<Long>(pmap1.keySet());
		Collections.sort(tmpList);
		double p1 = 0;
		double p0 = 0;
		int length0 = pmap0.keySet().size();
		int lenght1 = pmap1.keySet().size();
		if (lenght1 == 0 || length0 == 0)
			return;
		for (int i = 0; i < tmpList.size() && i < length0 && i < lenght1; i++) {
			long time = tmpList.get(i);
			Double v1 = pmap1.get(time);
			Double v0 = pmap0.get(time);
			if (v1 == null)
				return;
			if (v0 == null)
				return;
			p1 = p1 + v1;
			p0 = p0 + v0;
			double value = p1 / p0;
			double plast = (double) (Math.round(value * 100) / 100.0);
			//RaceUtils.method1_WriteText("----write--data---" + time + ":" + plast);
		}

	}

	public synchronized void aclAndRemove(final Map<Long, Double> pmap1, final Map<Long, Double> pmap0) {
		// String key = RaceConfig.prex_ratio + RaceConfig.teamcode;

		List<Long> tmpList = new ArrayList<Long>(pmap1.keySet());
		Collections.sort(tmpList);
		double p1 = 0;
		double p0 = 0;
		int length0 = pmap0.keySet().size();
		int lenght1 = pmap1.keySet().size();
		if (lenght1 == 0 || length0 == 0)
			return;
		long time = tmpList.get(0);
		Double v1 = pmap1.get(time);
		Double v0 = pmap0.get(time);
		if (v1 == null)
			return;
		if (v0 == null)
			return;
		p1 = p1 + v1;
		p0 = p0 + v0;
		double value = p1 / p0;
		double plast = (double) (Math.round(value * 100) / 100.0);
		pmap1.remove(time);
		pmap0.remove(time);
		//RaceUtils.method1_WriteText("----write--data---" + time + ":" + plast);

	}

	@Override
	public void execute(Tuple tuple) {
		String pt = tuple.getString(0);
		if ("end".equals(pt)) {
			//RaceUtils.method1_WriteText("----platfromcount--rev-endflag---");
			isend = true;
			return;
		}
		if (pt == null)
			return;
		Map<Long, Double> maps = (Map<Long, Double>) tuple.getValue(1);
		// RaceUtils.method1_WriteText("----countout--rev--maps--" + maps + ":"
		// + pt);
		if (maps == null)
			return;
		if ("1".equals(pt)) {
			if (pt0maps == null)
				pt0maps = maps;
			else
				combineMap(pt0maps, maps);
		}
		if ("0".equals(pt)) {
			if (pt1maps == null)
				pt1maps = maps;
			else
				combineMap(pt1maps, maps);

		}
		if (isend)
			aclValue(pt1maps, pt0maps);
		else if (pt0maps.keySet().size() > RaceConfig.pushNumber && pt1maps.keySet().size() > RaceConfig.pushNumber) {
			// aclAndRemove(pt1maps, pt0maps);
			aclValue(pt1maps, pt0maps);
		}

		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("tmplist"));
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

	public boolean writeData(long timestamp, Number str) {
		if (tairOperator == null)
			tairOperator = new TairOperatorImpl();
		String key = RaceConfig.prex_ratio + RaceConfig.teamcode + timestamp;
		//RaceUtils.method1_WriteText(key + ":" + str);
		return tairOperator.put(key, str);

	}
}