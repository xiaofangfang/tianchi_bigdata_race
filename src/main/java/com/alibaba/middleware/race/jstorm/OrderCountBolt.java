package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
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
import backtype.storm.tuple.Tuple;
import edu.emory.mathcs.backport.java.util.Collections;

public class OrderCountBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 15678978978978L;
	OutputCollector collector;
	private static Logger LOG = LoggerFactory.getLogger(OrderCountBolt.class);
	private transient TairOperatorImpl tairOperator;

	private static Map<Long, Double> tm_total = new HashMap<Long, Double>();
	private static Map<Long, Double> tb_total = new HashMap<Long, Double>();

	private boolean tm_endFlag = false;
	private boolean tb_endFlag = false;

	public void combineMap(Map<Long, Double> map1, Map<Long, Double> map2) {
		for (Long l : map2.keySet()) {
			Double price = map1.get(l);
			if (price == null)
				map1.put(l, map2.get(l));
			else
				map1.put(l, map2.get(l) + map1.get(l));
		}

	}

	// 如果消息size大于3，输出第一个并删除数据
	private void storeMessage(final Map<Long, Double> total, String ptFlag) {
		List<Long> timelist = new ArrayList<>(total.keySet());
		if (timelist.size() == 0)
			return;
		Collections.sort(timelist);
		long time = timelist.get(0);
		double price = total.get(time);
		// price保留2位小数
		price = (double) Math.round(price * 100) / 100.0;
		// if (!writeData(time, price, ptFlag)) {
		// //LOG.error("---更新" + ptFlag + "每分钟交易总量异常----" + time + ":" + price);
		// }
		total.remove(time);
	}

	private void storeAllMessage(final Map<Long, Double> total, String ptFlag) {
		List<Long> timelist = new ArrayList<>(total.keySet());
		if (timelist.size() == 0)
			return;
		Collections.sort(timelist);
		for (int i = 0; i < timelist.size(); i++) {
			long time = timelist.get(i);
			double price = total.get(time);
			price = (double) Math.round(price * 100) / 100.0;
			if (!writeData(time, price, ptFlag)) {
				LOG.error("---更新" + ptFlag + "每分钟交易总量异常----" + time + ":" + price);
			}
		}
	}

	@Override
	public void execute(Tuple tuple) {
		String pt = tuple.getString(0);
		if (pt == null) {
			//RaceUtils.method1_WriteText("---ordercount--recmessage --is -null");
			return;

		}
		Map<Long, Double> maps = (Map<Long, Double>) tuple.getValue(1);
		if (maps == null)
			return;
		if (RaceConfig.TM_flag.equals(pt)) {
			if (tm_total == null)
				tm_total = maps;
			else
				combineMap(tm_total, maps);
		}
		if (RaceConfig.TB_flag.equals(pt)) {
			if (tb_total == null)
				tb_total = maps;
			else
				combineMap(tb_total, maps);

		}
		// 检查是否存在结束符号
		if (!tb_endFlag && OrderIds.getTb().containsKey(RaceConfig.orderId_end)) {
			tb_endFlag = true;
			//RaceUtils.method1_WriteText("----get--tb--end--flag-------");
		}
		if (!tm_endFlag && OrderIds.getTm().containsKey(RaceConfig.orderId_end)) {
			tm_endFlag = true;
			//RaceUtils.method1_WriteText("----get--tm--end--flag-------");
		}

		// 数据保存至tair 包含结束标识，输出所有数据到tair
		if (tb_endFlag) {
			storeAllMessage(tb_total, RaceConfig.TB_flag);
		} else if (tb_total.keySet().size() > RaceConfig.pushNumber) {
			storeAllMessage(tb_total, RaceConfig.TB_flag);
		}

		if (tm_endFlag) {
			storeAllMessage(tm_total, RaceConfig.TM_flag);
		} else if (tm_total.keySet().size() > RaceConfig.pushNumber) {
			storeAllMessage(tm_total, RaceConfig.TM_flag);
		}
		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("payInfolist"));
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

	public boolean writeData(long timestamp, Number str, String pt) {
		if (tairOperator == null)
			tairOperator = new TairOperatorImpl();
		String key = null;
		if (RaceConfig.TM_flag.equals(pt))
			key = RaceConfig.prex_tmall + RaceConfig.teamcode + timestamp;
		else
			key = RaceConfig.prex_taobao + RaceConfig.teamcode + timestamp;
		//RaceUtils.method1_WriteText(key + ":" + str);
		return tairOperator.put(key, str);

	}

}