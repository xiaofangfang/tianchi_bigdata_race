package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PlatFromCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
//import edu.emory.mathcs.backport.java.util.Collections;

public class PlatFormMegerBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1557894564564L;
	OutputCollector collector;
	Map<String, Double> counts = new HashMap<String, Double>();
	private static Logger LOG = LoggerFactory.getLogger(PlatFormMegerBolt.class);
	long _beginStmap;
	long _endStamp;
	int countTimes = 0;
	String _timeStampValue;

	static Map<Short, PlatFromCount> paysMap = new HashMap<Short, PlatFromCount>();

	private transient TairOperatorImpl tairOperator;

	List<PlatFromCount> lists = new ArrayList<PlatFromCount>();

	@Override
	public void execute(Tuple tuple) {
		// plat 要处理的数据
		PlatFromCount plat = (PlatFromCount) tuple.getValue(0);
		if (plat == null) {
			Utils.sleep(20);
			return;
		}

		synchronized (paysMap) {
			// _tmp 已处理的数据
			PlatFromCount _tmp = paysMap.get(plat.getPlatFrom());
			// RaceUtils.method1_WriteText("-----------recev------plat---=="+plat+":"+_tmp);
			if (_tmp == null) {
				paysMap.put(plat.getPlatFrom(), plat);
				return;
			} else {
				long currentStamp = plat.getTimeStamp();
				long lastStamp = _tmp.getTimeStamp();
				if (lastStamp == currentStamp) {
					_tmp.setTotalprice(_tmp.getTotalprice() + plat.getTotalprice());
					_tmp.setLastTotalPrice(_tmp.getLastTotalPrice() + plat.getLastTotalPrice());
					_tmp.setMiddleTotalPrice(_tmp.getMiddleTotalPrice() + plat.getMiddleTotalPrice());
				}
				if (currentStamp < lastStamp) {
					if (currentStamp == _tmp.getMiddleStamp()) {
						_tmp.setMiddleTotalPrice(_tmp.getMiddleTotalPrice() + plat.getTotalprice());
					}
					if (currentStamp == _tmp.getLastStmap()) {
						_tmp.setLastTotalPrice(_tmp.getLastTotalPrice() + plat.getTotalprice());
					}
				}
				if (currentStamp > lastStamp) {
					short s1 = 1;
					short s0 = 0;
					PlatFromCount p0 = paysMap.get(s0);
					PlatFromCount p1 = paysMap.get(s1);

					if (p0 != null && p1 != null && _tmp.getPlatFrom() == s0) {

						double v = 0;
						if (p0.getLastStmap() == p1.getLastStmap())
							v = p1.getLastTotalPrice() / p0.getLastTotalPrice();
						if (p0.getLastStmap() == p1.getMiddleStamp())
							v = p1.getMiddleTotalPrice() / p0.getLastTotalPrice();
						v = (double) (Math.round(v * 100)) / 100;
						if (v != 0) {
							//RaceUtils.method1_WriteText(0 + "-----------recev------p" + p0 + "--" + p1 + ":v=" + v);
							if (!writeData(_tmp.getLastStmap() / 1000, v))
								LOG.error(0 + "----insert key value fail--" + _tmp.getLastStmap() / 1000 + ":" + v);
						}
					}
					if (p0 != null && p1 != null && _tmp.getPlatFrom() == s1) {

						if (p0 != null && p1 != null) {
							double v = 0;
							if (p1.getLastStmap() == p0.getLastStmap())
								v = p1.getLastTotalPrice() / p0.getLastTotalPrice();
							if (p1.getLastStmap() == p0.getMiddleStamp())
								v = p1.getLastTotalPrice() / p0.getMiddleTotalPrice();
							v = (double) (Math.round(v * 100)) / 100;
							if (v != 0) {
								//RaceUtils.method1_WriteText(1 + "-----------recev------" + p0 + "--" + p1 + ":v=" + v);
								if (!writeData(_tmp.getLastStmap() / 1000, v))
									LOG.error("----insert key value fail--" + _tmp.getLastStmap() / 1000 + ":" + v);
							}
						}

					}

					// 更新時間和金額
					_tmp.setLastStmap(_tmp.getMiddleStamp());
					_tmp.setMiddleStamp(_tmp.getTimeStamp());
					_tmp.setTimeStamp(currentStamp);

					_tmp.setLastTotalPrice(_tmp.getMiddleTotalPrice());
					_tmp.setMiddleTotalPrice(_tmp.getTotalprice());
					_tmp.setTotalprice(plat.getTotalprice());

					// RaceUtils.method1_WriteText(str);
				}
				paysMap.put(plat.getPlatFrom(), _tmp);
			}

		}
		// 消息处理
		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("orderid"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		tairOperator = new TairOperatorImpl();
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

	public boolean writeData(long timestamp, Number str) {

		String key = RaceConfig.prex_ratio + RaceConfig.teamcode + timestamp;
		RaceUtils.method1_WriteText(key + ":" + str);
		return tairOperator.write(key, str);

	}
}