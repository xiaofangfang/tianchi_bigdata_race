package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PlatFromTradeCountBolt implements IRichBolt {

	private static final long serialVersionUID = 17897646549784L;
	OutputCollector collector;
	Map<String, Double> counts = new HashMap<String, Double>();

	long _beginStmap;
	long _endStamp;
	int countTimes = 0;
	String _timeStampValue;
	// 如何判断上一分钟的数据已经处理完毕，可以发送到tair

	private transient TairOperatorImpl tairOperator;

	@Override
	public void execute(Tuple tuple) {
		if(!tuple.getSourceStreamId().equals(RaceConfig.PAY_STREAM_ID))
			return;	
		String[] data=tuple.getString(0).split(":");
		String platfrom=data[0];
		Double totalPice=Double.parseDouble(data[1]);
		
		
        
		String _thisTimeValue = tuple.getString(1);
		String _timeStamp = data[0];
		String _sumMoney = data[1];
		Double count = counts.get(_timeStamp);
		if (count == null) {
			counts.put(_timeStamp, Double.parseDouble(_sumMoney));
			_beginStmap = Long.parseLong(_timeStamp);
		} else {
			double sum = Double.parseDouble(_sumMoney) + count;
			counts.put(_timeStamp, sum);
		}

		// 通过对比处理的数据时间差来判断是否要进行数据保存处理
		if (_timeStampValue == null)
			_timeStampValue = _thisTimeValue;
		else {
			double t = Double.parseDouble(_thisTimeValue) - Double.parseDouble(_timeStampValue);
			// 处理的数据之间时间已经超过1分钟，可以进行数据保存操作
			if (t > 1.5) {
				Double _secPays = counts.get(_timeStamp);

				// 数据保存操作
				// if (writeData(_timeStamp, _secPays, platSign)) {
				// counts.remove(_timeStamp);
				// _timeStampValue = _timeStamp;
				// }
			}
		}

		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// TairOperatorImpl tairOperator = new
		// TairOperatorImpl(RaceConfig.TairConfigServer,
		// RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
		// RaceConfig.TairNamespace);
		// this.tairOperator = tairOperator;
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

	private boolean writeData(String _timeStamp, Double sum, String platFrom) {
		if (platFrom.contains(RaceConfig.MqTmallTradeTopic)) {
			String key = "platformTmall_teamcode_" + _timeStamp;
			return tairOperator.write(key, sum);
		}

		if (platFrom.contains(RaceConfig.MqTaobaoTradeTopic)) {
			String key = "platformTaobao_teamcode_" + _timeStamp;
			return tairOperator.write(key, sum);
		}
		return false;
	}
}