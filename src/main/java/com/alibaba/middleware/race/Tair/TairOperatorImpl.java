package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.applet.Applet;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/ group
 * 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl{

	private DefaultTairManager tairManager;

	public void init(String masterConfigServer, String slaveConfigServer, String groupName, int namespace) {
		List<String> confServers = new ArrayList<String>();
		confServers.add(masterConfigServer);
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);
		tairManager.setGroupName(groupName);
		tairManager.init();
	}

	public TairOperatorImpl() {
		init(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
				RaceConfig.TairNamespace);
		// 假设这是付款时间

	}

	public boolean write(Serializable key, Number value) {
		ResultCode result = tairManager.put(RaceConfig.TairNamespace, key, value, 0, 10000000);
		if (result.isSuccess())
			return true;
		return false;
	}

	public Object get(Serializable key) {
		return tairManager.get(RaceConfig.TairNamespace, key);
	}

	public boolean remove(Serializable key) {
		return false;
	}

	public void close() {
	}

	// 天猫的分钟交易额写入tair
	public static void main(String[] args) throws Exception {
		TairOperatorImpl tairOperator = new TairOperatorImpl();
		// 假设这是付款时间
		Long millisTime = System.currentTimeMillis();
		// 由于整分时间戳是10位数，所以需要转换成整分时间戳
		Long minuteTime = (millisTime / 1000 / 60) * 60;
		// 假设这一分钟的交易额是100;
		Double money = 100.0;
		String str="platformTmall_ts2efh_1467770700";
		double value=34054.34;
		// 写入tair
		//tairOperator.write(str, value);
		//Thread.sleep(60000);
		System.out.println("-------------" + tairOperator.get(str));

	}
}
