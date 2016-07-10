package com.alibaba.middleware.race.Tair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/ group
 * 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

	private DefaultTairManager tairManager;

	public boolean put(String key, Serializable value) {
		ResultCode result = null;
		result = tairManager.put(RaceConfig.TairNamespace, key, value);
		if (result.isSuccess() && result.getCode() == 0)
			return true;
		return false;
	}

	public Object get(String prefix, String key) {
		Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, prefix + key);
		if (result.isSuccess() && result.getRc().getCode() == 0) {
			DataEntry value = result.getValue();
			if (null != value) {
				return value.getValue();
			}
		}
		return null;
	}

	public void init(String masterConfigServer, String slaveConfigServer, String groupName, int namespace) {
		List<String> confServers = new ArrayList<String>();
		confServers.add(masterConfigServer);
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);
		tairManager.setGroupName(groupName);
		tairManager.init();
	}

	public TairOperatorImpl() {
		this.init(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
				RaceConfig.TairNamespace);

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
		String str = "ratio_42500hzpdx_1468027920";
		double value = 34054.34;
		// 写入tair
		// tairOperator.write(str, value);
		// Thread.sleep(60000);
		// System.out.println("-------------" + tairOperator.get(str));

	}
}
