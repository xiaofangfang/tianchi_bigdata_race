package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.List;

public class TestServrizableObj implements Serializable{
	private List<OrderMessage> tb;
	private List<OrderMessage>  tm;
	private List<PaymentMessage> pay;
	public List<OrderMessage> getTb() {
		return tb;
	}
	public void setTb(List<OrderMessage> tb) {
		this.tb = tb;
	}
	public List<OrderMessage> getTm() {
		return tm;
	}
	public void setTm(List<OrderMessage> tm) {
		this.tm = tm;
	}
	public List<PaymentMessage> getPay() {
		return pay;
	}
	public void setPay(List<PaymentMessage> pay) {
		this.pay = pay;
	}

}
