package com.alibaba.aloha.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class MetaConsumerFactory {

	private static final Logger LOG = Logger.getLogger(MetaConsumerFactory.class);
	private static final long serialVersionUID = 4641537252342232163L;

	public static Map<String, DefaultMQPushConsumer> consumers = new HashMap<String, DefaultMQPushConsumer>();
	private static final String[] topics=new String[]{"MiddlewareRaceTestData_TMOrder","MiddlewareRaceTestData_TBOrder","MiddlewareRaceTestData_Pay"};
    
	private static Random r=new Random();
	public static synchronized DefaultMQPushConsumer mkInstance(MessageListenerConcurrently listener) throws MQClientException  {
	
		String groupName="yuhaifang_916";
		String id=null;
		DefaultMQPushConsumer consumer=null;
		for(int i=0;i<topics.length;i++)
		{
			 id=groupName+"@"+topics[i];
			// System.out.println("----------id ------"+id);
		     consumer=consumers.get(id);
		     LOG.info("------------------id ------"+id+":"+consumer);
		     if(consumer==null)
		     {
		    	consumer = new DefaultMQPushConsumer(groupName);
		 		consumer.setNamesrvAddr("192.168.159.130:9876");
		 		consumer.subscribe(topics[i], "*");
		 		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		 		consumer.registerMessageListener(listener);
		 		consumer.start();
		 		consumers.put(id, consumer);
		 		LOG.info("-----------"+groupName+":"+topics[i]+"----consumer create success!");
		 		break;
		     }
		     
		}
		if(consumer==null)
		     return consumers.get(r.nextInt(topics.length));
		return consumer;

	}

}
