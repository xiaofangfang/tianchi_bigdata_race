package com.alibaba.middleware.race;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.alibaba.middleware.race.rocketmq.ProducerSingleton;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
    /**
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return
     */
	private static String DATE_PATTERN_3="yyyy/MM/dd HH:mm:ss";
	public static final String file="./src/result.txt";
	public static final String onminute="./src/result.txt";
	 public static void method1_WriteText(String conent) {
	       BufferedWriter out = null;
	       try {
	           out = new BufferedWriter(new OutputStreamWriter(
	           new FileOutputStream(file, true)));//追加写
	           out.write(conent+"\n");
	           
	       } catch (Exception e) {
	           e.printStackTrace();
	       } finally {
	           try {
	              out.close();
	           } catch (IOException e) {
	              e.printStackTrace();
	           }
	       }
	    }
	
	public static void main(String[] args) throws ParseException {
		ProducerSingleton  p=ProducerSingleton.getInstance();
//		p.getQues(RaceConfig.MqTaobaoTradeTopic);
//		p=ProducerSingleton.getInstance();
//		p.getQues(RaceConfig.MqPayTopic);
//		p=ProducerSingleton.getInstance();
//		p.getQues(RaceConfig.MqTmallTradeTopic);
		
	}
	
	public static  String formatDate(long str) { 
	    
	    SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN_3);  
	     return sdf.format( new Date(str));  
	    
	} 
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }

}
