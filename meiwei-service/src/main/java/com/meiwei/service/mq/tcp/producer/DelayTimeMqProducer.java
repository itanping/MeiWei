package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Date;

/**
 * 定时消息（延迟队列） - 生产者
 *
 * @author tanping
 * @date 2019/9/24 11:30
 */
public class DelayTimeMqProducer {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    // Topic 名称长度不得超过 64 字符长度限制，否则会导致无法发送或者订阅
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";

    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_DELAY_TIME";

    public static void main(String[] args) throws Exception {
        // 声明并实例化一个 producer 生产者来产生消息
        // 需要一个 producer group 名字作为构造方法的参数
        DefaultMQProducer producer = new DefaultMQProducer("meiwei-producer-delay-time");

        // 指定 NameServer 地址列表，多个nameServer地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        producer.setNamesrvAddr("127.0.0.1:9876");

        // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
        producer.start();

        // 循环发送MQ测试消息
        String content = "";
        for (int i = 0; i < 3; i++) {
            // 配置容灾机制，防止当前消息异常时阻断发送流程
            try {
                content = "【MQ测试消息】延时消息发送 " + i + "，时间 " + new Date();

                // Message Body 可以是任何二进制形式的数据，消息队列不做任何干预，需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式
                Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH, "KEY" + i, content.getBytes(RemotingHelper.DEFAULT_CHARSET));

                // 默认值为“1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h”，18个level
                // 设置延时等级3，这个消息将在 10s 之后发送
                message.setDelayTimeLevel(3);
                // 发送消息
                SendResult sendResult = producer.send(message);

                // 日志打印
                System.out.printf("Send MQ message success! Topic: %s，Tag: %s, msgId: %s, Message: %s %n",
                        message.getTopic(), message.getTags(), sendResult.getMsgId(), new String(message.getBody()));
            } catch (Exception e) {
                // 消息发送失败
                System.out.printf("%-10d Exception %s %n", i, e);
                e.printStackTrace();
            }
        }

        // 在发送完消息之后，销毁 Producer 对象。如果不销毁也没有问题
        producer.shutdown();
    }
}
