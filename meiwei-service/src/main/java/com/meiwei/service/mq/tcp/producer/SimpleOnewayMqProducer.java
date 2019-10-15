package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 单向（Oneway）发送 - 生产者
 * <p>
 * 原理
 * 单向（Oneway）发送特点为发送方只负责发送消息，不等待服务器回应且没有回调函数触发，即只发送请求不等待应答。
 * 此方式发送消息的过程耗时非常短，一般在微秒级别。
 * <p>
 * 应用场景
 * 适用于某些耗时非常短，但对可靠性要求并不高的场景，例如日志收集。
 *
 * @author tanping
 * @date 2019/9/19 12:30
 */
public class SimpleOnewayMqProducer {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";

    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_ONEWAY = "PID_MEIWEI_SMS_ONEWAY";

    public static void main(String[] args) throws Exception {
        // 创建一个 producer 生产者
        DefaultMQProducer producer = new DefaultMQProducer("meiwei-producer-oneway");
        // 指定 NameServer 地址列表，多个 nameServer 地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
        producer.start();

        // 循环发送MQ测试消息
        String content = "";
        for (int i = 0; i < 5; i++) {
            content = "【MQ测试消息】单向消息发送 " + i;

            // 构建一条消息
            Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_ONEWAY, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 单向发送模式没有返回值，就是说只管发不管发送投递是否成功
            producer.sendOneway(message);
            // 日志打印
            System.out.printf("Send MQ message success! Topic: %s, Tag: %s, Message: %s %n",
                    message.getTopic(), message.getTags(), new String(message.getBody()));
        }

        // 在发送完消息之后，销毁 Producer 对象。如果不销毁也没有问题
        producer.shutdown();
    }
}
