package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;

/**
 * 商品服务
 */
public class DrugsService {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    // Topic 名称长度不得超过 64 字符长度限制，否则会导致无法发送或者订阅
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_ORDER_TRANSACTION";

    public static void main(String[] args) throws Exception {
        // 声明并初始化一个 consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        // 指定 NameServer 地址列表，多个nameServer地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumerGroup("meiwei-consumer-transaction-mq");
        // 设置 consumer 所订阅的 Topic 和 Tag，这里的 Topic 需要与生产者保持一致
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH);
        // 注册一个消息监听器消费消息
        consumer.registerMessageListener(new DrugsMqConsumerListener());

        consumer.start();
        System.out.println("Drugs Service Start.");
    }
}
