package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Date;

/**
 * 可靠同步发送 - 生产者
 * <p>
 * 原理
 * 同步发送是指消息发送方发出数据后，会在收到接收方发回响应之后才发下一个数据包的通讯方式。
 * <p>
 * 应用场景
 * 此种方式应用场景非常广泛，例如重要通知邮件、报名短信通知、营销短信系统等。
 *
 * @author tanping
 * @date 2019/9/19 12:30
 */
public class SimpleSyncMqProducer {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_SYNC";

    public static void main(String[] args) throws Exception {
        // 创建一个 producer 生产者
        DefaultMQProducer producer = new DefaultMQProducer("meiwei-producer-simple-sync");
        // 指定 NameServer 地址列表，多个 nameServer 地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
        producer.start();

        // 循环发送MQ测试消息
        String content = "";
        for (int i = 0; i < 5; i++) {
            content = "【MQ测试消息】可靠同步发送 " + i;

            // 构建一条消息
            Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送消息，发送消息到一个 Broker。默认以同步方式发送
            SendResult sendResult = producer.send(message);

            // 同步发送消息，只要不抛异常就是成功
            if (sendResult != null) {
                // 消息发送成功
                System.out.printf("Send MQ message success! Topic: %s, Tag: %s, MsgId: %s, Message: %s %n",
                        message.getTopic(), message.getTags(), sendResult.getMsgId(), new String(message.getBody()));
            } else {
                System.out.println(new Date() + " Send MQ message failed! Topic: " + message.getTopic());
            }
        }

        // 在发送完消息之后，销毁 Producer 对象。如果不销毁也没有问题
        producer.shutdown();
    }
}
