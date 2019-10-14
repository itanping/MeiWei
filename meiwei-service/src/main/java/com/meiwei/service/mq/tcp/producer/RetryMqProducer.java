package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Date;

/**
 *
 * @author tanping
 * @date 2019/10/12 11:30
 */
public class RetryMqProducer {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    // Topic 名称长度不得超过 64 字符长度限制，否则会导致无法发送或者订阅
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";

    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_RETRY = "PID_MEIWEI_SMS_RETRY_TIMEOUT";

    public static void main(String[] args) throws Exception {
        // 声明并实例化一个 producer 生产者来产生消息
        // 需要一个 producer group 名字作为构造方法的参数
        DefaultMQProducer producer = new DefaultMQProducer("meiwei-producer-retry");

        // 指定 NameServer 地址列表，多个nameServer地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        producer.setNamesrvAddr("127.0.0.1:9876");

        // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
        producer.start();

        // 设置重试次数，默认情况下是2次重试
        producer.setRetryTimesWhenSendFailed(5);

        // 循环发送MQ测试消息
        String content = "";
        for (int i = 0; i < 2; i++) {
            content = "【MQ测试消息】测试消息 " + i;

            // Message Body 可以是任何二进制形式的数据，消息队列不做任何干预，需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式
            Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_RETRY, content.getBytes(RemotingHelper.DEFAULT_CHARSET));

            // 发送消息。这里调用的是同步的方式，所以会有返回结果，同时默认发送的也是普通消息
            SendResult sendResult = producer.send(message, 2000);

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
