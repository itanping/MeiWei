package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author tanping
 * @date 2019/10/12 11:30
 */
public class RetryTimeoutMqProducer {

    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_RETRY = "PID_MEIWEI_SMS_RETRY_TIMEOUT";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("meiwei-producer-retry");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        String content = "【MQ测试消息】测试消息 ";
        Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_RETRY, content.getBytes(RemotingHelper.DEFAULT_CHARSET));

        // 发送消息，发送消息到一个 Broker。默认以同步方式发送
        SendResult sendResult = producer.send(message);
        System.out.printf("Send MQ message success! Topic: %s, Tag: %s, MsgId: %s, Message: %s %n",
                message.getTopic(), message.getTags(), sendResult.getMsgId(), new String(message.getBody()));

        producer.shutdown();
    }
}
