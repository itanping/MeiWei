package com.meiwei.service.mq.tcp.producer;

import com.alibaba.fastjson.JSON;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.exception.MQClientException;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.client.producer.SendResult;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.common.message.Message;
import com.aliyun.openservices.shade.com.alibaba.rocketmq.remoting.common.RemotingHelper;

/**
 * @author tanping
 * @date 2019/9/19 12:30
 */
public class SimpleMQProducer {

    // Message 所属的 Topic
    private final static String MQ_CONFIG_TOPIC = "";
    // Tag 可以理解为 Gmail 中的标签，对消息进行再归类，方便 Consumer 指定过滤条件在消息队列 MQ 的服务器过滤
    private final static String MQ_CONFIG_TAG = "";

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("producer1");
        producer.setNamesrvAddr("127.0.0.1");
        producer.setVipChannelEnabled(false);

        try {
            // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        // 循环发送MQ测试消息
        String content = "";
        for (int i = 0; i < 10; i++) {
            content = "MQ测试消息" + i;
            try {
                // Message Body 可以是任何二进制形式的数据，消息队列不做任何干预，需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式
                Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(message);

                // 同步发送消息，只要不抛异常就是成功
                if (sendResult != null) {
                    System.out.println(System.currentTimeMillis() + " Send mq message success! Topic is: " + MQ_CONFIG_TOPIC + " msgId is: " + sendResult.getMsgId());
                    System.out.println(JSON.toJSONString(sendResult));
                }
            } catch (Exception e) {
                System.out.println(System.currentTimeMillis() + " Send mq message failed! Topic is: " + MQ_CONFIG_TOPIC );
                e.printStackTrace();
            }
        }

        // 在应用退出前，销毁 Producer 对象。如果不销毁也没有问题
        producer.shutdown();
    }
}
