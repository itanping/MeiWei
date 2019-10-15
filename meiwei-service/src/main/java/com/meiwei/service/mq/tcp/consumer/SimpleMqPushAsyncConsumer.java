package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 可靠异步发送 - 消费者（Push模式）
 */
public class SimpleMqPushAsyncConsumer {
    // Message 所属的 Topic 一级分类，须要与提供者的频道保持一致才能消费到消息内容
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_ASYNC";

    public static void main(String[] args) throws Exception {
        // 创建一个 consumer 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("meiwei-consumer-simple-async");
        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 设置 consumer 的消费策略
        // CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，即跳过历史消息
        // CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
        // CONSUME_FROM_TIMESTAMP 从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 设置 consumer 所订阅的 Topic 和 Tag，*代表全部的 Tag
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH);

        // 注册一个监听器，主要进行消息消费的逻辑处理
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println(Thread.currentThread().getName() + " Receive new message: " + list);
                for (MessageExt msg : list) {
                    System.out.printf("Thread: %s, Topic: %s, Tags: %s, MsgId: %s, Message: %s %n",
                            Thread.currentThread().getName(), msg.getTopic(), msg.getTags(), msg.getMsgId(),  new String(msg.getBody()));
                }

                // 返回消费状态
                // CONSUME_SUCCESS 消费成功
                // RECONSUME_LATER 消费失败，需要稍后重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("Simple Consumer Started.");
    }
}
