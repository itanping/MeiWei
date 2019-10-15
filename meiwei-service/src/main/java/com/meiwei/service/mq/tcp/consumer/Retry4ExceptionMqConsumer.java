package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;
import java.util.List;

public class Retry4ExceptionMqConsumer {
    // Message 所属的 Topic 一级分类，须要与提供者的频道保持一致才能消费到消息内容
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_RETRY_EXCEPTION";

    public static void main(String[] args) throws Exception {
        // 声明并初始化一个 consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("meiwei-consumer-retry-exception");
        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 设置 consumer 所订阅的 Topic 和 Tag，*代表全部的 Tag
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH);
//        // 设置最大重试数次
//        consumer.setMaxReconsumeTimes(5);

        // 注册一个监听器，主要进行消息消费的逻辑处理
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 获取消息
                MessageExt msg = list.get(0);

                try {
                    // 获取重试次数
                    int reconsumeTimes = msg.getReconsumeTimes() + 1;
                    System.out.printf(new Date() + "，第 %s 次轮询消费 %n", reconsumeTimes);

                    // 模拟业务逻辑。此处为超过最大重试次数，自动标记消息消费成功
                    if (reconsumeTimes >= 3) {
                        System.out.printf(new Date() + "，超过最大重试次数，自动标记消息消费成功 Topic: %s, Tags: %s, Message: %s %n",
                                msg.getTopic(), msg.getTags(), new String(msg.getBody()));
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }

                    // 模拟异常发生
                    int num = 1 / 0;
                    System.out.printf(new Date() + "，第 %s 次正常消费 %n", reconsumeTimes);

                    // 返回消费状态
                    // CONSUME_SUCCESS 消费成功
                    // RECONSUME_LATER 消费失败，需要稍后重新消费
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    // 获取重试次数
                    int reconsumeTimes = msg.getReconsumeTimes() + 1;
                    System.out.printf(new Date() + "，第 %s 次重试消费，异常信息：%s %n", reconsumeTimes, e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("Retry Consumer Started.");
    }
}
