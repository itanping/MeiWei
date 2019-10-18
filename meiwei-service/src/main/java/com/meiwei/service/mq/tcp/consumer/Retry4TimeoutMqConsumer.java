package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;

public class Retry4TimeoutMqConsumer {
    // Message 所属的 Topic 一级分类，须要与提供者的频道保持一致才能消费到消息内容
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_RETRY_TIMEOUT";

    public static void main(String[] args) throws Exception {
        // 创建一个 consumer 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("meiwei-consumer-retry-timeout");
        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 设置 consumer 所订阅的 Topic 和 Tag，*代表全部的 Tag
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH);
        // 设置消费超时时间（默认值15L，为15分钟）
        consumer.setConsumeTimeout(1L);
        // 设置最大重试数次
        consumer.setMaxReconsumeTimes(2);

        // 注册一个监听器，主要进行消息消费的逻辑处理
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 获取消息
                MessageExt msg = list.get(0);
                try {
                    // 获取重试次数
                    int reconsumeTimes = msg.getReconsumeTimes() + 1;
                    if (reconsumeTimes == 1) {
                        // 模拟操作：设置一个大于上面已经设置的消费超时时间 来验证超时重试场景（setConsumeTimeout(1L)）
                        System.out.println("---------- 服务暂停 ---------- " + new Date());
                        Thread.sleep(1000 * 60 * 2);
                    } else {
                        System.out.println("---------- 重试消费 ---------- " + new Date());
                    }

                    System.out.printf(new Date() + " 第 %s 次重试消费：Topic: %s, Tags: %s, MsgId: %s, Message: %s %n",
                            reconsumeTimes, msg.getTopic(), msg.getTags(), msg.getMsgId(), new String(msg.getBody()));

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    System.out.printf(new Date() + "，异常信息：%s %n", e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("Retry Timeout Consumer Started.");
    }
}
