package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class Retry4ExceptionMqConsumer {
    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    // Topic 名称长度不得超过 64 字符长度限制，否则会导致无法发送或者订阅
    // Message 所属的 Topic 一级分类，须要与提供者的频道保持一致才能消费到消息内容
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_RETRY_EXCEPTION";

    public static void main(String[] args) throws Exception {
        // 声明并初始化一个 consumer
        // 需要一个 consumer group 名字作为构造方法的参数
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("meiwei-consumer-retry-exception");

        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 设置 consumer 所订阅的 Topic 和 Tag，*代表全部的 Tag
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH);

        // 设置一个Listener，主要进行消息的逻辑处理
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

                if (CollectionUtils.isEmpty(list)) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

                // 指定消息第一条
                MessageExt msg = list.get(0);

                try {
                    int reconsumeTimes = msg.getReconsumeTimes() + 1;
                    System.out.printf("第 %s 次轮询消费 %n", reconsumeTimes);

                    int num = 1 / 0;
                    System.out.printf("第 %s 次正常消费 %n", reconsumeTimes);

                    // 返回消费状态
                    // CONSUME_SUCCESS 消费成功
                    // RECONSUME_LATER 消费失败，需要稍后重新消费
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

                } catch (Exception e) {
                    int reconsumeTimes = msg.getReconsumeTimes() + 1;
                    System.out.printf("消费异常，第 %s 次重试消费 : Thread: %s, Topic: %s, Tags: %s, MsgId: %s, Message: %s %n",
                            reconsumeTimes, Thread.currentThread().getName(),
                            msg.getTopic(), msg.getTags(), msg.getMsgId(), new String(msg.getBody()));

                    // 生产者设置重试5次，这里设置最大重试次数
                    // 这里模拟如果超过最大重试次数三次，自动标记消息消费成功，即消费者剩余的2次不会再走重试
                    if (reconsumeTimes >= 3) {
                        System.out.printf("超过最大重试次数，自动标记消息消费成功 Thread: %s, Topic: %s, Tags: %s, MsgId: %s, Message: %s %n",
                                Thread.currentThread().getName(),
                                msg.getTopic(), msg.getTags(), msg.getMsgId(), new String(msg.getBody()));
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("Retry Consumer Started.");
    }
}
