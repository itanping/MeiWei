package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 顺序消息 - 消费者
 *
 * 消息有序指的是可以按照消息的发送顺序来消费(FIFO)。RocketMQ可以严格的保证消息有序，可以分为分区有序或者全局有序。
 *
 * @author tanping
 * @date 2019/9/24 11:30
 */
public class OrderMqConsumer {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    // Topic 名称长度不得超过 64 字符长度限制，否则会导致无法发送或者订阅
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";

    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_A = "PID_MEIWEI_SMS_ORDER_A";
    private static final String MQ_CONFIG_TAG_B = "PID_MEIWEI_SMS_ORDER_B";
    private static final String MQ_CONFIG_TAG_C = "PID_MEIWEI_SMS_ORDER_C";

    public static void main(String[] args) throws Exception {
        // 声明并初始化一个 consumer
        // 需要一个 consumer group 名字作为构造方法的参数
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("meiwei-consumer-ordermq");

        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");

        // 这里设置的是一个consumer的消费策略
        // CONSUME_FROM_LAST_OFFSET：默认策略，从该队列最尾开始消费，即跳过历史消息
        // CONSUME_FROM_FIRST_OFFSET：从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
        // CONSUME_FROM_TIMESTAMP：从某个时间点开始消费，和setConsumeTimestamp() 配合使用，默认是半个小时以前
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 设置 consumer 所订阅的 Topic 和 Tag，*代表全部的 Tag
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_A + " || " + MQ_CONFIG_TAG_B + " || " + MQ_CONFIG_TAG_C);

        // 设置一个Listener，主要进行消息的逻辑处理
        // 注意这里使用的是 MessageListenerOrderly 这个接口来实现顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {

            Random random = new Random();

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                // 设置自动提交
                consumeOrderlyContext.setAutoCommit(true);

                list.forEach(mq->{
                    System.out.printf("Thread: %s, Topic: %s, Tags: %s, Message: %s",
                            Thread.currentThread().getName(),
                            mq.getTopic(),
                            mq.getTags(),
                            new String(mq.getBody()));
                    System.out.println();
                });

                try {
                    //模拟业务逻辑处理中...
                    TimeUnit.SECONDS.sleep(random.nextInt(10));
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // 返回消费状态
                // SUCCESS 消费成功
                // SUSPEND_CURRENT_QUEUE_A_MOMENT 消费失败，暂停当前队列的消费
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("Consumer Started.");
    }
}
