package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Calendar;
import java.util.List;

/**
 * 定时消息（延迟队列） - 消费者
 *
 * @author tanping
 * @date 2019/9/24 11:30
 */
public class DelayTimeMqConsumer {

    // Message 所属的 Topic 一级分类，须要与提供者的频道保持一致才能消费到消息内容
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_DELAY_TIME";

    public static void main(String[] args) throws Exception {
        // 创建一个 consumer 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("meiwei-consumer-delayTime");
        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 设置 consumer 所订阅的 Topic 和 Tag，*代表全部的 Tag
        consumer.subscribe(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH);

        // 注册一个监听器，主要进行消息消费的逻辑处理
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(mq->{
                    Calendar calendar = Calendar.getInstance();
                    String timeNow = new String(calendar.get(Calendar.HOUR_OF_DAY) + ":" + calendar.get(Calendar.MINUTE) + ":" + calendar.get(Calendar.SECOND));
                    System.out.printf("TimeNow: %s, Thread: %s, Topic: %s, Tags: %s, Message: %s",
                            timeNow, Thread.currentThread().getName(), mq.getTopic(), mq.getTags(), new String(mq.getBody()));
                    System.out.println();
                });

                // 返回消费状态
                // CONSUME_SUCCESS 消费成功
                // RECONSUME_LATER 消费失败，需要稍后重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("DelayTime Consumer Started.");
    }
}
