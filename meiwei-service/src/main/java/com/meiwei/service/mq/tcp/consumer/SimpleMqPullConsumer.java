package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SimpleMqPullConsumer {

    // 记录每个 MessageQueue 的消费位点 offset，可以持久化到 DB 或缓存 Redis，这里作为演示就保存在程序中
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    // Message 所属的 Topic 一级分类，须要与提供者的频道保持一致才能消费到消息内容
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_PUSH";

    public static void main(String[] args) throws Exception {
        // 创建一个 consumer 消费者
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("meiwei-consumer-pull");
        // 同样也要设置 NameServer 地址，须要与提供者的地址列表保持一致
        consumer.setNamesrvAddr("127.0.0.1:9876");
        // 调用 start() 方法启动 consumer
        consumer.start();
        System.out.println("Consumer Started !");

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(MQ_CONFIG_TOPIC);
        for (MessageQueue mq : mqs) {
            System.out.println("Consume from the queue: " + mq);

            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}
