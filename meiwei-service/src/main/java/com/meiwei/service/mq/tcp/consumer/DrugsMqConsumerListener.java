package com.meiwei.service.mq.tcp.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 商品消费端监听器
 */
public class DrugsMqConsumerListener implements MessageListenerConcurrently {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        Optional.ofNullable(list).orElse(Collections.emptyList()).forEach(msg -> {
            String orderId = msg.getKeys();
            System.out.println("【商品消费端监听器】您有新订单，orderId: " + orderId + "，商品库存需要更新");
        });

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
