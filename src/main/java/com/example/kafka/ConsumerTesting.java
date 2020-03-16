package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class ConsumerTesting {
    private static SimpleDateFormat dateFormat =
        new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    @Autowired
    private KafkaReceiver<Integer, String> kafkaFlux;

    @PostConstruct
    public void ConsumerTesting() {
        kafkaFlux
            .receive()
            .subscribe(record -> {
            ReceiverOffset offset = record.receiverOffset();
            System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                offset.topicPartition(),
                offset.offset(),
                dateFormat.format(new Date(record.timestamp())),
                record.key(),
                record.value());
            offset.acknowledge();
        });
    }

}
