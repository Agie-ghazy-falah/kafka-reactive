package com.example.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
public class ProducerController {
    private static final Logger log = LoggerFactory.getLogger(ProducerController.class.getName());

    @Autowired
    private KafkaSender<Integer, String> sender;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    @PostMapping("save")
    public void publishMessage() throws InterruptedException {
        System.out.println("save");

        sender.send(
            Flux.just(1)
            .map(i -> SenderRecord.create(
                new ProducerRecord<>("topic.testing", i, "Message_" + i),
                i)
            )
        )
        .doOnError(e -> log.error("Send failed", e))
        .subscribe(senderResult -> {
            RecordMetadata metadata = senderResult.recordMetadata();
            System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                senderResult.correlationMetadata(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                dateFormat.format(new Date(metadata.timestamp())));
        });
    }
}
