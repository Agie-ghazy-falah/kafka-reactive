package com.example.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;

public class ProducerControllerTest {

    @InjectMocks
    private ProducerController controller;

    @Mock
    private KafkaSender<Integer, String> sender;

    @Mock
    private SenderResult senderResult;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() {
        Mockito.verifyNoMoreInteractions(senderResult);
    }

    @Test
    public void publishMessageTest() {
        System.out.println("testing");

        RecordMetadata recordMetadata = new RecordMetadata(
            new TopicPartition("topic.testing", 0),
            0l, 0l, 1584346850484l,
            1l, 0, 0
        );

        Mockito.when(sender.send(Mockito.anyObject())).thenReturn(Flux.just(senderResult));
        Mockito.when(senderResult.recordMetadata()).thenReturn(recordMetadata);
        Mockito.when(senderResult.correlationMetadata()).thenReturn(1);

        try {
            controller.publishMessage();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Mockito.verify(sender).send(Mockito.anyObject());
        Mockito.verify(senderResult).recordMetadata();
        Mockito.verify(senderResult).correlationMetadata();
    }

}
