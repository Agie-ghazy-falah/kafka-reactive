package com.example.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;

public class ConsumerTestingTest {

    @InjectMocks
    private ConsumerTesting consumer;
    @Mock
    private static KafkaReceiver<Integer, String> kafkaFlux;

    @Mock
    private ReceiverRecord<Integer, String> record;
    @Mock
    ReceiverOffset receiverOffset;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() {
        Mockito.verifyNoMoreInteractions(kafkaFlux, record, receiverOffset);
    }

    @Test
    public void testing() {
        Mockito.when(kafkaFlux.receive()).thenReturn(Flux.just(record));

        Mockito.when(record.receiverOffset()).thenReturn(receiverOffset);
        Mockito.when(record.key()).thenReturn(1);
        Mockito.when(record.value()).thenReturn("value");
        Mockito.when(record.timestamp()).thenReturn(1584345325517l);
        Mockito.when(receiverOffset.topicPartition())
            .thenReturn(new TopicPartition("topic.testing", 0));
        Mockito.when(receiverOffset.offset()).thenReturn(5l);

        consumer.ConsumerTesting();

        Mockito.verify(kafkaFlux).receive();
        Mockito.verify(record).receiverOffset();
        Mockito.verify(record).key();
        Mockito.verify(record).value();
        Mockito.verify(record).timestamp();
        Mockito.verify(receiverOffset).topicPartition();
        Mockito.verify(receiverOffset).offset();
        Mockito.verify(receiverOffset).acknowledge();
    }


}
