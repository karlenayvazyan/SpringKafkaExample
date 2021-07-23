package am.ak.spring.kafka.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SpringKafkaExampleApplicationTests extends AbstractIntegrationTest {

    @Value("${io.kafka.topic.name}")
    private String topicName;

    @Autowired
    private KafkaProperties properties;

    @Test
    void contextLoads() {
        final Consumer<String, String> consumer = createConsumer(topicName);
        final ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 5000);
        final List<String> actualValues = new ArrayList<>();
        final List<String> expected = new ArrayList<>();
        LongStream.range(0, 10).forEach(i -> {
            String key = "kafka";
            String value = key.toUpperCase() + i;
            expected.add(value);
        });
        records.forEach(stringStringConsumerRecord -> {
            actualValues.add(stringStringConsumerRecord.value());
        });

        assertEquals(expected, actualValues);
    }

    private Consumer<String, String> createConsumer(String topicName) {
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                properties.buildConsumerProperties(), StringDeserializer::new, StringDeserializer::new)
                .createConsumer();

        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }
}
