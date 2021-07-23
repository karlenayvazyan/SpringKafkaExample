package am.ak.spring.kafka.example;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.SuccessCallback;

import java.util.Locale;
import java.util.stream.LongStream;

@SpringBootApplication
public class SpringKafkaExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaExampleApplication.class, args);
    }

    @Value("${io.kafka.topic.name}")
    private String topicName;
    @Value("${io.kafka.topic.replication}")
    private Short replication;
    @Value("${io.kafka.topic.partitions}")
    private Integer partitions;

    @Bean
    NewTopic myTestTopic() {
        return new NewTopic(topicName, partitions, replication);
    }

}

@Component
@RequiredArgsConstructor
@Log4j2
class Producer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @EventListener(ApplicationStartedEvent.class)
    public void produce() {
        LongStream.range(0, 10)
                .forEach(i -> {
                            String key = "kafka";
                            String value = key.toUpperCase() + i;
                            kafkaTemplate.send("topic", key, value)
                                    .addCallback(this::successResult, log::error);
                        }
                );
        kafkaTemplate.flush();
    }

    private void successResult(SendResult<String, String> result) {
        if (result != null) {
            RecordMetadata recordMetadata = result.getRecordMetadata();
            log.info("produced to {}, {}, {}",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
    }
}
