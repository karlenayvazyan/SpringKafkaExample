package am.ak.spring.kafka.example;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SpringKafkaExampleApplication.class)
@ActiveProfiles("test")
public abstract class AbstractIntegrationTest {

    @ClassRule
    private static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        kafkaContainer.start();
        registry.add("spring.kafka.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.consumer.properties.auto.offset.reset", () -> "earliest");
    }
}
