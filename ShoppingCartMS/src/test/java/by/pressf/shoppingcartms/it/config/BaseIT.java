package by.pressf.shoppingcartms.it.config;

import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Import(TestConsumerConfig.class)
public abstract class BaseIT {
    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
                    .withDatabaseName("test_ms_db")
                    .withUsername("postgres")
                    .withPassword("root");

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    private static void overrideProperties(DynamicPropertyRegistry registry) {
        // Меняем настройки контекста для брокера сообщений
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.producer.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", KAFKA::getBootstrapServers);

        // Меняем настройки контекста для база данных
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        // Меняем настройки контекста для liquibase
        registry.add("spring.liquibase.url", POSTGRES::getJdbcUrl);
        registry.add("spring.liquibase.user", POSTGRES::getUsername);
        registry.add("spring.liquibase.password", POSTGRES::getPassword);
    }

    protected static final String TOPIC = "r-order-w-cart-events";

    @Autowired
    protected MockMvc mockMvc;
    @Autowired
    protected ObjectMapper mapper;
    @Autowired
    protected Consumer<String, String> spyConsumer;
    @Autowired
    protected ShoppingCartRepository shoppingCartRepository;
}
