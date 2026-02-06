package by.pressf.shoppingcartms.unit.service;

import by.pressf.shoppingcartms.dao.repository.ShoppingCartRepository;
import by.pressf.shoppingcartms.service.ShoppingCartService;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
class CartServiceUnitTests {
    @Mock
    private Environment env;
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Mock
    private ShoppingCartRepository shoppingCartRepository;

    @InjectMocks
    private ShoppingCartService cartService;


}
