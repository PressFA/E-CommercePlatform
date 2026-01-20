package by.pressf.emailnotificationms.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class AppConfig {
    private final Environment env;

    @Bean
    public JavaMailSender mailSender() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
        mailSender.setHost(env.getRequiredProperty("spring.mail.host"));
        mailSender.setPort(Integer.parseInt(env.getRequiredProperty("spring.mail.port")));
        mailSender.setUsername(env.getRequiredProperty("spring.mail.username"));
        mailSender.setPassword(env.getRequiredProperty("spring.mail.password"));

        Properties props = mailSender.getJavaMailProperties();
        props.put("mail.smtp.auth",
                env.getRequiredProperty("spring.mail.properties.mail.smtp.auth"));
        props.put("mail.smtp.starttls.enable",
                env.getRequiredProperty("spring.mail.properties.mail.smtp.starttls.enable"));

        return mailSender;
    }
}
