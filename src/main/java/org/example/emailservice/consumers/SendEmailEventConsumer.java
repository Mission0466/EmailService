package org.example.emailservice.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.emailservice.dtos.SendEmailEventDto;
import org.example.emailservice.utils.EmailUtil;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import java.util.Properties;

@Service
public class SendEmailEventConsumer {

    private ObjectMapper objectMapper;

    public SendEmailEventConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "sendEmail", groupId = "emailService")
    public void handleSendEmailEvent(String message) throws JsonProcessingException {
        // 1. Convert message to SendEmailEventDto
        // 2. Send email
        SendEmailEventDto sendEmailEventDto = null;

        sendEmailEventDto = objectMapper.readValue(
                message, SendEmailEventDto.class
        );

        String to = sendEmailEventDto.getTo();
        String from = sendEmailEventDto.getFrom();
        String subject = sendEmailEventDto.getSubject();
        String body = sendEmailEventDto.getBody();


        Properties props = new Properties();
        props.put("mail.smtp.host", "smtp.gmail.com"); //SMTP Host
        props.put("mail.smtp.port", "587"); //TLS Port
        props.put("mail.smtp.auth", "true"); //enable authentication
        props.put("mail.smtp.starttls.enable", "true"); //enable STARTTLS

        //create Authenticator object to pass in Session.getInstance argument
        Authenticator auth = new Authenticator() {
            //override the getPasswordAuthentication method
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("nani.mullangi97@gmail.com", "qtprvtwiikoqesyv");
            }
        };
        Session session = Session.getInstance(props, auth);

        EmailUtil.sendEmail(
                session,
                to,
                subject,
                body);


    }
}
