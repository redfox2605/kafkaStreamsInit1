package com.github.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class BankTransactionProducerTest {


    @Test
    public void newRandomTransacionTest(){
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction("helena");
        String key = record.key();
        String value = record.value();
        assertEquals(key, "helena");
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertEquals(node.get("name").asText(), "helena");
            assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100);

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(value);
    }
}
