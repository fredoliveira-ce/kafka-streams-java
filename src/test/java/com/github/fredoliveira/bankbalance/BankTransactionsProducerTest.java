package com.github.fredoliveira.bankbalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BankTransactionsProducerTest {

    @Test
    public void newRandomTransactionsTest() throws JsonProcessingException {
        var record = BankTransactionsProducer.newRandomTransaction("john");
        var key = record.key();
        var value = record.value();
        var mapper = new ObjectMapper();
        var node = mapper.readTree(value);

        assertEquals("john", key);
        assertEquals("john", node.get("name").asText());
        Assertions.assertTrue(node.get("amount").asInt() < 100, "Amount should be less than 100");
    }

}