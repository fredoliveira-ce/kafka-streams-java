package com.github.fredoliveira.bankbalance;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;

@JsonSchemaInject(strings =
        {@JsonSchemaString(path="javaType", value="com.github.fredoliveira.bankbalance.UserBalance")})
public record UserBalance(
        @JsonProperty String name,
        @JsonProperty int amount,
        @JsonProperty String time
){ }