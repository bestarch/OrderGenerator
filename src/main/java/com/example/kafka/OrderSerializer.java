package com.example.kafka;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.example.kafka.domain.Order;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderSerializer implements Serializer<Order> {

	private ObjectMapper objectMapper;

	public OrderSerializer() {
		this.objectMapper = new ObjectMapper();
		this.objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		// left empty
	}

	public byte[] serialize(String topic, Order data) {
		try {
			byte[] result = null;
			if (data != null) {
				result = this.objectMapper.writeValueAsBytes(data);
			}
			return result;
		} catch (IOException ex) {
			throw new SerializationException(ex);
		}
	}

	public void close() {
		// left empty
	}
}
