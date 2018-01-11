package com.example.kafka;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.example.kafka.domain.Order;

public class OrderGenerator {

	private static String[] CATEGORY = { "Mobile phone", "Electronics", "Cosmetics", "Furniture" };
	private static String[] ITEMS = { 
			"iPhone", "Samsung android phone", "Nokia", "Mi", "Sony experia", 
			"TV", "Air conditioner", "Home theater", "Refrigerator", "DVD Player", 
			"Sunscreen", "Shampoo", "Make-up kit", "Perfume", "Lipstick", 
			"Table", "Chair", "Dining table", "Almirah", "Bed" };
	private static String[] ZIPCODE = { "20120", "20111", "29000", "10011" };
	private static String[] PAYMENT = { "Credit Card", "Cash on delivery", "Netbanking", "Wallet" };
	
	private static String INPUT_ORDER = "freshorders";
	
	private static Map<String, Double> price = new HashMap<>();
	
	static {
		price.put("iPhone", 50700d);
		price.put("Samsung android phone", 40000d);
		price.put("Nokia", 20670d);
		price.put("Mi", 10990d);
		price.put("Sony experia", 40999d);
		price.put("TV", 80000d);
		price.put("Air conditioner", 30000d);
		price.put("Home theater", 19000d);
		price.put("Refrigerator", 34540d);
		price.put("DVD Player", 7999d);
		price.put("Sunscreen", 365d);
		price.put("Shampoo", 240d);
		price.put("Make-up kit", 1200d);
		price.put("Perfume", 3450d);
		price.put("Lipstick", 800d);
		price.put("Table", 11500d);
		price.put("Chair", 4500d);
		price.put("Dining table", 25400d);
		price.put("Almirah", 21500d);
		price.put("Bed", 35000d);
	}

	public static void main(String[] args) {
		List<Order> list = new ArrayList<>();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		KafkaProducer<Long, Order> producer = (KafkaProducer<Long, Order>) OrderProducerConfig.createProducer();
		
		executorService.submit(()->{
			int count = 1;
			Random random = new Random();
			Long msgId = 1l;
			while (count <= 5000) {
				Order order = createRandomOrder(list, count++, random);
				System.out.println(order.toString());
				producer.send(new ProducerRecord<>(INPUT_ORDER, msgId++, order), (RecordMetadata,Exception)->System.out.println("Export completed"));
				try {
					TimeUnit.SECONDS.sleep(8);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		executorService.shutdown();
	}

	private static Order createRandomOrder(List<Order> list, int count, Random random) {
		int i = random.nextInt(4);
		String item = ITEMS[random.nextInt(5) + 4 * i];
		int quantity = new Random().nextInt(2) + 1;
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, 3 * i);
		Order order = new Order(UUID.randomUUID().toString(), item, ZIPCODE[random.nextInt(4)], quantity,
				quantity * price.get(item), PAYMENT[random.nextInt(4)], cal.getTime(),
				"ID" + random.nextInt(100) + count, CATEGORY[i]);
		list.add(order);
		return order;
	}

}
