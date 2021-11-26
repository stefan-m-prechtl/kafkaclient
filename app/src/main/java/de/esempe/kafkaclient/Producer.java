package de.esempe.kafkaclient;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer 
{
	private KafkaProducer<String,String> producer;
	private String brokers;
	private Properties properties;
	
	public Producer(final String bootstrapServer)
	{
		this.brokers = bootstrapServer;
		this.properties = createProperties();
		this.producer = new KafkaProducer<>(properties);
	}
	
	public void produceMsg(String topic, String key, String msg) 
	{
		try 
		{
			producer.send(new ProducerRecord<String, String>(topic, key, msg), new Callback() 
			{
				@Override
				public void onCompletion(RecordMetadata m, Exception e) 
				{
					if (e != null) 
					{
						e.printStackTrace();
					} 
					else 
					{
						System.out.printf("Produced record to topic %s in partition [%d] @offset %d (Timestamp: %d)%n", m.topic(), m.partition(), m.offset(), m.timestamp());
					}
		          }
				});
			producer.flush();
		} 
		catch (Exception e) 
		{
			System.out.print("Produce message failed\n");
			System.out.print(e.getMessage());
		}
	}


	private Properties createProperties() 
	{
		var properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
		
		return properties;
	}
	
	
}
