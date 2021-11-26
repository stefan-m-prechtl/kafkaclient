package de.esempe.kafkaclient;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Admin 
{
	private String brokers;
	private Properties properties;
	private AdminClient adminClient;
	
	public Admin(final String bootstrapServer)
	{
		this.brokers = bootstrapServer;
		this.properties = createProperties();
		this.adminClient = KafkaAdminClient.create(this.properties);
	}

	private Properties createProperties()
	{
		var properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

		return properties;
	}
	
	public void describeTopics() throws InterruptedException, ExecutionException
	{
		var result = adminClient.listTopics();
		var topics = result.names().get();
		for (String topicName : topics) {
			final var describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
			var description = describeTopicsResult.values().get(topicName).get();
			System.out.println(description.toString());
		}
	}
	
	public Properties getProperties()
	{
		return this.properties;
	}
	
}
