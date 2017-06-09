package com.swisscom.api.controllers;


import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.swisscom.api.AllServiceConfiguration;
import com.swisscom.api.ResourceUtil;


@RestController
@RequestMapping("/rabbitmq")
public class RabbitMqController {

	@Autowired
	private AllServiceConfiguration sc;
	ResourceUtil rutil = new ResourceUtil();


	  private final static String FIRST_QUEUE = "FirstQueue";
	  private final static String SECOND_QUEUE = "SecondQueue";
	@RequestMapping(path = "/push", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> pushinQueue(@RequestParam("this") String it) throws Exception {
		   Channel channel;
		try {
			Connection connection = (Connection) sc.getServiceInstance();
			channel = connection.createChannel();
			
	
		    channel.queueDeclare(FIRST_QUEUE, false, false, false, null);
		    for (int i = 0; i <= 100000; i++) {
		    String	ite=it+i;
			    channel.basicPublish("", FIRST_QUEUE, null, ite.getBytes());
							
			}
		    System.out.println(" [x] Sent '" + it + "'");
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		} catch (IOException e) {
			return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
		}			
		
	  return new ResponseEntity<String>(HttpStatus.OK);
	}
	@RequestMapping(path = "/push1", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> pushinQueue1() throws Exception  {
		   Channel channel;
		try {
			Connection connection = (Connection) sc.getServiceInstance();
			channel = connection.createChannel();
		
		    channel.queueDeclare(SECOND_QUEUE, false, false, false, null);
			String message = "AppCloudTesting";
		      channel.basicPublish("", SECOND_QUEUE, null,message.getBytes() );
			System.out.println(" [x] Sent '" + message + "'");
		} catch (IOException e) {
			 return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
		}			
	  return new ResponseEntity<String>(HttpStatus.OK);
	}
	
	@RequestMapping(path = "/recieve", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> failSearch() {
		Channel channel ;
		try{
			Connection connection = (Connection) sc.getServiceInstance();
			channel = connection.createChannel();
		    channel.queueDeclare(FIRST_QUEUE, false, false, false, null);
		    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		    Consumer consumer = new DefaultConsumer(channel) {
		      @Override
		      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
		          throws IOException {
		        String message = new String(body, "UTF-8");
		        System.out.println(" [x] Received '" + message);
		      }
		    };
		    channel.basicConsume(FIRST_QUEUE, true, consumer);
		}catch (Exception e) {
			System.out.println("I am Catching the Exception expected");
			  return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
		}	
		return new ResponseEntity<String>(HttpStatus.OK);
	}

}
