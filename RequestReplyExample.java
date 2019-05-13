package org.javaee7.extra.camel;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.jboss.logging.Logger;

public class RequestReplyExample {
	
	static Logger log=Logger.getLogger(RequestReplyExample.class); 

	public static void main(final String[] args) throws Exception { 

		final Map<String, TextMessage> requestMap = new HashMap<String, TextMessage>();

		Connection connection = null;

		String brokerURL = "(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&retryInterval=5000&retryIntervalMultiplier=1.0";

		String username = "aaaaaa";

		String password = "mmmmmm";

		String requestQueueName = "CAPI.INT.APIPROCESSOR.TOKENIZATION.RQ.Queue";

		String responseQueueName = "CAPI.INT.APIPROCESSOR.TOKENIZATION.RS.Queue";

		String detokenReq = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns2:DecodeTokenRq xmlns:ns2=\"http://api.dbs.com/capi/xsd/tokenization/v1_0\"><ns2:PartyToken>17520907939856225154001</ns2:PartyToken></ns2:DecodeTokenRq>";

		try {

			RequestConsumer requestConsumer = new RequestConsumer();
			

			Queue requestQueue = new ActiveMQQueue(requestQueueName);

			ConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, username, password);

			connection = cf.createConnection();

			connection.start();

			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			System.out.println(connection.getMetaData().getJMSXPropertyNames());

			final MessageProducer producer = session.createProducer(requestQueue);

			Queue replyQueue = new ActiveMQQueue(responseQueueName);
			List<String> messageIds = new ArrayList<>();

			TextMessage[] requestMsgs = textMesaages(detokenReq, session, replyQueue);

			Arrays.stream(requestMsgs).forEach(i -> {
				try {
					producer.send(i);
					log.info(i.getJMSMessageID());
					messageIds.add(i.getJMSMessageID());
				} catch (JMSException e1) {
					e1.printStackTrace();
				}
			});

			log.info("Request message sent."); 

			log.info(messageIds);
			  messageIds.forEach(messageId -> {
				MessageConsumer replyConsumer;
				try {
					replyConsumer = session.createConsumer(replyQueue, "JMSCorrelationID='" + messageId + "'");
					//replyConsumer = session.createConsumer(replyQueue);
					TextMessage replyMessageReceived = (TextMessage) replyConsumer.receive(5000);
					log.info("-------------Start--------------------");
					if (replyMessageReceived == null) {
						log.info("Time out for messgaeid : " + messageId);
					} else {
						log.info("Got reply for CorrelatedId: " + replyMessageReceived.getJMSCorrelationID());
						// log.info("Received reply: " + replyMessageReceived.getText());
					}
					log.info("----------------END-----------------");
					//replyConsumer.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}

			});   

			producer.close();

		} catch (JMSException e) {
			e.printStackTrace();
		} finally {

			if (connection != null) {

				connection.close();

			}

		}

	}

	private static TextMessage[] textMesaages(String detokenReq, Session session, Queue replyQueue)
			throws JMSException {
		TextMessage[] textMesaages = new TextMessage[10];
		for (int i = 0; i < 10; i++) {

			textMesaages[i] = session.createTextMessage(detokenReq);
			textMesaages[i].setJMSReplyTo(replyQueue);
			System.out.println(textMesaages[i].getJMSCorrelationID());
		}
		return textMesaages;
	}

}

class RequestConsumer implements MessageListener{
private Session session = null;
final 	String detokenReq = "Reply";
final String brokerURL = "(tcp://localhost:61616,tcp://localhost:61617)?ha=true&reconnectAttempts=-1&retryInterval=5000&retryIntervalMultiplier=1.0";
final String username = "aaaaaa";
final String password = "mmmmmm";
	public RequestConsumer() throws Exception{

		String requestQueueName = "CAPI.INT.APIPROCESSOR.TOKENIZATION.RQ.Queue";
		String responseQueueName = "CAPI.INT.APIPROCESSOR.TOKENIZATION.RS.Queue";
			Queue requestQueue = new ActiveMQQueue(requestQueueName);
			Queue replyQueue = new ActiveMQQueue(responseQueueName);

			ConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, username, password);

			Connection connection = cf.createConnection();

			connection.start();

			this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			MessageConsumer consumer = session.createConsumer(requestQueue);
			consumer.setMessageListener(this);

	}

	@Override
	public void onMessage(Message message) {
		try{

			ConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, username, password);

			Connection connection = cf.createConnection();

			connection.start();

			this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			MessageProducer producer = session.createProducer(null);


		TextMessage replyMessage = session.createTextMessage(detokenReq);
		replyMessage.setJMSCorrelationID(message.getJMSMessageID());
		producer.send(message.getJMSReplyTo(), replyMessage);
		this.session.close();
		connection.close();
	}catch(Exception e){
		e.printStackTrace();
	}
	}

}