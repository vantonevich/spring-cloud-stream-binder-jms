/*
 *  Copyright 2002-2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cloud.stream.binder.jms.solace;

import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.impl.DurableTopicEndpointImpl;
import com.solacesystems.jcsmp.impl.XMLContentMessageImpl;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import com.solacesystems.jms.SolJmsUtility;
import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.cloud.stream.binder.jms.solace.config.SolaceConfigurationProperties;
import org.springframework.core.io.ClassPathResource;

import javax.jms.ConnectionFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.solacesystems.jcsmp.JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST;
import static com.solacesystems.jcsmp.JCSMPSession.WAIT_FOR_CONFIRM;

public class SolaceTestUtils {

    public static final String APPLICATION_YML = "application.yml";

    public static final String DLQ_NAME = "#DEAD_MSG_QUEUE";
    public static final Queue DLQ = JCSMPFactory.onlyInstance().createQueue(DLQ_NAME);

    /**
     * Gets solace properties within application.yml, without requiring a Spring App.
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static SolaceConfigurationProperties getSolaceProperties() throws Exception {
        YamlMapFactoryBean factoryBean = new YamlMapFactoryBean();
        factoryBean.setResources(new ClassPathResource(APPLICATION_YML));

        Map<String, Object> mapObject = factoryBean.getObject();
        Map<String, String> solacePropertyMap = (Map<String, String>) mapObject.get("solace");

        SolaceConfigurationProperties solaceConfigurationProperties = new SolaceConfigurationProperties();
        solaceConfigurationProperties.setMaxRedeliveryAttempts(null);
        solaceConfigurationProperties.setUsername(solacePropertyMap.get("username"));
        solaceConfigurationProperties.setPassword(solacePropertyMap.get("password"));
        solaceConfigurationProperties.setHost(solacePropertyMap.get("host"));

        return solaceConfigurationProperties;
    }

    public static ConnectionFactory createConnectionFactory() throws Exception {
        SolaceConfigurationProperties solaceProperties = getSolaceProperties();
        ConnectionFactory cf = SolJmsUtility.createConnectionFactory(
                solaceProperties.getHost(),
                solaceProperties.getUsername(),
                solaceProperties.getPassword(),
                null,
                null
                );
        return cf;
    }

    public static JCSMPSession createSession() {
        try {
            return new SolaceQueueProvisioner.SessionFactory(getSolaceProperties()).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void deprovisionTopicAndDLQ(String topicName) throws JCSMPException {
        if (topicName != null) {
            TopicEndpoint topicEndpoint = new DurableTopicEndpointImpl(topicName);
            deprovision(topicEndpoint, DLQ);
        } else {
            deprovisionDLQ();
        }
    }

    public static void deprovisionTopicAndDLQ(Topic topic) throws JCSMPException {
        String name = topic != null ? topic.getName() : null;
        deprovisionTopicAndDLQ(name);
    }

    public static void deprovisionDLQ() throws JCSMPException {
        deprovision(DLQ);
    }

    public static void deprovision(Endpoint ... endPoints) throws JCSMPException {
        JCSMPSession session = createSession();
        try {
            for (Endpoint endPoint : endPoints) {
                session.deprovision(endPoint, WAIT_FOR_CONFIRM | FLAG_IGNORE_DOES_NOT_EXIST);
            }
        } finally {
            session.closeSession();
        }
    }

    public static BytesXMLMessage waitForDeadLetter() {
        return waitForDeadLetter(2000);
    }

    public static BytesXMLMessage waitForDeadLetter(int timeout) {

        ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
        consumerFlowProperties.setEndpoint(DLQ);

        CountDownLatch latch = new CountDownLatch(1);
        CountingListener countingListener = new CountingListener(latch);

        try {
            FlowReceiver consumer = createSession().createFlow(countingListener, consumerFlowProperties);
            consumer.start();

            boolean success = countingListener.awaitExpectedMessages(timeout);
            return success ? countingListener.getMessages().get(0) : null;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class CountingListener implements XMLMessageListener {
        private final CountDownLatch latch;

        private final List<JCSMPException> errors = new ArrayList<>();

        private final List<String> payloads = new ArrayList<>();
        private final List<BytesXMLMessage> messages = new ArrayList<>();

        public CountingListener(CountDownLatch latch) {
            this.latch = latch;
        }

        public CountingListener(int expectedMessages) {
            this.latch = new CountDownLatch(expectedMessages);
        }

        public List<BytesXMLMessage> getMessages() {
            return messages;
        }

        @Override
        public void onReceive(BytesXMLMessage bytesXMLMessage) {
            if (bytesXMLMessage instanceof XMLContentMessageImpl) {
                payloads.add(((XMLContentMessageImpl)bytesXMLMessage).getXMLContent());
            }
            else {
                payloads.add(bytesXMLMessage.toString());
            }

            messages.add(bytesXMLMessage);
            latch.countDown();
        }

        @Override
        public void onException(JCSMPException e) {
            errors.add(e);
        }

        boolean awaitExpectedMessages() throws InterruptedException {
            return awaitExpectedMessages(2000);
        }

        boolean awaitExpectedMessages(int timeout) throws InterruptedException {
            return latch.await(timeout, TimeUnit.MILLISECONDS);
        }

        List<JCSMPException> getErrors() {
            return errors;
        }

        List<String> getPayloads() {
            return payloads;
        }
    }

    public static class RollbackListener implements XMLMessageListener {

        private AtomicInteger receivedMessageCount = new AtomicInteger();

        private TransactedSession transactedSession;

        public RollbackListener(TransactedSession transactedSession) {
            this.transactedSession = transactedSession;
        }

        @Override
        public void onReceive(BytesXMLMessage bytesXMLMessage) {
            receivedMessageCount.incrementAndGet();
            try {
                transactedSession.rollback();
            } catch (JCSMPException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onException(JCSMPException e) {

        }

        public int getReceivedMessageCount() {
            return receivedMessageCount.get();
        }
    }

    public static void close(JCSMPSession ...sessions) {
        for (JCSMPSession session : sessions) {
            if (session != null) {
                session.closeSession();
            }
        }

    }

}
