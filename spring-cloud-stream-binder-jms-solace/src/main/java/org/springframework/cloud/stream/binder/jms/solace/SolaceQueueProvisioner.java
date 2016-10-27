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

import java.util.Arrays;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.jms.solace.config.SolaceConfigurationProperties;
import org.springframework.cloud.stream.binder.jms.spi.QueueProvisioner;
import org.springframework.jms.support.JmsUtils;

import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.TopicEndpoint;
import com.solacesystems.jcsmp.impl.DurableTopicEndpointImpl;
import com.solacesystems.jms.SolJmsUtility;

/**
 * {@link QueueProvisioner} for Solace.
 *
 * @author Jack Galilee
 * @author Jonathan Sharpe
 * @author Joseph Taylor
 * @author José Carlos Valero
 * @since 1.1
 */
public class SolaceQueueProvisioner implements QueueProvisioner {
    protected final Log logger = LogFactory.getLog(this.getClass());
    private static final String DMQ_NAME = "#DEAD_MSG_QUEUE";
    private final SessionFactory sessionFactory;
    private final ConnectionFactory connectionFactory;

    private SolaceConfigurationProperties solaceConfigurationProperties;

    public SolaceQueueProvisioner(SolaceConfigurationProperties solaceConfigurationProperties) throws Exception {
        this.solaceConfigurationProperties = solaceConfigurationProperties;
        this.sessionFactory = new SessionFactory(solaceConfigurationProperties);
        this.connectionFactory = SolJmsUtility.createConnectionFactory(
                solaceConfigurationProperties.getHost(),
                solaceConfigurationProperties.getUsername(),
                solaceConfigurationProperties.getPassword(),
                null,
                null
        );
    }

    @Override
    public Destinations provisionTopicAndConsumerGroup(String name, String... groups) {
        Destinations.Factory destinationsFactory = new Destinations.Factory();
        if (logger.isDebugEnabled()) {
            logger.debug("provisionTopicAndConsumerGroup called with name:" + name +
                    " groups:" + Arrays.toString(groups));
        }
        Connection connection = null;
        javax.jms.Session jmsSession = null;
        try {
            Topic topic = JCSMPFactory.onlyInstance().createTopic(name);
            JCSMPSession session = sessionFactory.build();
            connection = connectionFactory.createConnection();
            jmsSession = connection.createSession(/*transacted:*/false,
                        /*acknowledgeMode:*/Session.AUTO_ACKNOWLEDGE);
            // Using Durable... because non-durable Solace TopicEndpoints don't have names
            TopicEndpoint topicEndpoint = new DurableTopicEndpointImpl(name);
            if (logger.isDebugEnabled()) {
                logger.debug("Provisioning Solace topic " + name);
            }
            session.provision(topicEndpoint, null, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            if (logger.isDebugEnabled()) {
                logger.debug("Solace topic " + name+ "provisioned OK.");
            }

            javax.jms.Topic jmsTopic = jmsSession.createTopic(name);
            if (logger.isDebugEnabled()) {
                logger.debug("JMS topic " + name+ "created OK.");
            }
            destinationsFactory.withTopic(jmsTopic);
            if (!ArrayUtils.isEmpty(groups)) {
                for (String group : groups) {
                    if (group == null || group.isEmpty()) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Received request to create empty queue for topic " + name);
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Creating JMS queue for group " + group);
                        }
                        javax.jms.Queue destinationGroup = jmsSession.createQueue(group);
                        if (logger.isDebugEnabled()) {
                            logger.debug("JMS queue for group " + name + " created OK.");
                        }
                        destinationsFactory.addGroup(destinationGroup);
                        doProvision(session, topic, group);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Provisioning Solace queue for group " + group + " completed OK.");
                        }
                    }
                }
            }
            logger.debug("Commiting JMS session....");
            JmsUtils.commitIfNecessary(jmsSession);
            logger.debug("Closing JMS session....");
            jmsSession.close();
            jmsSession = null;
            logger.debug("Closing JMS connection....");
            connection.close();
            connection = null;
            logger.debug("provisionTopicAndConsumerGroup completed OK.");
        } catch (JCSMPErrorResponseException e) {
            if (JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT != e.getSubcodeEx()) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            JmsUtils.closeSession(jmsSession);
            JmsUtils.closeConnection(connection);
        }
        return destinationsFactory.build();
    }

    @Override
    public String provisionDeadLetterQueue() {
        EndpointProperties properties = new EndpointProperties();
        properties.setPermission(EndpointProperties.PERMISSION_DELETE);
        properties.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

        Queue deadMsgQueue = JCSMPFactory.onlyInstance().createQueue(DMQ_NAME);

        try {
            sessionFactory.build().provision(deadMsgQueue, properties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return DMQ_NAME;
    }

    private void doProvision(JCSMPSession session, Topic topic, String group) throws Exception {
        if (group != null) {

            Queue addedQueue = JCSMPFactory.onlyInstance().createQueue(group);

            EndpointProperties endpointProperties = new EndpointProperties();
            endpointProperties.setPermission(EndpointProperties.PERMISSION_DELETE);
            endpointProperties.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

            Integer maxRedeliveryAttempts = solaceConfigurationProperties.getMaxRedeliveryAttempts();
            if (maxRedeliveryAttempts != null && maxRedeliveryAttempts >= 0) {
                endpointProperties.setMaxMsgRedelivery(maxRedeliveryAttempts);
            }

            endpointProperties.setQuota(100);

            session.provision(addedQueue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            session.addSubscription(addedQueue, topic, JCSMPSession.WAIT_FOR_CONFIRM);
        }
    }

    protected static class SessionFactory {
        private SolaceConfigurationProperties properties;

        public SessionFactory(SolaceConfigurationProperties properties) {
            this.properties = properties;
        }

        public JCSMPSession build() throws InvalidPropertiesException {
            JCSMPProperties sessionProperties = new JCSMPProperties();
            sessionProperties.setProperty("host", properties.getHost());
            sessionProperties.setProperty("username", properties.getUsername());
            sessionProperties.setProperty("password", properties.getPassword());

            return JCSMPFactory.onlyInstance().createSession(sessionProperties);
        }
    }
}