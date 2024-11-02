package org.springframework.boot.autoconfigure.amqp;

import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.CredentialsRefreshService;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.RoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.amqp.MultiRabbitAutoConfiguration.MultiRabbitConnectionFactoryCreator;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.RabbitConnectionFactoryCreator;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ResourceLoader;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiRabbitConnectionFactoryCreatorTest {

    private static final String DUMMY_KEY = "dummy-key";

    @Mock
    private ConfigurableListableBeanFactory beanFactory;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private ObjectProvider<ConnectionNameStrategy> connectionNameStrategy;

    @Mock
    private ResourceLoader resourceLoader;

    @Mock
    private ObjectProvider<CredentialsProvider> credentialsProvider;

    @Mock
    private ObjectProvider<CredentialsRefreshService> credentialsRefreshService;

    @Mock
    private ObjectProvider<SslBundles> sslBundles;

    @Mock
    private ObjectProvider<ConnectionFactoryCustomizer> connectionFactoryCustomizer;

    @Mock
    private ConnectionFactory connectionFactory0;

    @Mock
    private ConnectionFactory connectionFactory1;

    @Mock
    private SimpleRabbitListenerContainerFactory containerFactory;

    @Mock
    private RabbitAdmin rabbitAdmin;

    @Mock
    private RabbitConnectionDetails rabbitConnectionDetails;

    @Mock
    private MultiRabbitProperties multiRabbitProperties;

    @Mock
    private MultiRabbitConnectionDetailsMap multiRabbitConnectionDetailsMap;

    @Mock
    private RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer;

    @Mock
    private CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer;

    @Mock
    private RabbitConnectionFactoryCreator rabbitConnectionFactoryCreator;

    @Mock
    private RabbitConnectionFactoryCreator multiRabbitConnectionFactoryCreator;

    @Mock
    private final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap
            = new MultiRabbitConnectionFactoryCreatorMap(multiRabbitProperties);

    private MultiRabbitConnectionFactoryCreator creator() {
        return creator(rabbitConnectionFactoryCreator, multiRabbitConnectionFactoryCreatorMap);
    }

    private MultiRabbitConnectionFactoryCreator creator(
            final RabbitConnectionFactoryCreator rabbitConnectionFactoryCreator,
            final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap) {
        final MultiRabbitConnectionFactoryCreator config
                = new MultiRabbitConnectionFactoryCreator(rabbitConnectionFactoryCreator,
                multiRabbitConnectionFactoryCreatorMap);
        config.setBeanFactory(beanFactory);
        config.setApplicationContext(applicationContext);
        return config;
    }

    @Test
    void shouldInstantiateExternalEmptyWrapper() {
        final MultiRabbitConnectionFactoryWrapper emptyWrapper = creator().externalEmptyWrapper();
        assertTrue(emptyWrapper.getEntries().isEmpty());
        assertNull(emptyWrapper.getDefaultConnectionFactory());
    }

    @Test
    void shouldInstantiateRoutingConnectionFactory() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);
        externalWrapper.addConnectionFactory(DUMMY_KEY, connectionFactory1, containerFactory, rabbitAdmin);

        final RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                rabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles);
        final CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                connectionNameStrategy);

        when(rabbitConnectionFactoryCreator.rabbitConnectionFactory(rabbitConnectionFactoryBeanConfigurer,
                rabbitCachingConnectionFactoryConfigurer, connectionFactoryCustomizer))
                .thenReturn(new CachingConnectionFactory());

        assertTrue(creator().routingConnectionFactory(rabbitConnectionDetails, multiRabbitProperties,
                multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader, credentialsProvider,
                credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer, sslBundles)
                instanceof RoutingConnectionFactory);
    }

    @Test
    void shouldInstantiateRoutingConnectionFactoryWithDefaultAndMultipleConnections() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);
        externalWrapper.addConnectionFactory(DUMMY_KEY, connectionFactory1, containerFactory, rabbitAdmin);

        final ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitConnectionDetails,
                multiRabbitProperties, multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader,
                credentialsProvider, credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer,
                sslBundles);

        assertTrue(routingConnectionFactory instanceof SimpleRoutingConnectionFactory);
        verify(beanFactory).registerSingleton(DUMMY_KEY, containerFactory);
        verify(beanFactory).registerSingleton(
                eq(DUMMY_KEY + MultiRabbitConstants.RABBIT_ADMIN_SUFFIX), any(RabbitAdmin.class)
        );
        verifyNoMoreInteractions(beanFactory);
    }

    @Test
    void shouldInstantiateRoutingConnectionFactoryWithOnlyDefaultConnectionFactory() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);

        final ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitConnectionDetails,
                multiRabbitProperties, multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader,
                credentialsProvider, credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer,
                sslBundles);

        assertTrue(routingConnectionFactory instanceof SimpleRoutingConnectionFactory);
        verifyNoMoreInteractions(beanFactory);
    }

    @Test
    void shouldInstantiateRoutingConnectionFactoryWithOnlyMultipleConnectionFactories() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);
        externalWrapper.addConnectionFactory(DUMMY_KEY, connectionFactory1, containerFactory, rabbitAdmin);
        final RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                rabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles);
        final CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                connectionNameStrategy);

        when(rabbitConnectionFactoryCreator.rabbitConnectionFactory(rabbitConnectionFactoryBeanConfigurer,
                rabbitCachingConnectionFactoryConfigurer, connectionFactoryCustomizer))
                .thenReturn(new CachingConnectionFactory());

        final ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitConnectionDetails,
                multiRabbitProperties, multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader,
                credentialsProvider, credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer,
                sslBundles);

        assertTrue(routingConnectionFactory instanceof SimpleRoutingConnectionFactory);
        verify(beanFactory).registerSingleton(DUMMY_KEY, containerFactory);
        verify(beanFactory).registerSingleton(
                eq(DUMMY_KEY + MultiRabbitConstants.RABBIT_ADMIN_SUFFIX), any(RabbitAdmin.class)
        );
        verifyNoMoreInteractions(beanFactory);
    }

    @Test
    void shouldReachDefaultConnectionFactoryWhenNotBound() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);
        externalWrapper.addConnectionFactory(DUMMY_KEY, connectionFactory1, containerFactory, rabbitAdmin);

        creator().routingConnectionFactory(rabbitConnectionDetails, multiRabbitProperties,
                multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader, credentialsProvider,
                credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer, sslBundles)
                .getVirtualHost();

        verify(connectionFactory0).getVirtualHost();
        verify(connectionFactory1, never()).getVirtualHost();
    }

    @Test
    void shouldBindAndReachMultiConnectionFactory() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);
        externalWrapper.addConnectionFactory(DUMMY_KEY, connectionFactory1, containerFactory, rabbitAdmin);

        final ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitConnectionDetails,
                multiRabbitProperties, multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader,
                credentialsProvider, credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer,
                sslBundles);

        SimpleResourceHolder.bind(routingConnectionFactory, DUMMY_KEY);
        routingConnectionFactory.getVirtualHost();
        SimpleResourceHolder.unbind(routingConnectionFactory);

        verify(connectionFactory0, never()).getVirtualHost();
        verify(connectionFactory1).getVirtualHost();
    }

    @Test
    void shouldInstantiateMultiRabbitConnectionFactoryWrapperWithDefaultConnection() throws Exception {
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        final RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                rabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles);
        final CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                connectionNameStrategy);
        when(rabbitConnectionFactoryCreator.rabbitConnectionFactory(rabbitConnectionFactoryBeanConfigurer,
                rabbitCachingConnectionFactoryConfigurer, connectionFactoryCustomizer))
                .thenReturn(new CachingConnectionFactory());

        assertNotNull(creator().routingConnectionFactory(rabbitConnectionDetails, null,
                multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader, credentialsProvider,
                credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer, sslBundles));
    }

    @Test
    void shouldInstantiateMultiRabbitConnectionFactoryWrapperWithMultipleConnections() throws Exception {
        final MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(DUMMY_KEY, new RabbitProperties());
        multiRabbitProperties.setDefaultConnection(DUMMY_KEY);

        final RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                rabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles);
        final CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer
                = rabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                connectionNameStrategy);

        when(rabbitConnectionFactoryCreator.rabbitConnectionFactory(eq(rabbitConnectionFactoryBeanConfigurer),
                eq(rabbitCachingConnectionFactoryConfigurer),
                eq(connectionFactoryCustomizer)))
                .thenReturn(new CachingConnectionFactory());

        final RabbitConnectionFactoryCreator rabbitConnectionFactoryCreator
                = mock(RabbitConnectionFactoryCreator.class);
        final RabbitConnectionFactoryCreator multiRabbitConnectionFactoryCreator
                = mock(RabbitConnectionFactoryCreator.class);
        final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap
                = new MultiRabbitConnectionFactoryCreatorMap(Map.of(DUMMY_KEY, multiRabbitConnectionFactoryCreator));

        final RabbitConnectionDetails multiRabbitConnectionDetails = mock(RabbitConnectionDetails.class);
        final MultiRabbitConnectionDetailsMap multiRabbitConnectionDetailsMap
                = new PropertiesMultiRabbitConnectionDetailsMap(Map.of(DUMMY_KEY, multiRabbitConnectionDetails));

        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                multiRabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles))
                .thenReturn(rabbitConnectionFactoryBeanConfigurer);

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(multiRabbitConnectionDetails,
                connectionNameStrategy)).thenReturn(rabbitCachingConnectionFactoryConfigurer);

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactory(rabbitConnectionFactoryBeanConfigurer,
                rabbitCachingConnectionFactoryConfigurer, connectionFactoryCustomizer))
                .thenReturn(new CachingConnectionFactory());

        final MultiRabbitConnectionFactoryCreator creator = creator(rabbitConnectionFactoryCreator,
                multiRabbitConnectionFactoryCreatorMap);
        creator.routingConnectionFactory(rabbitConnectionDetails, multiRabbitProperties,
                multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader, credentialsProvider,
                credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer, sslBundles);

        verify(this.rabbitConnectionFactoryCreator).rabbitConnectionFactory(
                this.rabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                        rabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles),
                this.rabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                        connectionNameStrategy),
                connectionFactoryCustomizer);
    }

    @Test
    void shouldInstantiateMultiRabbitConnectionFactoryWrapperWithDefaultAndMultipleConnections() throws Exception {
        final MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(DUMMY_KEY, new RabbitProperties());

        final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap
                = new MultiRabbitConnectionFactoryCreatorMap(Map.of(DUMMY_KEY, multiRabbitConnectionFactoryCreator));

        final RabbitConnectionDetails multiRabbitConnectionDetails = mock(RabbitConnectionDetails.class);
        final MultiRabbitConnectionDetailsMap multiRabbitConnectionDetailsMap
                = new PropertiesMultiRabbitConnectionDetailsMap(Map.of(DUMMY_KEY, multiRabbitConnectionDetails));

        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();
        externalWrapper.setDefaultConnectionFactory(connectionFactory0);

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                multiRabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles))
                .thenReturn(rabbitConnectionFactoryBeanConfigurer);

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(multiRabbitConnectionDetails,
                connectionNameStrategy)).thenReturn(rabbitCachingConnectionFactoryConfigurer);

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactory(rabbitConnectionFactoryBeanConfigurer,
                rabbitCachingConnectionFactoryConfigurer, connectionFactoryCustomizer))
                .thenReturn(new CachingConnectionFactory());

        final MultiRabbitConnectionFactoryCreator creator = creator(rabbitConnectionFactoryCreator,
                multiRabbitConnectionFactoryCreatorMap);
        creator.routingConnectionFactory(rabbitConnectionDetails, multiRabbitProperties,
                multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader, credentialsProvider,
                credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer, sslBundles);

        verify(multiRabbitConnectionFactoryCreator).rabbitConnectionFactory(
                rabbitConnectionFactoryBeanConfigurer,
                rabbitCachingConnectionFactoryConfigurer,
                connectionFactoryCustomizer);

        verify(multiRabbitConnectionFactoryCreator).rabbitConnectionFactory(
                multiRabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                        multiRabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles),
                multiRabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                        connectionNameStrategy),
                connectionFactoryCustomizer);

        verify(rabbitConnectionFactoryCreator).rabbitConnectionFactory(
                rabbitConnectionFactoryCreator.rabbitConnectionFactoryBeanConfigurer(resourceLoader,
                        rabbitConnectionDetails, credentialsProvider, credentialsRefreshService, sslBundles),
                rabbitConnectionFactoryCreator.rabbitConnectionFactoryConfigurer(rabbitConnectionDetails,
                        connectionNameStrategy),
                connectionFactoryCustomizer);
    }

    @Test
    void shouldEncapsulateExceptionWhenFailingToCreateBean() throws Exception {
        final RabbitProperties rabbitProperties = new RabbitProperties();
        final MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(DUMMY_KEY, rabbitProperties);

        final RabbitConnectionDetails rabbitConnectionDetails = new PropertiesRabbitConnectionDetails(rabbitProperties);
        final MultiRabbitConnectionFactoryWrapper externalWrapper = new MultiRabbitConnectionFactoryWrapper();

        final MultiRabbitConnectionDetailsMap multiRabbitConnectionDetailsMap
                = new PropertiesMultiRabbitConnectionDetailsMap(multiRabbitProperties);
        final RabbitConnectionFactoryCreator rabbitConnectionFactoryCreator
                = new RabbitConnectionFactoryCreator(rabbitProperties);
        final MultiRabbitConnectionFactoryCreatorMap multiRabbitConnectionFactoryCreatorMap
                = new MultiRabbitConnectionFactoryCreatorMap(Map.of(DUMMY_KEY, multiRabbitConnectionFactoryCreator));

        when(multiRabbitConnectionFactoryCreator.rabbitConnectionFactory(
                any(), any(), any())).thenThrow(new Exception("mocked-exception"));

        final MultiRabbitConnectionFactoryCreator creator = creator(rabbitConnectionFactoryCreator,
                multiRabbitConnectionFactoryCreatorMap);

        final Executable executable = () -> creator.routingConnectionFactory(rabbitConnectionDetails,
                multiRabbitProperties, multiRabbitConnectionDetailsMap, externalWrapper, resourceLoader,
                credentialsProvider, credentialsRefreshService, connectionNameStrategy, connectionFactoryCustomizer,
                sslBundles);

        assertThrows(Exception.class, executable, "mocked-exception");
    }
}
