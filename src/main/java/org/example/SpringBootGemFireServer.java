package org.example;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.DistributionLocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.gemfire.CacheFactoryBean;
import org.springframework.data.gemfire.PartitionedRegionFactoryBean;
import org.springframework.data.gemfire.RegionAttributesFactoryBean;
import org.springframework.data.gemfire.server.CacheServerFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A Spring Boot application bootstrapping a Pivotal GemFire Server in the JVM process.
 *
 * @author John Blum
 * @see org.springframework.boot.SpringApplication
 * @see org.springframework.boot.autoconfigure.SpringBootApplication
 * @see org.springframework.context.annotation.Bean
 * @see com.gemstone.gemfire.cache.Cache
 * @see com.gemstone.gemfire.cache.server.CacheServer
 * @since 1.0.0
 */
@SpringBootApplication
@SuppressWarnings("unused")
public class SpringBootGemFireServer {

	protected static final int DEFAULT_CACHE_SERVER_PORT = CacheServer.DEFAULT_PORT;
	protected static final int DEFAULT_LOCATOR_PORT = DistributionLocator.DEFAULT_LOCATOR_PORT;
	protected static final int DEFAULT_MANAGER_PORT = 1099;

	protected static final String DEFAULT_LOG_LEVEL = "config";

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		SpringApplication.run(SpringBootGemFireServer.class);
	}

	String applicationName() {
		return SpringBootGemFireServer.class.getSimpleName();
	}

	@Bean
	Properties gemfireProperties(
			@Value("${spring.gemfire.log-level:"+DEFAULT_LOG_LEVEL+"}") String logLevel,
			@Value("${spring.gemfire.locators:localhost["+DEFAULT_LOCATOR_PORT+"]}") String locators,
			@Value("${spring.gemfire.manager.port:"+DEFAULT_MANAGER_PORT+"}") int managerPort,
			@Value("${spring.gemfire.manager.start:false}") boolean jmxManagerStart,
			@Value("${spring.gemfire.start-locator}") String startLocator) {

		logger.warn("spring.gemfire.log-level is [{}]", logLevel);

		Properties gemfireProperties = new Properties();

		gemfireProperties.setProperty("name", applicationName());
		gemfireProperties.setProperty("mcast-port", "0");
		gemfireProperties.setProperty("log-level", logLevel);
		gemfireProperties.setProperty("locators", locators);
		gemfireProperties.setProperty("jmx-manager", "true");
		gemfireProperties.setProperty("jmx-manager-port", String.valueOf(managerPort));
		gemfireProperties.setProperty("jmx-manager-start", String.valueOf(jmxManagerStart));

		if (StringUtils.hasText(startLocator)) {
			gemfireProperties.setProperty("start-locator", startLocator);
		}

		return gemfireProperties;
	}

	@Bean
	CacheFactoryBean gemfireCache(@Qualifier("gemfireProperties") Properties gemfireProperties) {
		CacheFactoryBean gemfireCache = new CacheFactoryBean();

		gemfireCache.setClose(true);
		gemfireCache.setProperties(gemfireProperties);

		return gemfireCache;
	}

	@Bean
	CacheServerFactoryBean gemfireCacheServer(Cache gemfireCache,
			@Value("${spring.gemfire.cache.server.bind-address:localhost}") String bindAddress,
			@Value("${spring.gemfire.cache.server.hostname-for-clients:localhost}") String hostnameForClients,
			@Value("${spring.gemfire.cache.server.port:"+DEFAULT_CACHE_SERVER_PORT+"}") int port) {

		CacheServerFactoryBean gemfireCacheServer = new CacheServerFactoryBean();

		gemfireCacheServer.setAutoStartup(true);
		gemfireCacheServer.setCache(gemfireCache);
		gemfireCacheServer.setBindAddress(bindAddress);
		gemfireCacheServer.setHostNameForClients(hostnameForClients);
		gemfireCacheServer.setMaxTimeBetweenPings(Long.valueOf(TimeUnit.SECONDS.toMillis(15)).intValue());
		gemfireCacheServer.setPort(port);

		return gemfireCacheServer;
	}

	@Bean(name = "Factorials")
	PartitionedRegionFactoryBean factorialsRegion(Cache gemfireCache,
			@Qualifier("factorialRegionAttributes") RegionAttributes<Long, Long> factorialRegionAttributes) {

		PartitionedRegionFactoryBean<Long, Long> factorials = new PartitionedRegionFactoryBean<>();

		factorials.setAttributes(factorialRegionAttributes);
		factorials.setCache(gemfireCache);
		factorials.setCacheLoader(factorialsCacheLoader());
		factorials.setClose(false);
		factorials.setPersistent(false);

		return factorials;
	}

	@Bean
	@SuppressWarnings("unchecked")
	RegionAttributesFactoryBean factorialRegionAttributes() {
		RegionAttributesFactoryBean factorialRegionAttributes = new RegionAttributesFactoryBean();

		factorialRegionAttributes.setKeyConstraint(Long.class);
		factorialRegionAttributes.setValueConstraint(Long.class);

		return factorialRegionAttributes;
	}

	CacheLoader<Long, Long> factorialsCacheLoader() {
		return new CacheLoader<Long, Long>() {
			@Override
			public Long load(LoaderHelper<Long, Long> helper) throws CacheLoaderException {
				Long number = helper.getKey();

				Assert.notNull(number, "Number must not be null");
				Assert.isTrue(number >= 0, String.format("Number [%d] must be greater than equal to 0", number));

				if (number <= 2L) {
					return (number < 2L ? 1L : 2L);
				}

				long result = number;

				while (number-- > 2L) {
					result *= number;
				}

				return result;
			}

			@Override
			public void close() {
			}
		};
	}
}
