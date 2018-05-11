package org.example;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;
import org.springframework.data.gemfire.config.annotation.CacheServerApplication;
import org.springframework.data.gemfire.config.annotation.EnableLocator;
import org.springframework.data.gemfire.config.annotation.EnableManager;
import org.springframework.util.Assert;

/**
 * A Spring Boot application bootstrapping a Pivotal GemFire Server in the JVM process.
 *
 * @author John Blum
 * @see org.springframework.boot.SpringApplication
 * @see org.springframework.boot.autoconfigure.SpringBootApplication
 * @see org.springframework.context.annotation.Bean
 * @see org.apache.geode.cache.Cache
 * @see org.apache.geode.cache.CacheLoader
 * @see org.apache.geode.cache.server.CacheServer
 * @since 1.0.0
 */
@SpringBootApplication
@CacheServerApplication(name = "SpringBootGemFireServer", locators = "localhost[10334]")
@ImportResource("spring-gemfire-server-cache.xml")
@SuppressWarnings("all")
public class SpringBootGemFireServer {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootGemFireServer.class);
	}

	@Configuration
	@EnableLocator
	@EnableManager(start = true)
	@Profile("locator-manager")
	static class LocatorManagerConfiguration { }

	public static CacheLoader<Long, Long> factorialsCacheLoader() {

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
