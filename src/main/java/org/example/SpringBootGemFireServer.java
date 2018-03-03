package org.example;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.RegionAttributes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.gemfire.PartitionedRegionFactoryBean;
import org.springframework.data.gemfire.RegionAttributesFactoryBean;
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
@CacheServerApplication(name = "SpringBootGemFireServer")
@EnableLocator
@EnableManager
@SuppressWarnings("all")
public class SpringBootGemFireServer {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootGemFireServer.class);
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
