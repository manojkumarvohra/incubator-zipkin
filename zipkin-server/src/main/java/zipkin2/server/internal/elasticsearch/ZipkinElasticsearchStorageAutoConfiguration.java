/*
 * Copyright 2015-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.server.internal.elasticsearch;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import brave.http.HttpTracing;
import brave.okhttp3.TracingInterceptor;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import okhttp3.Dispatcher;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.logging.HttpLoggingInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import zipkin2.elasticsearch.ElasticsearchStorage;
import zipkin2.elasticsearch.ElasticsearchStorage.HostsSupplier;
import zipkin2.server.internal.ConditionalOnSelfTracing;
import zipkin2.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinElasticsearchStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "elasticsearch")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinElasticsearchStorageAutoConfiguration {
  // allows extensions like zipkin-storage-elasticsearch-aws to intercept requests
  @Autowired(required = false) List<Interceptor> networkInterceptors = Collections.emptyList();
  // allows extensions like zipkin-storage-elasticsearch-aws to control host resolution
  @Autowired(required = false) HostsSupplier hostsSupplier;

  // allows arbitrary modifications to the http client
  @Bean OkHttpClient.Builder okHttpClientBuilder() {
    return new OkHttpClient.Builder();
  }

  @Bean OkHttpClient okHttpClient(
    OkHttpClient.Builder okHttpClientBuilder,
    @Value("${zipkin.storage.elasticsearch.timeout:10000}") int timeout) {
    for (Interceptor interceptor : networkInterceptors) {
      okHttpClientBuilder.addNetworkInterceptor(interceptor);
    }
    return okHttpClientBuilder.connectTimeout(timeout, TimeUnit.MILLISECONDS)
      .readTimeout(timeout, TimeUnit.MILLISECONDS)
      .writeTimeout(timeout, TimeUnit.MILLISECONDS).build();
  }

  @Bean @Conditional(HttpLoggingSet.class)
  Interceptor loggingInterceptor(ZipkinElasticsearchStorageProperties es) {
    Logger logger = Logger.getLogger(ElasticsearchStorage.class.getName());
    return new HttpLoggingInterceptor(logger::info).setLevel(es.getHttpLogging());
  }

  @Bean @Conditional(BasicAuthRequired.class)
  Interceptor basicAuthInterceptor(ZipkinElasticsearchStorageProperties es) {
    return new BasicAuthInterceptor(es);
  }

  @Bean @ConditionalOnMissingBean StorageComponent storage(
    ZipkinElasticsearchStorageProperties elasticsearch,
    OkHttpClient okHttpClient,
    @Value("${zipkin.query.lookback:86400000}") int namesLookback,
    @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
    @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
    @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys,
    @Value("${zipkin.storage.autocomplete-ttl:3600000}") int autocompleteTtl,
    @Value("${zipkin.storage.autocomplete-cardinality:20000}") int autocompleteCardinality) {
    ElasticsearchStorage.Builder result = elasticsearch
      .toBuilder(okHttpClient)
      .namesLookback(namesLookback)
      .strictTraceId(strictTraceId)
      .searchEnabled(searchEnabled)
      .autocompleteKeys(autocompleteKeys)
      .autocompleteTtl(autocompleteTtl)
      .autocompleteCardinality(autocompleteCardinality);
    if (hostsSupplier != null) result.hostsSupplier(hostsSupplier);
    return result.build();
  }

  // our elasticsearch impl uses an instance of OkHttpClient, not Call.Factory, so we have to trace
  // carefully the pieces inside OkHttpClient
  @Configuration
  @ConditionalOnSelfTracing
  static class TracingOkHttpClientBuilderEnhancer implements BeanPostProcessor {

    @Autowired(required = false) HttpTracing httpTracing;
    @Autowired(required = false) Tracing tracing;

    @Override public Object postProcessBeforeInitialization(Object bean, String beanName) {
      return bean;
    }

    @Override public Object postProcessAfterInitialization(Object bean, String beanName) {
      if (httpTracing == null) return bean;
      if (!(bean instanceof OkHttpClient.Builder)) return bean;
      Tracer tracer = tracing.tracer();

      OkHttpClient.Builder builder = (OkHttpClient.Builder) bean;
      builder.addInterceptor(new Interceptor() {
        /** create a local span with the same name as the request tag */
        @Override public Response intercept(Chain chain) throws IOException {
          // don't start new traces (to prevent amplifying writes to local storage)
          if (tracer.currentSpan() == null) return chain.proceed(chain.request());

          Request request = chain.request();
          ScopedSpan span = tracer.startScopedSpan(request.tag().toString());
          try {
            return chain.proceed(request);
          } catch (RuntimeException | IOException | Error e) {
            span.error(e);
            throw e;
          } finally {
            span.finish();
          }
        }
      });
      builder.addNetworkInterceptor(
        TracingInterceptor.create(httpTracing.clientOf("elasticsearch")));
      ExecutorService executor = new Dispatcher().executorService();
      builder.dispatcher(new Dispatcher(tracing.currentTraceContext().executorService(executor)));
      return builder;
    }
  }

  static final class HttpLoggingSet implements Condition {
    @Override public boolean matches(ConditionContext condition, AnnotatedTypeMetadata ignored) {
      return !isEmpty(
        condition.getEnvironment().getProperty("zipkin.storage.elasticsearch.http-logging"));
    }
  }

  static final class BasicAuthRequired implements Condition {
    @Override public boolean matches(ConditionContext condition, AnnotatedTypeMetadata ignored) {
      String userName =
        condition.getEnvironment().getProperty("zipkin.storage.elasticsearch.username");
      String password =
        condition.getEnvironment().getProperty("zipkin.storage.elasticsearch.password");
      return !isEmpty(userName) && !isEmpty(password);
    }
  }

  private static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }
}
