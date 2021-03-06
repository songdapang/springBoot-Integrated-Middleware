package com.dp.elasticsearch.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author songqinglong
 * @date 2021-06-24
 **/
@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
public class ESRestClient {

    private static final Logger log = LoggerFactory.getLogger(ESRestClient.class);

    private static final int ADDRESS_LENGTH = 2;
    private static final String HTTP_SCHEME = "http";

    @Value("${elasticsearch.host}")
    String host;
    private String user;
    private String password;

    public void setHost(String host) {
        this.host = host;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Bean(name = "restClientBuilder")
    public RestClientBuilder restClientBuilder() {
        String[] split = host.split(",");
        HttpHost[] hosts = Arrays.stream(split)
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        return RestClient.builder(hosts);
    }

    @Bean(name = "highLevelClient")
    public RestHighLevelClient highLevelClient(@Autowired RestClientBuilder restClientBuilder){
        //??????????????????
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        restClientBuilder.setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        //????????????????????????????????????
        restClientBuilder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(60000).setConnectTimeout(120000));
        //??????HTTP????????????ES????????????
        restClientBuilder.setHttpClientConfigCallback(
                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom().setIoThreadCount(10).build()));
        //??????????????????????????????????????????????????????????????????????????????
        restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                super.onFailure(node);
                log.error(node.getHost() + "--->??????????????????");
            }
        });
        return new RestHighLevelClient(restClientBuilder);
    }

    private HttpHost makeHttpHost(String str) {
        assert StringUtils.isNotEmpty(str);
        String[] address = str.split(":");
        if (address.length == ADDRESS_LENGTH) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            log.info("ES??????ip???port:{},{}", ip, port);
            return new HttpHost(ip, port, HTTP_SCHEME);
        } else {
            log.error("?????????ip??????????????????");
            return null;
        }
    }
}
