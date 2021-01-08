package study.wyy.es.client.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author by wyaoyao
 * @Description
 * @Date 2021/1/3 5:20 下午
 */
@Slf4j
public class RestClientBuilderTest {

    RestClientBuilder restClientBuilder;

    @Before
    public void initHost(){
        restClientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        );

    }
    @Test
    public void hello(){
        log.info("hello world");
    }

    /**
     *  构造简单的es RestClient
     */
    @Test
    public void testBuild() throws IOException {
        // 1 构造RestClient
        RestClient client = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ).build();
        // 2 关闭
        client.close();
    }

    /**
     *  配置请求头
     */
    public void testConfigHead() throws IOException {
        Header[] hears = new Header[]{new BasicHeader("key","value")} ;
        // 接受的是一个Header数组
        RestClient client = restClientBuilder.setDefaultHeaders(hears).build();
        client.close();
    }

    /****
     * 配置监听器
     */
    @Test
    public void testConfigListener() throws IOException {
        // 设置一个监听器，该监听器在每次节点失败时都会收到通知，在启用嗅探时在内部使用。
        RestClient client = restClientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                super.onFailure(node);
            }
        }).build();
        client.close();
    }

    /****
     * 设置节点选择器
     */
    @Test
    public void testConfigNodeSelector() throws IOException {
        RestClient client = restClientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS)
                .build();
        client.close();
    }

    /****
     * 配置超时时间
     */
    @Test
    public void testConfigTimeout() throws IOException {
        RestClient client = restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                // 设置套接字超时时间
                requestConfigBuilder.setSocketTimeout(10000);
                // 链接超时时间
                requestConfigBuilder.setConnectTimeout(5000);
                return requestConfigBuilder;
            }
        }).build();
        client.close();
    }

    /****
     * 配置线程数量
     * @throws IOException
     */
    @Test
    public void testConfigThread() throws IOException {
        RestClient client= restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
               httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(2).build());
                return httpClientBuilder;
            }
        }).build();
        client.close();
    }
}
