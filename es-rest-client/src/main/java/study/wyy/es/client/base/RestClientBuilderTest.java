package study.wyy.es.client.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
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
    private RestClientBuilder initHost(){
        restClientBuilder = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        );
        return restClientBuilder;
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


}
