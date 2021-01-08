package study.wyy.es.client.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/8 10:47
 */
@Slf4j
public class BuildRequestTest {

    RestClient restClient;

    @Before
    public void buildRestClient(){
        restClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .build();
    }

    /***
     * 构建请求对象
     */
    @Test
    public void testBuildRequest() throws IOException {
        // 构建一个请求
        Request request = new Request("GET", "/");
        Response response = restClient.performRequest(request);
        log.info("response: {}", EntityUtils.toString(response.getEntity()));
        restClient.close();
    }

    @Test
    public void testBuildRequest2() throws IOException {
        // 构建一个请求
        Request request = new Request("GET", "_cat/indices");
        Response response = restClient.performRequest(request);
        log.info("response: {}", EntityUtils.toString(response.getEntity()));
        restClient.close();
    }

    @Test
    public void testBuildRequest3() throws IOException {
        // 构建一个请求
        Request request = new Request("GET", "books/_doc/1");
        Response response = restClient.performRequest(request);
        log.info("response: {}", EntityUtils.toString(response.getEntity()));
        restClient.close();
    }


}
