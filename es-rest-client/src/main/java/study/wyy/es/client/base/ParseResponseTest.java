package study.wyy.es.client.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
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
 * @date 2021/1/8 14:29
 */
@Slf4j
public class ParseResponseTest {
    RestClient restClient;

    @Before
    public void buildRestClient(){
        restClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .build();
    }


    @Test
    public void parseResponseTest() throws IOException {
        // 构建一个请求
        Request request = new Request("GET", "books/_doc/1");
        Response response = restClient.performRequest(request);
        // 拿到请求信息
        RequestLine requestLine = response.getRequestLine();
        log.info("请求信息: {}",requestLine);
        // 拿到host地址
        HttpHost host = response.getHost();
        log.info("es地址：{}",host);
        // 响应头
        Header[] headers = response.getHeaders();
        log.info("响应头：{}",headers);
        String contentType = response.getHeader("content-type");
        log.info("contentType：{}",contentType);
        // 状态码
        int statusCode = response.getStatusLine().getStatusCode();
        log.info("状态码：{}",statusCode);
        // 响应体
        String body = EntityUtils.toString(response.getEntity());
        log.info("响应体：{}",body);

    }

}
