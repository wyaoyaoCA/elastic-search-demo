package study.wyy.es.client.base;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/8 13:49
 */
@Slf4j
public class PerformRequestTest {

    /***
     * 测试异步
     * @throws IOException
     */
    public static void main(String[] args) {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .build();
        // 构建一个请求
        Request request = new Request("GET", "/");
        restClient.performRequestAsync(request, new ResponseListener() {
            // 执行成功的回调函数
            public void onSuccess(Response response) {
                log.info("执行成功");
            }
            // 执行异常的回调函数
            public void onFailure(Exception exception) {
                log.error("执行异常:{}",exception.getMessage(),exception);
            }
        });
        // 不能在这关闭了，异步请求，可能还没有拿到结果，客户端关闭了
        //restClient.close();
    }
}
