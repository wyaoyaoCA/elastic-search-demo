package study.wyy.esclient.high.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/8 15:19
 */
public class BuildClientTest {

    @Test
    public void buildHighClient() throws IOException {
        // 可见高级客户端的构建也是基于低级客户端构建，可以大胆猜测高级客户端就是对低级客户端的包装
        // 对外暴露的API对使用者来说更加友好
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        restHighLevelClient.close();
    }
}
