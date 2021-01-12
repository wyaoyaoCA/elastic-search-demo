package study.wyy.esclient.high;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/8 15:30
 */
public abstract class BaseTest {

    public static RestHighLevelClient client;
    public static ObjectMapper objectMapper;

    static {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        objectMapper = new ObjectMapper();
    }
}
