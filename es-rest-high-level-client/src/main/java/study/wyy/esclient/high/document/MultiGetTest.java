package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/12 11:53
 */
@Slf4j
public class MultiGetTest extends BaseTest {

    @Test
    public void testMultiGetSync() throws IOException {
        // 1 构建请求
        MultiGetRequest request = new MultiGetRequest();
        String[] ids = new String[]{"1", "2", "3"};
        // 遍历id，添加到request
        for (int i = 0; i < ids.length; i++){
            // 参参数一: 索引库名字， 参数二: 文档id
            request.add(new MultiGetRequest.Item("books",ids[i]));
            // 每个MultiGetRequest.Item，也可以设置source过滤，路由，版本等参数
            /*
            new MultiGetRequest.Item("books",ids[i])
                    .version(1)
                    .routing("hello");

             */
        }
        // 可选参数
        //关闭实时性，默认是开启
        request.realtime(false);
        // 查询前，是否执行刷新，默认是false
        request.refresh(true);

        // 2 执行请求
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        // 3 响应解析
        MultiGetItemResponse[] responses = response.getResponses();

        for (var res :responses) {
            String index = res.getIndex();
            String id = res.getId();
            GetResponse getResponse = res.getResponse();
            String s = objectMapper.writer().writeValueAsString(getResponse.getSourceAsMap());
            System.out.println(s);
        }
    }
}
