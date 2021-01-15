package study.wyy.esclient.high.search;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import javax.sound.midi.Soundbank;
import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/14 16:03
 */
@Slf4j
public class MultiSearchTest extends BaseTest {

    @Test
    public void testSync() throws IOException {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        // 第一个SearchRequest
        SearchRequest searchRequest1 = new SearchRequest();
        searchRequest1.indices("books");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("language","python"));
        searchRequest1.source(searchSourceBuilder);

        // 第二个SearchRequest
        SearchRequest searchRequest2 = new SearchRequest();
        searchRequest2.indices("books");
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2.query(QueryBuilders.matchQuery("language","java"));
        searchRequest2.source(searchSourceBuilder2);

        // 将两个SearchRequest添加到MultiSearchRequest
        multiSearchRequest.add(searchRequest1);
        multiSearchRequest.add(searchRequest2);

        MultiSearchResponse response = client.msearch(multiSearchRequest, RequestOptions.DEFAULT);
        MultiSearchResponse.Item[] responses = response.getResponses();
        log.info("数量: {}",responses.length);
        if(responses != null && responses.length>0){
            for (var item :responses) {
                // 获得之前的SearchResponse对象
                SearchResponse response1 = item.getResponse();
                log.info("response: {}",objectMapper.writer().writeValueAsString(response1));
            }
        }

    }
}
