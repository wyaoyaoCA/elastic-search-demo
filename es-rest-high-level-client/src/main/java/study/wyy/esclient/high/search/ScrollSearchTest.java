package study.wyy.esclient.high.search;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/13 16:58
 * 滚动搜索测试
 */
@Slf4j
public class ScrollSearchTest extends BaseTest {

    @Test
    public void testScrollSearch() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("books");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 设置一次检索多少结果
        searchSourceBuilder.size(1);
        request.source(searchSourceBuilder);
        // 设置滚动间隔
        request.scroll(TimeValue.timeValueMinutes(1));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 读取返回的滚动id 该id指向保持活动状态的搜索上下文，并在后续搜索滚动调用中被需要
        String scrollId = response.getScrollId();
        // 检索第一批结果
        SearchHits hits = response.getHits();
        log.info("第1批结果: {}",objectMapper.writer().writeValueAsString(hits));
        int i = 2;
        while (hits !=null && hits.getHits().length >0){
            log.info("当前的scrollId: {}",scrollId);
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(30));

            SearchResponse scrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            // 读取新的滚动id，该id指向保持活动状态的搜索上下文，并在后续搜索滚动调用被需要
            scrollId = scrollResponse.getScrollId();
            // 获取另一批结果
            hits= scrollResponse.getHits();
            log.info("第{}批结果: {}",i,objectMapper.writer().writeValueAsString(hits));
            i++;
        }
        // 清除滚动搜索的上下文
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        // 将滚动id添加进去
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        log.info("是否清除成功: {}", succeeded);
        client.close();
    }




}
