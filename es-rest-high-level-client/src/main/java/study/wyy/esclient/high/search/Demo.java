package study.wyy.esclient.high.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/12 16:58
 * 入门程序
 */
@Slf4j
public class Demo extends BaseTest {
    @Test
    public void testSync() throws IOException {
        // 1 构建搜索请求 SearchRequest
        SearchRequest request = buildSearchRequest();
        // 2 执行请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取hits
        SearchHits hits = response.getHits();
        // 获取搜索结果的数量
        long totalHits = hits.getTotalHits().value;
        // 获取最高的得分
        float maxScore = hits.getMaxScore();
        // 获取搜索结果的相关性数据
        TotalHits.Relation relation = hits.getTotalHits().relation;
        // 搜索结果
        SearchHit[] result = hits.getHits();
        // 日志输出，这里我给转成json了
        log.info(new ObjectMapper().writer().writeValueAsString(response));
        //log.info(new ObjectMapper().writer().writeValueAsString(hits));
        //log.info(new ObjectMapper().writer().writeValueAsString(result));
    }



    public SearchRequest buildSearchRequest(){
        // 1 构建搜索请求 SearchRequest
        SearchRequest request = new SearchRequest();
        // 1.1 指定索引库，可变参数可指定多个，也可以不指定，那就是全部索引都搜索
        request.indices("books");
        // 1.2 大部分搜索参数都可以通过SearchSourceBuilder构建
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 1.3 设置查询条件: 将＂全部匹配（match_all）＂添加到查询中
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 1.4 将searchSourceBuilder添加到SearchRequest中
        request.source(searchSourceBuilder);
        return request;
    }
}
