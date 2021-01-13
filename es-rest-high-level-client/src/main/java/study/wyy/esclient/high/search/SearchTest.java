package study.wyy.esclient.high.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/13 14:17
 */
@Slf4j
public class SearchTest extends BaseTest {

    @Test
    public void test() throws IOException {
        // 1 构建SearchRequest
        SearchRequest request = buildSearchRequest();
        // 2 执行请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        log.info(new ObjectMapper().writer().writeValueAsString(response));

    }

    private SearchRequest buildSearchRequest() {
        SearchRequest request = new SearchRequest();
        // 设置要搜索的索引
        request.indices("books");
        // 添加搜索条件,大多数搜索参数都是通过SearchSourceBuilder构建
        // 构建SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设置一些基本参数，超时时间，分页信息（from size）等
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(10));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        // 构建查询参数
        QueryBuilder queryBuilder = buildQueryParam();
        searchSourceBuilder.query(queryBuilder);

        // 设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // title字段高亮
        HighlightBuilder.Field title = new HighlightBuilder.Field("title");
        highlightBuilder.field(title);
        searchSourceBuilder.highlighter(highlightBuilder);

        // 设置Suggest
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        TermSuggestionBuilder text = SuggestBuilders.termSuggestion("description").text("python");
        suggestBuilder.addSuggestion("my_suggest",text);
        searchSourceBuilder.suggest(suggestBuilder);

        // source过滤
        String[] excludes = {"id","publish_time"};
        searchSourceBuilder.fetchSource(null,excludes);

        // 排序
        FieldSortBuilder sortBuilder = new FieldSortBuilder("price").order(SortOrder.DESC);
        searchSourceBuilder.sort(sortBuilder);

        // 最后不要忘记将searchSourceBuilder添加到SearchRequest
        request.source(searchSourceBuilder);
        return request;
    }

    private QueryBuilder buildQueryParam() {
        QueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                // 过滤
                .filter(QueryBuilders.termQuery("author", "张若愚"))
                // 模糊查询
                .must(QueryBuilders.fuzzyQuery("title", "python"))
                // 前缀查询
                .mustNot(QueryBuilders.prefixQuery("language", "j"))
                // 范围查询
                .should(QueryBuilders.rangeQuery("price").gte(0).lte(100));
        return boolQueryBuilder;
    }
}
