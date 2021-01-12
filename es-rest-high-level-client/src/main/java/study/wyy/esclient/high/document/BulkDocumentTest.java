package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;
import java.util.Date;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/11 17:19
 * 文档批量操作
 */
@Slf4j
public class BulkDocumentTest extends BaseTest {

    @Test
    public void testBulkRequest() throws IOException {
        // 1 构建请求BulkRequest
        BulkRequest request = buildRequest();
        // 2 执行请求
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        log.info("response:{}",objectMapper.writer().writeValueAsString(responses));

        // 异步执行：
        /*
        client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });
         */
    }

    public BulkRequest buildRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        // 添加一个IndexRequest,
        IndexRequest indexRequest = buildIndexRequest();
        request.add(indexRequest);
        // 添加一个删除请求
        DeleteRequest deleteRequest = buildDeleteRequest();
        request.add(deleteRequest);
        // 当然还可以添加更新请求：就不做测试了
        //request.add(new UpdateRequest())
        // 其他可选配置：超时时间，刷新策略，分片副本数量，全局路由等
        // 超时时间
        request.timeout(TimeValue.timeValueMinutes(2));
        // 刷新策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        // 设置在继续执行索引/更新/删除操作之前必须处于活动状态的分片副本数量
        request.waitForActiveShards(2);
        return request;
    }

    public DeleteRequest buildDeleteRequest() {
        DeleteRequest request = new DeleteRequest("books");
        // 设置id
        request.id("1");
        return request;
    }

    public IndexRequest buildIndexRequest() throws IOException {
        IndexRequest indexRequest = new IndexRequest("books");
        // 设置id
        indexRequest.id("4");
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        xContentBuilder.field("id", 4L);
        xContentBuilder.field("title", "Python基础教程");
        xContentBuilder.field("language", "python");
        xContentBuilder.field("author", "Helant");
        xContentBuilder.field("price", 54.50f);
        xContentBuilder.field("publish_time", new Date());
        xContentBuilder.field("description", "经典的Python入门教程，层次鲜明，结构严谨，内容翔实");
        xContentBuilder.endObject();
        indexRequest.source(xContentBuilder);
        return indexRequest;
    }
}
