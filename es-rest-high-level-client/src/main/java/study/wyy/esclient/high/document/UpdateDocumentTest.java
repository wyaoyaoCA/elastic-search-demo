package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;
import java.util.Map;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/11 14:52
 * 更新文档测试
 */
@Slf4j
public class UpdateDocumentTest extends BaseTest {

    /****
     * 同步执行
     */
    @Test
    public void testSync() throws IOException {
        // 1 构建请求 索引库 文档id
        UpdateRequest request = new UpdateRequest("books","1");
        // 2 配置参数
        // 2.1 设置路由
        //request.routing("hello");
        // 2.2 设置超时时间
        request.timeout(TimeValue.timeValueMinutes(2));
        // 2.3 设置刷新策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        // 2.4 设置：如果更新的文档在更新时被另一个操作更改，则重试更新的次数
        request.retryOnConflict(3);
        // 2.5 启用源检查，默认是关闭 就是restAPI中的是是否返回Source字段
        //request.fetchSource(false);
        // 设置include和exclude
                     String[] includes = new String[]{"title","language"};
        String[] exclude  = new String[]{"description"};
        request.fetchSource(includes,exclude);

        // 3 构建json文档： 前面已经介绍过了es支持三种方式：json字符串，Map，XContentBuilder
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        xContentBuilder.field("language","python");
        xContentBuilder.field("title","改个题目试试");
        xContentBuilder.endObject();
        request= request.doc(xContentBuilder);
        // 如果被更新的文档不存在，可以是使用`upsert`方法，这样如果不存在就会执行创建操作
        // request.upsert(xContentBuilder);

        // 4 执行
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        // 5 解析响应
        // 5.1 获取索引库和文档id
        String index = response.getIndex();
        String id = response.getId();
        log.info("索引库: {}; 文档id: {}",index,id);
        // 5.2 获取结果：枚举值
        DocWriteResponse.Result result = response.getResult();
        log.info("响应结果: {}",result);
        // 5.3 分片信息
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        log.info("分片信息: {}",shardInfo);
        // 5.4 版本号
        long version = response.getVersion();
        log.info("版本号: {}",version);
        GetResult getResult = response.getGetResult();
        Map<String, Object> source = getResult.getSource();
        log.info("source: {}",source);
        Map<String, DocumentField> documentFields = getResult.getDocumentFields();
        log.info("documentFields: {}",documentFields);
        Map<String, DocumentField> metadataFields = getResult.getMetadataFields();
        log.info("metadataFields: {}",metadataFields);
        client.close();

    }
}
