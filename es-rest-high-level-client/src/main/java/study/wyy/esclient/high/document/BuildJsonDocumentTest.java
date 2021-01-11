package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/8 16:32
 */
@Slf4j
public class BuildJsonDocumentTest extends BaseTest {

    /****
     * 直接使用字符串形式，测试同步执行，和响应结果的解析
     */
    @Test
    public void testString() throws IOException {
        // 索引的名字
        IndexRequest request = new IndexRequest("books");
        // 设置文档id, 也可以不设置es会自动生成一个
        request.id("1");
        // String 类型文档
        String json = "{\n" +
                "    \"id\":\"1\",\n" +
                "    \"title\":\"Java编程思想\",\n" +
                "    \"language\":\"java\",\n" +
                "    \"author\":\"Bruce Eckel\",\n" +
                "    \"price\":70.2,\n" +
                "    \"publish_time\":\"2007-10-01\",\n" +
                "    \"description\":\"Java学习必读经典,殿堂级著作！赢得了全球程序员的广泛赞誉。\"\n" +
                "}";
        request.source(json, XContentType.JSON);
        // 设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1L));
        // request.timeout("1s");
        // 设置超时策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        // 设置版本
        //request.version(2);
        // 设置版本类型
        //request.versionType(VersionType.EXTERNAL);
        // 设置操作类型
        //request.opType("create");
        request.opType(DocWriteRequest.OpType.CREATE);

        // 同步执行执行请求
        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            // 响应状态
            RestStatus status = response.status();
            log.info("响应状态: {}",status);
            // 获取文档id
            String id = response.getId();
            // 获取文档索引
            String index = response.getIndex();
            log.info("文档索引库: {}; 文档id: {}",index,id);
            // 获取结果: 枚举值：创建，删除，更新，not find
            // 对于此处来说，只能是创建或者是更新，es当id存在的时候就是执行更新，id不存在就是创建，和restAPI是一样的
            DocWriteResponse.Result result = response.getResult();
            log.info("响应结果: {}",result);
            // 获取分片信息
            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            log.info("分片信息: {}",shardInfo);
            // 获取文档版本
            long version = response.getVersion();
            log.info("文档版本号: {}",version);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            client.close();
        }

    }

    /****
     * 使用Map构建，并测试异步执行
     *
     */
    public static void testMap(){
        // 索引的名字
        IndexRequest request = new IndexRequest("books");
        // 设置文档id, 也可以不设置es会自动生成一个
        request.id("2");
        Map<String,Object> mappings = new HashMap<>();
        mappings.put("id",2L);
        mappings.put("title","Java程序性能优化");
        mappings.put("language","java");
        mappings.put("author","葛一鸣");
        mappings.put("price",45.60f);
        mappings.put("publish_time",new Date());
        mappings.put("description","让你的Java程序更快、更稳定。深入剖析软件设计层面、代码层面、JVM虚拟机层面的优化方法");
        request.source(mappings);

        // 异步执行
        client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            // 执行成功的回调
            @Override
            public void onResponse(IndexResponse indexResponse) {
                log.info("成功");

            }
            // 执行失败的回调
            @Override
            public void onFailure(Exception e) {
                log.info("失败");
            }
        });

    }

    /****
     * 使用XContentBuilder构建
     *
     */
    @Test
    public void testXContentBuilder() throws IOException {
        // 索引的名字
        IndexRequest request = new IndexRequest("books");
        // 设置文档id, 也可以不设置es会自动生成一个
        request.id("3");
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        // startObject 构建对象，可以理解为json中的{
        // startArray：构建数组，可以理解为json中的[
        xContentBuilder.startObject();
        xContentBuilder.field("id",3L);
        xContentBuilder.field("title","Python科学计算");
        xContentBuilder.field("language","python");
        xContentBuilder.field("author","张若愚");
        xContentBuilder.field("price",81.40f);
        xContentBuilder.field("publish_time",new Date());
        xContentBuilder.field("description","零基础学python,光盘中作者独家整合开发winPython运行环境，涵盖了Python各个扩展库");
        // endObject 可以理解为json中的}
        xContentBuilder.endObject();
        request.source(xContentBuilder);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        client.close();
    }

    public static void main(String[] args) {
        testMap();
    }

}
