package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/11 14:19
 * 删除文档测试
 */
@Slf4j
public class DeleteDocumentTest extends BaseTest {

    /***
     * 同步执行： 测试删除存在的文档
     */
    @Test
    public void testSync() throws IOException {
        // 1 构建请求参数 索引库和文档id
        DeleteRequest request = new DeleteRequest("books","1");

        // 2 设置参数
        // 2.1 设置路由
        //request.routing("hello");
        // 2.2 设置超时时间 2分钟 下面两个都可以
        //request.timeout("2m");
        request.timeout(TimeValue.timeValueMinutes(2L));
        // 2.3 设置刷新策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        // 2.4 设置版本号
        // request.version(1L);
        // 2.5 设置版本类型
        //request.versionType(VersionType.EXTERNAL);

        // 3 执行请求
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);

        // 4 解析响应
        // 4.1 获取索引库和文档id
        String index = response.getIndex();
        String id = response.getId();
        log.info("索引库: {}; 文档id: {}",index,id);
        // 4.2 获取结果：枚举值
        DocWriteResponse.Result result = response.getResult();
        log.info("响应结果: {}",result);
        // 4.3 分片信息
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        log.info("分片信息: {}",shardInfo);
        // 4.4 版本号
        long version = response.getVersion();
        log.info("版本号: {}",version);

        // 5 关闭客户端
        client.close();

    }

    /****
     * 测试异步和不存在的文档
     */
    public static void testAsync(){
        // 1 构建请求参数 索引库和文档id
        DeleteRequest request = new DeleteRequest("books","9999");
        client.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse response) {
                // 4 解析响应
                // 4.1 获取索引库和文档id
                String index = response.getIndex();
                String id = response.getId();
                log.info("索引库: {}; 文档id: {}",index,id);
                // 4.2 获取结果：枚举值
                DocWriteResponse.Result result = response.getResult();
                log.info("响应结果: {}",result);
                // 4.3 分片信息
                ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
                log.info("分片信息: {}",shardInfo);
                // 4.4 版本号
                long version = response.getVersion();
                log.info("版本号: {}",version);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("发送异常",e);
            }
        });
    }

    public static void main(String[] args) {
        testAsync();
    }
}
