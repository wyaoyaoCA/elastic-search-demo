package study.wyy.esclient.high.index;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import javax.lang.model.element.VariableElement;
import java.io.IOException;
import java.util.List;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/12 14:04
 */
@Slf4j
public class ReIndexTest extends BaseTest {

    @Test
    public void test() throws IOException {
        // 1 构建ReIndexRequest
        ReindexRequest request = buildReindexRequest();
        // 2 执行
        try {
            BulkByScrollResponse response = client.reindex(request, RequestOptions.DEFAULT);
            // 3 解析
            // 获取总耗时
            TimeValue took = response.getTook();
            log.info("总耗时: {}", took.getMillis());
            // 请求是否超时
            boolean timedOut = response.isTimedOut();
            log.info("请求是否超时: {}", timedOut);
            // 获取已经处理的文档数量
            long total = response.getTotal();
            log.info("处理的文档数量: {}", total);
            // 获取更新的文档数量
            long updated = response.getUpdated();
            log.info("更新的文档数量: {}", updated);
            // 获取创建的文档数量
            long created = response.getCreated();
            log.info("创建的文档数量: {}", created);
            // 获取删除的文档数量
            long deleted = response.getDeleted();
            log.info("删除的文档数量: {}", deleted);
            // 获取执行的批次
            int batches = response.getBatches();
            log.info("执行的批次数量: {}", batches);
            // 获取跳过的文档数量
            long noops = response.getNoops();
            log.info("跳过的文档数量: {}", noops);
            // 获取版本冲突数量
            long versionConflicts = response.getVersionConflicts();
            log.info("版本冲突数量: {}", versionConflicts);
            // 重试批量索引的次数
            long bulkRetries = response.getBulkRetries();
            log.info("重试批量索引的次数: {}", bulkRetries);
            // 重试搜索操作的次数
            long searchRetries = response.getSearchRetries();
            log.info("重试搜索操作的次数: {}", searchRetries);
            // 请求阻塞的总时间，不包括当前处于休眠状态的限制时间
            TimeValue throttled = response.getStatus().getThrottled();
            log.info("请求阻塞的总时间，不包括当前处于休眠状态的限制时间: {}", throttled.getMillis());
            // 获取查询失败
            List<ScrollableHitSource.SearchFailure> searchFailures = response.getSearchFailures();
            log.info("查询失败的数量: {}", searchFailures != null ? searchFailures.size() : 0);
            // 获取批量操作失败
            List<BulkItemResponse.Failure> bulkFailures = response.getBulkFailures();
            log.info("批量操作失败的数量: {}", bulkFailures != null ? bulkFailures.size() : 0);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }


    public ReindexRequest buildReindexRequest() {
        // 1 构建ReIndexRequest
        ReindexRequest request = new ReindexRequest();
        // 1.1 设置源索引，接收的是一个可变参数，可设置多个索引
        request.setSourceIndices("books");
        // 1.2 设置目标索引: 注意先去将该索引的映射，分片等信息设置好
        request.setDestIndex("books_copy");
        // 1.3 其他可选参数
        // 设置目标索引的版本类型
        // request.setDestVersionType(VersionType.EXTERNAL);
        // 设置目标索引的操作类型
        // request.setDestOpType("create");
        // 默认情况下，版本冲突会中止重新索引进程
        // request.setConflicts("proceed");
        // 通过添加查询限制文档，比如这里就是只对language字段词条是包括java的进行操作
        // 简单了来说就是进行文档的过滤
        request.setSourceQuery(new TermQueryBuilder("language", "java"));
        // 默认情况下是1000
        // request.setSourceBatchSize(100);
        // 设置超时时间
        request.setTimeout(TimeValue.timeValueMinutes(10));
        // reIndex之后刷新索引
        request.setRefresh(true);
        return request;
    }
}
