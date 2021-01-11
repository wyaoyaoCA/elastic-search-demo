package study.wyy.esclient.high.document;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/11 13:46
 */
@Slf4j
public class DocumentExistCheckTest extends BaseTest {
    /*****
     * 同步执行
     */
    @Test
    public void testSync() throws IOException {
        // 1 构建请求 （索引名，文档id）
        GetRequest request = new GetRequest("books","1");
        // 由于Exist API的返回值只有布尔值，因此建议用户关闭提取源和任何存储字段
        // 关闭提取源
        request.fetchSourceContext(new FetchSourceContext(false));
        // 禁用提取存储字段
        request.storedFields("_none_");

        // 2 执行请求
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        log.info("当前文档是否存在: {}",exists);
        client.close();
    }


    /*****
     * 异步执行
     */
    public static void testAsync() throws IOException {
        // 1 构建请求 （索引名，文档id）
        GetRequest request = new GetRequest("books","4");
        // 由于Exist API的返回值只有布尔值，因此建议用户关闭提取源和任何存储字段
        // 关闭提取源
        request.fetchSourceContext(new FetchSourceContext(false));
        // 禁用提取存储字段
        request.storedFields("_none_");

        // 2 执行请求
        client.existsAsync(request, RequestOptions.DEFAULT, new ActionListener<Boolean>() {
            @SneakyThrows
            @Override
            public void onResponse(Boolean exists) {
                log.info("当前文档是否存在: {}",exists);
                client.close();
            }

            @SneakyThrows
            @Override
            public void onFailure(Exception e) {
                log.error("执行异常",e);
                client.close();
            }
        });
    }

    public static void main(String[] args) throws IOException {
        testAsync();
    }

}
