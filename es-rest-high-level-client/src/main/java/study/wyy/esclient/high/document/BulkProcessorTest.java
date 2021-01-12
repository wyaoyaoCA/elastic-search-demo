package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/12 9:50
 * 批量请求处理器测试
 */
@Slf4j
public class BulkProcessorTest extends BulkDocumentTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        new BulkProcessorTest().testBulkProcessor();
    }


    public void testBulkProcessor() throws IOException, InterruptedException {
        // 需要两个参数：BiConsumer，BulkProcessor.Listener
        BulkProcessor bulkProcessor = BulkProcessor.builder(new BiConsumer<BulkRequest, ActionListener<BulkResponse>>() {
            @Override
            public void accept(BulkRequest bulkRequest, ActionListener<BulkResponse> bulkResponseActionListener) {
                // 执行请求
                client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener);
            }
        }, new BulkProcessor.Listener() {
            // 执行请求之前的前置处理
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // request: 肯定就是封装的请求参数
                // 打印一下这个id
                System.out.println("前置处理: " + executionId);
            }

            // 后置处理
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("后置处理: " + executionId);
            }

            // 异常处理
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("异常处理: " + executionId);
            }
        }).build();
        // 添加一个indexRequest
        bulkProcessor.add(buildIndexRequest());
        // 添加一个DeleteRequest
        bulkProcessor.add(buildDeleteRequest());
        bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);

    }

    public void testBulkProcessorWithLambda() throws IOException, InterruptedException {
        // 需要两个参数：BiConsumer，BulkProcessor.Listener
        BulkProcessor bulkProcessor = BulkProcessor.builder((request,listener)->{
            client.bulkAsync(request,RequestOptions.DEFAULT,listener);
        }, new BulkProcessor.Listener() {
            // 执行请求之前的前置处理
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // request: 肯定就是封装的请求参数
                // 打印一下这个id
                System.out.println("前置处理: " + executionId);
            }

            // 后置处理
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("后置处理: " + executionId);
            }

            // 异常处理
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("异常处理: " + executionId);
            }
        }).build();
        // 添加一个indexRequest
        bulkProcessor.add(buildIndexRequest());
        // 添加一个DeleteRequest
        bulkProcessor.add(buildDeleteRequest());
        bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);

    }

}
