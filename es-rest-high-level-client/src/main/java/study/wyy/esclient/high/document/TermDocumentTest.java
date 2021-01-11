package study.wyy.esclient.high.document;

import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.junit.Test;
import study.wyy.esclient.high.BaseTest;

import java.io.IOException;
import java.util.List;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/11 16:07
 * 获取文档索引的词向量
 */
@Slf4j
public class TermDocumentTest extends BaseTest {

    @Test
    public void testSync() throws IOException {
        // 1 构建请求对象指定索引库和文帝id
        TermVectorsRequest request = new TermVectorsRequest("books","1");
        // 设置字段
        request.setFields(new String[]{"description"});

        // 2 执行
        TermVectorsResponse response = client.termvectors(request, RequestOptions.DEFAULT);

        // 3 解析响应
        String index = response.getIndex();
        String id = response.getId();
        log.info("索引库: {}; 文档id: {}",index,id);
        // 是否找到文档
        boolean found = response.getFound();
        log.info("是否找到文档: {};",found);
        long docVersion = response.getDocVersion();
        log.info("文档版本: {}",docVersion);
        List<TermVectorsResponse.TermVector> termVectorsList = response.getTermVectorsList();
        client.close();
    }

    public static void testAsync(){
        // 1 构建请求对象指定索引库和文帝id
        TermVectorsRequest request = new TermVectorsRequest("books","1");
        // 设置字段
        request.setFields(new String[]{"description"});

        // 2 执行
        client.termvectorsAsync(request, RequestOptions.DEFAULT, new ActionListener<TermVectorsResponse>() {
            @Override
            public void onResponse(TermVectorsResponse termVectorsResponse) {
                // 成功的回调
            }

            @Override
            public void onFailure(Exception e) {
                // 异常的回调
            }
        });

    }


}
