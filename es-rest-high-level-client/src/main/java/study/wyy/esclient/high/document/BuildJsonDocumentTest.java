package study.wyy.esclient.high.document;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wyaoyao
 * @description
 * @date 2021/1/8 16:32
 */
public class BuildJsonDocumentTest {

    /****
     * 直接使用字符串形式
     */
    @Test
    public void testString(){
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


    }

    /****
     * 使用Map构建
     *
     */
    @Test
    public void testMap(){
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
    }

    /****
     * 使用Map构建
     *
     */
    @Test
    public void testMap2() throws IOException {
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

    }

}
