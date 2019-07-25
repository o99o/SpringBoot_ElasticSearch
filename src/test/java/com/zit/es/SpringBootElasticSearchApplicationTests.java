package com.zit.es;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class SpringBootElasticSearchApplicationTests {
	
	@Autowired
	RestHighLevelClient highLevelClient;
	
	@Autowired
	RestClient restClient;

	// 创建索引库
	/*@Test
	public void testCreateIndex() throws IOException{
		// 创建索引请求对象，并设置索引名称
		CreateIndexRequest createIndexRequest = new CreateIndexRequest("es_index");
		// 设置索引参数
		createIndexRequest.settings(Settings.builder().put("number_of_shards",1).
				put("number_of_replicas",0));
		// 设置映射
		createIndexRequest.mapping("{\n" +
	                " \t\"properties\": {\n" +
	                "           \"name\": {\n" +
	                "              \"type\": \"text\",\n" +
	                "              \"analyzer\":\"ik_max_word\",\n" +
	                "              \"search_analyzer\":\"ik_smart\"\n" +
	                "           },\n" +
	                "           \"description\": {\n" +
	                "              \"type\": \"text\",\n" +
	                "              \"analyzer\":\"ik_max_word\",\n" +
	                "              \"search_analyzer\":\"ik_smart\"\n" +
	                "           },\n" +
	                "           \"studymodel\": {\n" +
	                "              \"type\": \"keyword\"\n" +
	                "           },\n" +
	                "           \"price\": {\n" +
	                "              \"type\": \"float\"\n" +
	                "           }\n" +
	                "        }\n" +
	                "}", XContentType.JSON);
		 // 创建索引操作客户端
		 IndicesClient indices = highLevelClient.indices();
		 // 创建响应对象
		 CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
		 // 得到响应结果
		 boolean acknowledged = createIndexResponse.isAcknowledged();
		 System.out.println(acknowledged);
	}*/
	
	// 删除索引库
	/*
	 * @Test public void testDeleteIndex() throws IOException{ // 删除索引请求对象
	 * DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("es_index");
	 * /// 删除索引 // 创建索引操作客户端 IndicesClient indices = highLevelClient.indices();
	 * AcknowledgedResponse acknowledgedResponse =
	 * indices.delete(deleteIndexRequest, RequestOptions.DEFAULT); // 得到响应结果 boolean
	 * acknowledged = acknowledgedResponse.isAcknowledged();
	 * System.out.println(acknowledged); }
	 */
	
	// 添加文档
	/*@Test
	public void testAddDoc() throws IOException{
		// 准备数据
		Map<String, Object> jsonMap = new HashMap<>();
		jsonMap.put("name", "Spring Cloud");
		jsonMap.put("description", "注册中心Nacos");
		jsonMap.put("studymodel", "20190725");
		jsonMap.put("price", 5.6f);
		// 索引请求对象
		IndexRequest indexRequest = new IndexRequest("es_index");
		// 指定索引文档内容
		indexRequest.source(jsonMap);
		// 索引响应对象
		IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
		// 获取响应结果
		DocWriteResponse.Result result = indexResponse.getResult();
		System.out.println(result);
	}*/
	
	// 查询文档
	/*@Test
	public void getDoc() throws IOException{
		GetRequest getRequest = new GetRequest("es_index","_doc","ruZCJ2wB7dcg4IHXgfGu");
		GetResponse getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT);
		boolean exists = getResponse.isExists();
		if (exists) {
			Map<String, Object> sourceAsMap =  getResponse.getSourceAsMap();
			System.out.println(sourceAsMap);
		}else {
			System.out.println("未找到。");
		}
	}*/
	
	// 更新文档
	/*@Test
	public void updateDoc() throws IOException{
		UpdateRequest updateRequest = 
				new UpdateRequest("es_index","_doc","ruZCJ2wB7dcg4IHXgfGu");
		Map<String, Object> map = new HashMap<>();
		map.put("name", "Spring Cloud 实战");
		updateRequest.doc(map);
		UpdateResponse update = highLevelClient.update(updateRequest, RequestOptions.DEFAULT);
		RestStatus status = update.status();
		System.out.println(status);
	}*/
	
	// 删除文档
	// 根据id删除文档
	/*@Test
	public void testDelDoc() throws IOException{
		// 删除文档id
		String id = "ruZCJ2wB7dcg4IHXgfGu";
		// 删除索引请求对象
		DeleteRequest deleteRequest = new DeleteRequest("es_index","_doc","ruZCJ2wB7dcg4IHXgfGu");
		// 响应对象
		DeleteResponse deleteResponse = highLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
		// 获取响应结果
		DocWriteResponse.Result result = deleteResponse.getResult();
		System.out.println(result);
	}*/
	
	// 搜索type下所有记录
	/*@Test
	public void testSearchAll() throws IOException{
		SearchRequest searchRequest = new SearchRequest("es_index");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		// source源字段过滤
		searchSourceBuilder.fetchSource(new String[]{"name","studymodel"},new String[]{});
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchHits = hits.getHits();
		for(SearchHit hit:searchHits) {
			String index = hit.getIndex();
			float score = hit.getScore();
			String sourceAString = hit.getSourceAsString();
			Map<String, Object> sourceASMap = hit.getSourceAsMap();
			String name = (String) sourceASMap.get("name");
			String studymodel = (String) sourceASMap.get("studymodel");
			System.out.println(name);
			System.out.println(studymodel);
		}
	}*/
	
	// 分页查询
	@Test
	public void pageQuery() throws IOException {
		SearchRequest searchRequest = new SearchRequest("es_index");
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		// 分页查询，设置起始下标，从0开始
		searchSourceBuilder.from(0);
		// 每页显示个数
		searchSourceBuilder.size(10);
		// source源字段过滤
		searchSourceBuilder.fetchSource(new String[] {"name","studymodel"},new String[] {});
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse =highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
		SearchHits hits = searchResponse.getHits();
		SearchHit[] searchHits = hits.getHits();
		for(SearchHit hit:searchHits) {
			String index = hit.getIndex();
			float score = hit.getScore();
			String sourceAString = hit.getSourceAsString();
			Map<String, Object> sourceASMap = hit.getSourceAsMap();
			String name = (String) sourceASMap.get("name");
			String studymodel = (String) sourceASMap.get("studymodel");
			System.out.println(name);
			System.out.println(studymodel);
		}
	}

}
