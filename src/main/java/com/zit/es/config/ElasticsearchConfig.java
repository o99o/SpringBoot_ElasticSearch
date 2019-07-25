package com.zit.es.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {
	@Value("${elasticsearch.hostlist}")
	private String hostlist;

	@Bean
	public RestHighLevelClient restHighLevelClient() {
		// 解析hostlist配置信息
		String[] spilt = hostlist.split(",");
		// 创建HttpHost数组，其中存放主机和端口的配置信息
		HttpHost[] httpHostArray = new HttpHost[spilt.length];
		for(int i=0;i<spilt.length;++i) {
			String item = spilt[i];
			httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
		}
		
		// 创建RestHighLevelClient客户端
		return new RestHighLevelClient(RestClient.builder(httpHostArray));
	}
	
	// 项目主要使用RestHighLevelClient，对于低级客户端暂时不用
	@Bean
	public RestClient restClient() {
		// 解析hostlist配置信息
		String[] spilt = hostlist.split(",");
		// 创建HttpPost数组，存放es主机和端口的配置信息
		HttpHost[] httpHostArray = new HttpHost[spilt.length];
		for(int i=0;i<spilt.length;++i) {
			String item = spilt[i];
			httpHostArray[i] = new HttpHost(item.split(":")[0],
					Integer.parseInt(item.split(":")[1]),"http");
		}
		return RestClient.builder(httpHostArray).build();
	}
	
}
