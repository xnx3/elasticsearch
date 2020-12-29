package com.xnx3.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * ElasticSearch 操作
 * @author 管雷鸣
 *
 */
public class ElasticSearchUtil {
	private RestHighLevelClient client;
	private String hostname = "127.0.0.1";
	private int port = 9200;
	private String scheme = "http";
	
	public ElasticSearchUtil(String hostname) {
		this.hostname = hostname;
	}
	
	/**
	 * 获取操作的 client
	 * @return {@link RestHighLevelClient}
	 */
	public RestHighLevelClient getClient(){
		if(client == null){
			client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, scheme)));
		}
		return client;
	}
	

    /**
     * 创建索引
     * @param indexName 要创建的索引的名字，传入如： testindex
     * @return 创建索引的响应对象。可以使用 {@link CreateIndexResponse#isAcknowledged()} 来判断是否创建成功。如果为true，则是创建成功
     */
    public CreateIndexResponse createIndex(String indexName) throws IOException {
    	CreateIndexResponse response;
    	if(existIndex(indexName)){
    		response = new CreateIndexResponse(false, false, indexName);
            return response;
        }
    	
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        response = getClient().indices().create(request, RequestOptions.DEFAULT);
        return response;
    }
	
    public static void log(String text){
    	System.out.println(text);
    }

    /**
     * 判断索引是否存在
     * @param indexName 要判断是否存在的索引的名字，传入如： testindex
     * @return 返回是否存在。
     * 		<ul>
     * 			<li>true:存在</li>
     * 			<li>false:不存在</li>
     * 		</ul>
     */
    public boolean existIndex(String index){
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        boolean exists;
		try {
			exists = getClient().indices().exists(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
        return exists;
    }
	
    /**
     * 数据添加，正定ID
     * @param params 要增加的数据，key-value形式。 其中map.value 支持的类型有 String、int、long、float、double、boolean
     * @param index 索引，类似数据库的表，是添加进那个表
     * @param id 数据ID, 如果传入null，则自动生成一个32位uuid
     * @return 创建结果。如果 {@link IndexResponse#getId()} 不为null、且id长度大于0，那么就成功了
     */
    public IndexResponse put(Map<String, Object> params, String index, String id) throws IOException {
    	if(id == null){
    		id = UUID.randomUUID().toString().replaceAll("-", "").toLowerCase();
    	}
        //创建请求
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.timeout(TimeValue.timeValueSeconds(5));
        
        IndexResponse response = getClient().index(request.source(mapToJsonString(params), XContentType.JSON), RequestOptions.DEFAULT);
        return response;
    }

    /**
     * 将Map<String, Object>转化为json字符串
     * @param params 其中map.value 支持的类型有 String、int、long、float、double、boolean
     * @return 转化为json格式字符串，返回如： {"username":"管雷鸣","age":29} 
     */
    public static String mapToJsonString(Map<String, Object> params){
    	if(params == null){
    		return "{}";
    	}
    	StringBuffer sb = new StringBuffer();
        sb.append("{");
        for(Map.Entry<String, Object> entry : params.entrySet()){
        	Object obj = entry.getValue();
        	if(sb.length() > 1){
        		sb.append(",");
        	}
        	
        	if(obj instanceof String){
        		sb.append("\""+entry.getKey()+"\":\""+((String)obj).replaceAll("\"", "\\\\\"")+"\"");
        	}else if(obj instanceof Integer){
        		sb.append("\""+entry.getKey()+"\":"+(Integer)obj+"");
        	}else if(obj instanceof Long){
        		sb.append("\""+entry.getKey()+"\":"+(Long)obj+"");
        	}else if(obj instanceof Float){
        		sb.append("\""+entry.getKey()+"\":"+(Float)obj+"");
        	}else if( obj instanceof Double){
        		sb.append("\""+entry.getKey()+"\":"+(Double)obj+"");
        	}else if(obj instanceof Boolean){
        		sb.append("\""+entry.getKey()+"\":"+(Boolean)obj+"");
        	}else{
        		//其他类型全当成toString()来处理
        		sb.append("\""+entry.getKey()+"\":"+obj.toString()+"");
        	}
        }
        sb.append("}");
        
        return sb.toString();
    }
    
    public static void main(String[] args) {
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put("username", "管e\"/reriu");
    	map.put("age", 12);
    	map.put("price", 12.6f);
    	map.put("a", true);
    	System.out.println(mapToJsonString(map));
    	
    	ElasticSearchUtil es = new ElasticSearchUtil("192.168.31.134");
		System.out.println(es.existIndex("testind")+"");
		
		
	}
    
}
