package com.xnx3.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

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
	
	/**
	 * 缓存。
	 * key:  indexName
	 * value： 要打包提交的 List
	 */
	public Map<String, List<Map<String, Object>>> cacheMap;
	public int cacheMaxNumber = 100; //如果使用缓存，这里是缓存中的最大条数，超过这些条就会自动打包提交
	
	/**
	 * 通过传入自定义 {@link RestHighLevelClient} 的方式，创建工具类
	 * @param client 传入如：
	 * 	<pre>
	 *   new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", "9200", "http")))
	 * 	</pre>
	 */
	public ElasticSearchUtil(RestHighLevelClient client) {
		this.client = client;
		cacheMap = new HashMap<String, List<Map<String,Object>>>();
	}
	
	/**
	 * 初始化 ElasticSearchUtil
	 * @param hostname elasticsearch所在的ip，传入如 127.0.0.1
	 */
	public ElasticSearchUtil(String hostname) {
		this.hostname = hostname;
		cacheMap = new HashMap<String, List<Map<String,Object>>>();
	}
	
	/**
	 * 初始化 ElasticSearchUtil
	 * @param hostname elasticsearch所在的ip，传入如 127.0.0.1
	 * @param port elasticsearch服务的端口号，传入如 9200
	 * @param scheme 协议，传入如 http
	 */
	public ElasticSearchUtil(String hostname, int port, String scheme) {
		this.hostname = hostname;
		this.port = port;
		this.scheme = scheme;
		cacheMap = new HashMap<String, List<Map<String,Object>>>();
	}
	
	
	/**
	 * 设置缓存中最大缓存的条数。超过这些条就会自动打包提交。 这里自动提交的便是 {@link #cache(Map)} 所缓存的数据
	 * @param cacheMaxNumber 缓存的条数。如果不设置，默认是100
	 */
	public void setCacheMaxNumber(int cacheMaxNumber) {
		this.cacheMaxNumber = cacheMaxNumber;
	}

	/**
	 * 获取操作的 client
	 * @return {@link RestHighLevelClient}
	 */
	public RestHighLevelClient getClient(){
		if(client == null){
			client = new RestHighLevelClient(RestClient.builder(new HttpHost(this.hostname, this.port, this.scheme)));
		}
		return client;
	}
	
	/**
	 * 将之提交到缓存Cache中。这里不同意put,put是直接提交到ElasticSearch中，而这个只是提交到Java缓存中，等积累到一定条数之后，在一起将Java缓存中的打包一次性提交到 Elasticsearch中
	 * <p>默认同一个indexName索引中，缓存最大条数是100条，达到100条会自动提交到 elasticsearch。 这个最大条数，可以通过  {@link #setCacheMaxNumber(int)} 进行设置。建议不要超过4000条 </p>
	 * @param params 要增加的数据，key-value形式。 其中map.value 支持的类型有 String、int、long、float、double、boolean
	 * @param indexName 索引名字，类似数据库的表，是将数据添加进哪个表
	 */
	public synchronized void cache(Map<String, Object> params, String indexName){
		List<Map<String,Object>> list = cacheMap.get(indexName);
		if(list == null){
			list = new LinkedList<Map<String,Object>>();
		}
		list.add(params);
		
		if(list.size() >= this.cacheMaxNumber){
			//提交
			boolean submit = cacheSubmit(indexName);
			if(submit){
				//提交成功，那么清空indexName的list
				list.clear();
			}
		}
		
		//重新赋予cacheMap
		cacheMap.put(indexName, list);
	}
	
	/**
	 * 将当前缓存中某个索引中的数据提交到elasticsearch中
	 * @param indexName 索引名字，类似数据库的表，是将数据添加进哪个表
	 * @return true:成功；  false:提交失败
	 */
	public synchronized boolean cacheSubmit(String indexName){
		List<Map<String,Object>> list = cacheMap.get(indexName);
		if(list == null){
			return true;
		}
		
		BulkResponse res = puts(list, indexName);
		if(res.hasFailures()){
			//出现错误，那么不清空list
			return false;
		}else{
			//成功，那么清空缓存中这个索引的数据
			list.clear();
			cacheMap.put(indexName, list);
			return true;
		}
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
     * 数据添加，网 elasticsearch 中添加一条数据
     * @param params 要增加的数据，key-value形式。 其中map.value 支持的类型有 String、int、long、float、double、boolean
     * @param indexName 索引名字，类似数据库的表，是添加进那个表
     * @param id 要添加的这条数据的id, 如果传入null，则由es系统自动生成一个唯一ID
     * @return 创建结果。如果 {@link IndexResponse#getId()} 不为null、且id长度大于0，那么就成功了
     */
    public IndexResponse put(Map<String, Object> params, String indexName, String id){
        //创建请求
        IndexRequest request = new IndexRequest(indexName);
        if(id != null){
        	request.id(id);
    	}
        request.timeout(TimeValue.timeValueSeconds(5));
        
        IndexResponse response = null;
		try {
			response = getClient().index(request.source(mapToJsonString(params), XContentType.JSON), RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
        return response;
    }
    

    /**
     * 数据添加，网 elasticsearch 中添加一条数据
     * @param params 要增加的数据，key-value形式。 其中map.value 支持的类型有 String、int、long、float、double、boolean
     * @param indexName 索引名字，类似数据库的表，是添加进那个表
     * @param id 要添加的这条数据的id, 如果传入null，则由es系统自动生成一个唯一ID
     * @return 创建结果。如果 {@link IndexResponse#getId()} 不为null、且id长度大于0，那么就成功了
     */
    public IndexResponse put(Map<String, Object> params, String indexName){
        return put(params, indexName, null);
    }
    
    /**
     * 批量添加数据
     * @param list 批量添加的数据的List
     * @param indexName 索引名字，类似数据库的表，是添加进那个表
     * @return {@link BulkResponse}
     */
    public BulkResponse puts(List<Map<String, Object>> list, String indexName){
    	//批量增加
        BulkRequest bulkAddRequest = new BulkRequest();
        IndexRequest indexRequest;
        for (int i = 0; i < list.size(); i++) {
        	indexRequest = new IndexRequest(indexName);
        	indexRequest.source(mapToJsonString(list.get(i)), XContentType.JSON);
        	bulkAddRequest.add(indexRequest);
		}
        
        BulkResponse bulkAddResponse = null;
        try {
        	bulkAddResponse = getClient().bulk(bulkAddRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
        return bulkAddResponse;
    }

    /**
     * 查询并分页
     * @param indexName 索引名字
     * @param query 查询条件， {@link SearchSourceBuilder}
     * @param from 从第几条开始查询，相当于 limit a,b 中的a ，比如要从最开始第一条查，可传入： 0
     * @param size 本次查询最大查询出多少条数据 ,相当于 limit a,b 中的b
     * @return {@link SearchResponse} 结果，可以通过 response.status().getStatus() == 200 来判断是否执行成功
     */
    public SearchResponse search(String indexName, SearchSourceBuilder searchSourceBuilder, Integer from, Integer size){
        SearchRequest request = new SearchRequest(indexName);
        searchSourceBuilder.from(from);
        searchSourceBuilder.size(size);
        request.source(searchSourceBuilder);
        SearchResponse response = null;
		try {
			response = getClient().search(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
        return response;
    }
    

    /**
     * 查询数据
     * @param indexName 索引名字
     * @param queryString 查询条件，传入如： name:guanleiming AND age:123
     * @param from 从第几条开始查询，相当于 limit a,b 中的a ，比如要从最开始第一条查，可传入： 0
     * @param size 本次查询最大查询出多少条数据 ,相当于 limit a,b 中的b
     * @param sort 排序方式。如果不需要排序，传入null即可。 比如要根据加入时间time由大到小，传入的便是： SortBuilders.fieldSort("time").order(SortOrder.DESC)
     * @return 查询的结果，封装成list返回。list中的每条都是一条结果。如果链接es出错或者查询异常又或者什么都没查出，那么都是返回一个 new ArrayList<Map<String,Object>>(); ，任何情况返回值不会为null
     * 		<p>返回的结果集中，每条会自动加入一项 esid ，这个是在es中本条记录的唯一id编号，es自动赋予的。</p> 
     */
    public List<Map<String,Object>> search(String indexName, String queryString, Integer from, Integer size, SortBuilder sort){
    	List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
    	
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(queryString);
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        //判断是否使用排序
        if(sort != null){
        	searchSourceBuilder.sort(sort);
        }
        SearchResponse response = search(indexName, searchSourceBuilder, from, size);
        if(response.status().getStatus() == 200){
        	SearchHit shs[] = response.getHits().getHits();
        	for (int i = 0; i < shs.length; i++) {
        		Map<String, Object> map = shs[i].getSourceAsMap();
        		map.put("esid", shs[i].getId());
				list.add(map);
			}
        }else{
        	//异常
        }
        
        return list;
    }
    

    /**
     * 查询数据
     * <p>如果数据超过100条，那么只会返回前100条数据。<p>
     * @param indexName 索引名字
     * @param queryString 查询条件，传入如： name:guanleiming AND age:123
     * @return 查询的结果，封装成list返回。list中的每条都是一条结果。如果链接es出错或者查询异常又或者什么都没查出，那么都是返回一个 new ArrayList<Map<String,Object>>(); ，任何情况返回值不会为null
     * 		<p>返回的结果集中，每条会自动加入一项 esid ，这个是在es中本条记录的唯一id编号，es自动赋予的。</p> 
     */
    public List<Map<String,Object>> search(String indexName, String queryString){
    	return search(indexName, queryString, 0, 100, null);
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
        	
        	if(obj == null){
        		sb.append("\""+entry.getKey()+"\":\"\"");
        	}else if(obj instanceof String){
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
    

    /**
     * 通过elasticsearch数据的id，获取这条数据
     * @param indexName 索引名字
     * @param id elasticsearch数据的id
     * @return 这条数据的内容。 如果返回null，则是没有找到这条数据，或者执行过程出错。
     */
    public Map<String,Object> searchById(String indexName, String id){
        GetRequest request = new GetRequest(indexName, id);
        GetResponse response = null;
		try {
			response = getClient().get(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		if(response.isSourceEmpty()){
			//没有这条数据
			return null;
		}
		
        Map<String, Object> map = response.getSource();
        //为返回的数据添加id
        map.put("esid",response.getId());
        return map;
    }
    

    /**
     * 通过elasticsearch数据的id，来删除这条数据
     * @param indexName 索引名字
     * @param id 要删除的elasticsearch这行数据的id
     */
    public boolean deleteById(String indexName, String id) {
        DeleteRequest request = new DeleteRequest(indexName, id);
        DeleteResponse delete = null;
		try {
			delete = client.delete(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
			//删除失败
			return false;
		}
		
		if(delete == null){
			//这种情况应该不存在
			return false;
		}
		if(delete.getResult().equals(Result.DELETED)){
			return true;
		}else{
			return false;
		}
    }
    
    public static void main(String[] args) {
    	String indexName = "testind";
    	Map<String, Object> map = new HashMap<String, Object>();
    	map.put("username", "zhangqun222");
    	map.put("age", 17);
    	map.put("price", 12.6f);
    	map.put("a", true);
    	
//    	List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
//    	list.add(map);
//    	map.put("age", 14);
//    	list.add(map);
//    	map.put("age", 15);
//    	list.add(map);
    	
    	ElasticSearchUtil es = new ElasticSearchUtil("192.168.31.24");
//    	IndexResponse ir = es.put(map, indexName);
//    	BulkResponse ir = es.puts(list, indexName);
//    	System.out.println(ir);
    	
//    	QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("age:12 AND a:false");
//    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(queryBuilder);
//		System.out.println(es.searchListData(indexName, searchSourceBuilder, 0, 10).toString());

    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	es.cache(map, indexName);
    	System.out.println(es.cacheSubmit(indexName));
    	
    	
//    	List<Map<String, Object>> lists = es.search("useraction", "", 0, 100, null);
    	List<Map<String, Object>> lists = es.search(indexName, "username:zhangqun222");
    	for (int i = 0; i < lists.size(); i++) {
			System.out.println(lists.get(i));
		}
    	System.out.println(lists.size());
//    	Map<String, Object> m = es.searchById(indexName, "ffcb76770ecb40dcb74bdd5b9a993164");
//    	System.out.println(m);
//    	boolean b = es.deleteById(indexName, "9902241cc47445458e17bc8d5520cb22");
//    	System.out.println(b);
	}
    
}
