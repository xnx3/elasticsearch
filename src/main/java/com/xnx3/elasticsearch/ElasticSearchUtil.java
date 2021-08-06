package com.xnx3.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xnx3.elasticsearch.bean.GroupByListItem;
import com.xnx3.elasticsearch.jsonFormat.DefaultJsonFormat;
import com.xnx3.elasticsearch.jsonFormat.JsonFormatInterface;

/**
 * ElasticSearch 操作
 * @author 管雷鸣
 *
 */
public class ElasticSearchUtil {
	private RestHighLevelClient restHighLevelClient;
	private RestClient restClient;
	
	private String hostname = "127.0.0.1";
	private int port = 9200;
	private String scheme = "http";
	private String username = "";	//elasticsearch链接的用户名，如果es本身没设置使用用户名密码的，那这里就不用设置
	private String password = "";	//elasticsearch链接的密码，如果es本身没设置使用用户名密码的，那这里就不用设置
	private JsonFormatInterface jsonFormatInterface; //JSON格式化接口。默认使用 DefaultJsonFormat();
	private HttpHost[] httpHosts;
	
	/**
	 * 缓存。
	 * key:  indexName
	 * value： 要打包提交的 List
	 */
	public Map<String, List<Map<String, Object>>> cacheMap;
	public int cacheMaxNumber = 100; //如果使用缓存，这里是缓存中的最大条数，超过这些条就会自动打包提交
	
	/**
	 * 通过传入自定义 {@link HttpHost} 的方式，创建工具类
	 * @param client 传入如：
	 * 	<pre>
	 *   new HttpHost("127.0.0.1", "9200", "http"))
	 * 	</pre>
	 */
	public ElasticSearchUtil(HttpHost... httpHosts) {
		this.httpHosts = httpHosts;
		cacheMap = new HashMap<String, List<Map<String,Object>>>();
		jsonFormatInterface = new DefaultJsonFormat();
	}
	
	/**
	 * 初始化 ElasticSearchUtil
	 * @param hostname elasticsearch所在的ip，传入如 127.0.0.1
	 */
	public ElasticSearchUtil(String hostname) {
		this.hostname = hostname;
		cacheMap = new HashMap<String, List<Map<String,Object>>>();
		jsonFormatInterface = new DefaultJsonFormat();
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
		jsonFormatInterface = new DefaultJsonFormat();
	}
	
	/**
	 * 初始化 ElasticSearchUtil
	 * @param hostname elasticsearch所在的ip，传入如 127.0.0.1
	 * @param port elasticsearch服务的端口号，传入如 9200
	 * @param scheme 协议，传入如 http
	 * @param username 如果ElasticSearch 有设置用户名密码登录的方式，这里要传入设置的用户名。如果没有密码，这里传入null或者空字符串都行
	 * @param password 如果ElasticSearch 有设置用户名密码登录的方式，这里要传入设置的密码。如果没有密码，这里传入null或者空字符串都行
	 */
	public ElasticSearchUtil(String hostname, int port, String scheme, String username, String password) {
		this.hostname = hostname;
		this.port = port;
		this.scheme = scheme;
		if(username != null && username.length() > 0) {
			this.username = username;
		}
		if(password != null && password.length() > 0) {
			this.password = password;
		}
		cacheMap = new HashMap<String, List<Map<String,Object>>>();
		jsonFormatInterface = new DefaultJsonFormat();
	}
	
	/**
	 * 设置缓存中最大缓存的条数。超过这些条就会自动打包提交。 这里自动提交的便是 {@link #cache(Map)} 所缓存的数据
	 * @param cacheMaxNumber 缓存的条数。如果不设置，默认是100
	 */
	public void setCacheMaxNumber(int cacheMaxNumber) {
		this.cacheMaxNumber = cacheMaxNumber;
	}

	/**
	 * JSON格式化接口。如果不设置此处，默认使用 {@link DefaultJsonFormat}
	 * @param jsonFormatInterface 设置自定义json序列化方法
	 */
	public void setJsonFormatInterface(JsonFormatInterface jsonFormatInterface) {
		this.jsonFormatInterface = jsonFormatInterface;
	}

	/**
	 * 获取操作的 {@link RestHighLevelClient} 对象
	 * @return {@link RestHighLevelClient}
	 */
	public RestHighLevelClient getRestHighLevelClient(){
		if(this.restHighLevelClient == null){
			if(this.httpHosts == null){
				//没有直接传入 httpshosts，那么就是使用单个的
				HttpHost httpHost = new HttpHost(this.hostname, this.port, this.scheme);
				this.httpHosts = new HttpHost[1];
				this.httpHosts[0] = httpHost;
			}
			this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(this.httpHosts));
		}
		return this.restHighLevelClient;
	}
	
	/**
	 * 获取操作的 {@link RestClient} 对象
	 * @return {@link RestClient}
	 */
	public RestClient getRestClient(){
		if(this.restClient == null){
			if(this.httpHosts == null){
				//没有直接传入 httpshosts，那么就是使用单个的
				HttpHost httpHost = new HttpHost(this.hostname, this.port, this.scheme);
				this.httpHosts = new HttpHost[1];
				this.httpHosts[0] = httpHost;
			}
			this.restClient = RestClient.builder(this.httpHosts).build();
		}
		return this.restClient;
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
		if(res == null || res.hasFailures()){
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
        response = getRestHighLevelClient().indices().create(request, RequestOptions.DEFAULT);
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
			exists = getRestHighLevelClient().indices().exists(request, RequestOptions.DEFAULT);
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
			response = getRestHighLevelClient().index(request.source(jsonFormatInterface.mapToJsonString(params), XContentType.JSON), RequestOptions.DEFAULT);
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
     * @return {@link BulkResponse} ，如果没提交，或者提交的是空，或者出错，那么会返回null。判断其有没有提交成功可以使用  (res != null && !res.hasFailures())    
     */
    public BulkResponse puts(List<Map<String, Object>> list, String indexName){
    	if(list.size() < 1){
    		return null;
    	}
    	
    	//批量增加
        BulkRequest bulkAddRequest = new BulkRequest();
        IndexRequest indexRequest;
        for (int i = 0; i < list.size(); i++) {
        	indexRequest = new IndexRequest(indexName);
        	indexRequest.source(jsonFormatInterface.mapToJsonString(list.get(i)), XContentType.JSON);
        	bulkAddRequest.add(indexRequest);
		}
        
        BulkResponse bulkAddResponse = null;
        try {
        	bulkAddResponse = getRestHighLevelClient().bulk(bulkAddRequest, RequestOptions.DEFAULT);
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
			response = getRestHighLevelClient().search(request, RequestOptions.DEFAULT);
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
    	
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	if(queryString != null && queryString.length() > 0){
    		//有查询条件，才会进行查询，否则会查出所有
    		QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(queryString);
    		searchSourceBuilder.query(queryBuilder);
    	}
    	
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
     * 通过elasticsearch数据的id，获取这条数据
     * @param indexName 索引名字
     * @param id elasticsearch数据的id
     * @return 这条数据的内容。 如果返回null，则是没有找到这条数据，或者执行过程出错。
     */
    public Map<String,Object> searchById(String indexName, String id){
        GetRequest request = new GetRequest(indexName, id);
        GetResponse response = null;
		try {
			response = getRestHighLevelClient().get(request, RequestOptions.DEFAULT);
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
			delete = getRestHighLevelClient().delete(request, RequestOptions.DEFAULT);
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
    
    /**
     * 以 sql查询语句的形式，搜索 elasticsearch
     * @param sqlQuery sql查询语句，传入如： select * from user WHERE username = 'guanleiming'
     * @return List结果
     */
    public List<Map<String, Object>> searchBySqlQuery(String sqlQuery){
    	List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
    	
    	String method = "GET";
        String endPoint = "/_sql";
        Request request = new Request(method, endPoint);
        request.addParameter("format", "json");
        request.setJsonEntity("{\"query\":\""+sqlQuery+"\"}");
    	
    	try {
			Response response = getRestClient().performRequest(request);
			String text = EntityUtils.toString(response.getEntity());
			
			JSONObject json = JSONObject.parseObject(text);
			JSONArray columnsJsonArray = json.getJSONArray("columns");
			String columns[] = new String[columnsJsonArray.size()];
			//遍历columns
			for (int i = 0; i < columnsJsonArray.size(); i++) {
				JSONObject columnJsonObject = columnsJsonArray.getJSONObject(i);
				columns[i] = columnJsonObject.getString("name");
			}
			
			//遍历数据
			JSONArray rowsJsonArray = json.getJSONArray("rows");
			for (int i = 0; i < rowsJsonArray.size(); i++) {
				JSONArray row = rowsJsonArray.getJSONArray(i);
				
				Map<String, Object> map = new HashMap<String, Object>();
				for (int j = 0; j < row.size(); j++) {
					Object obj = row.get(j);
					if(obj != null){
						//如果此项不为null，那么加入 map
						map.put(columns[j], obj);
					}
				}
				list.add(map);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return list;
    }
    
    /**
     * group by 统计
     * @param indexName 要统计的是哪个索引（数据库表）
     * @param field 针对的是哪个字段，也就是 group by field ，如传入 username
     * @param queryBuilder 查询条件，传入如
     * <pre>
     * //查询time > 1 AND time < 10 
     * QueryBuilders.rangeQuery("time").gt(1).lt(10);
     * </pre>
     * 		其中: 
     * 			<ul>
     * 				<li>gt : 大于</li>
     * 				<li>gte : 大于等于</li>
     * 				<li>lt : 小于</li>
     * 				<li>lte : 小于等于</li>
     * 			</ul>
     * @return 结果，按照统计条数有大到小排序。如果失败，那么返回的 list.size() 为0
     */
    public List<GroupByListItem> groupBy(String indexName, String field, QueryBuilder queryBuilder){
    	SearchRequest searchRequest = new SearchRequest();
    	searchRequest.indices(indexName);
    	TermsAggregationBuilder aggregation = AggregationBuilders.terms("termsname").field(field+".keyword").order(BucketOrder.count(false)).size(100);
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    	searchSourceBuilder.aggregation(aggregation);
    	if(queryBuilder != null){
    		searchSourceBuilder.query(queryBuilder);
    	}
    	searchRequest.source(searchSourceBuilder);
    	
    	List<GroupByListItem> list = new ArrayList<GroupByListItem>();
    	SearchResponse response;
    	try {
			response = getRestHighLevelClient().search(searchRequest, RequestOptions.DEFAULT);
			Terms byAgeAggregation = response.getAggregations().get("termsname");
			for (Terms.Bucket buck : byAgeAggregation.getBuckets()) {
				GroupByListItem item = new GroupByListItem();
				item.setName(buck.getKeyAsString());
				item.setCount(buck.getDocCount());
				list.add(item);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return list;
    }
    
    /**
     * @param countSql 统计的sql语句，传入格式如： 
     * 	<pre>
     * select count(*) from useraction WHERE action='我是字符串类型' AND time > 1624001953
     * 	</pre>
     * @return 执行统计语句所获取到的结果，返回值为int类型
     */
    public int count(String countSql){
    	List<Map<String, Object>> list = searchBySqlQuery(countSql);
    	int count = (Integer) list.get(0).values().stream().findAny().get();
	    return count;
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
    	
//    	final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//    	credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("admin", "123456Abc"));
//    	RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("114.116.251.118", 9200, "https")
//                ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        httpClientBuilder.disableAuthCaching();
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                })
                
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        httpClientBuilder.disableAuthCaching();
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                })
//        );
    	
    	ElasticSearchUtil es = new ElasticSearchUtil("192.168.31.24");
    	if(!es.existIndex(indexName)){
    		try {
				es.createIndex(indexName);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	
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
//    	System.out.println(es.cacheSubmit(indexName));
    	
    	
//    	String method = "GET";
//        String endPoint = "/_sql";
//        Request request = new Request(method, endPoint);
//        request.addParameter("format", "json");
//        
//        String sql = "update testind set age = 18 WHERE username = 'zhangqun222'";
//        //String sql = "select * from testind WHERE username = 'zhangqun222'";
//        
//        request.setJsonEntity("{\"query\":\""+sql+"\"}");
//
//        Response response = es.getClient().p
////        		.performRequest(request);
//    	
    	
//    	RestHighLevelClient
//    	RestClient restClient = RestClient.builder(
//                new HttpHost("192.168.31.24", 9200, "http")
//         ).build();
//    	try {
//			Response response = restClient.performRequest(request);
//			String text = EntityUtils.toString(response.getEntity());
//			System.out.println(text);
//			
//			JSONObject json = JSONObject.parseObject(text);
////					parseObject(EntityUtils.toString(response.getEntity()));
//			System.out.println(json);
//			JSONArray columnsJsonArray = json.getJSONArray("columns");
//			String columns[] = new String[columnsJsonArray.size()];
//			//遍历columns
//			for (int i = 0; i < columnsJsonArray.size(); i++) {
//				JSONObject columnJsonObject = columnsJsonArray.getJSONObject(i);
//				columns[i] = columnJsonObject.getString("name");
//				System.out.println(columns[i]);
//			}
//			System.out.println(columns);
//			
//			//遍历数据
//			JSONArray rowsJsonArray = json.getJSONArray("rows");
//			for (int i = 0; i < rowsJsonArray.size(); i++) {
//				JSONArray row = rowsJsonArray.getJSONArray(i);
//				System.out.println(row);
//			}
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
    	
    	
//    	List<Map<String, Object>> lists = es.search("useraction", "", 0, 100, null);
//    	List<Map<String, Object>> lists = es.search("useraction", "SELECT username, COUNT(*) as number GROUP BY username");
//    	List<Map<String, Object>> lists = es.search(indexName, "SELECT * FROM testind WHERE username='zhangqun222'");
//    	for (int i = 0; i < lists.size(); i++) {
//			System.out.println(lists.get(i));
//		}
//    	System.out.println(lists.size());
//    	
    	
//    	Map<String, Object> m = es.searchById(indexName, "ffcb76770ecb40dcb74bdd5b9a993164");
//    	System.out.println(m);
//    	boolean b = es.deleteById(indexName, "9902241cc47445458e17bc8d5520cb22");
//    	System.out.println(b);
    	
    	
//    	List<Map<String, Object>> list = es.searchBySqlQuery("SELECT * FROM useraction WHERE username = 'wangzhan' AND time > 1610340207219 ORDER BY time ASC LIMIT 10");
//    	for (int i = 0; i < list.size(); i++) {
//			System.out.println(list.get(i));
//		}
//		
    	
    	List<Map<String,Object>> list = es.search("useraction", "sum(*)");
    	for (int i = 0; i < list.size(); i++) {
			System.out.println(list.get(i));
		}
    	
    	
	}
    
}
