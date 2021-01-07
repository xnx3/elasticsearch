## elasticsearch
ElasticSearch工具类，像操作数据库一般两行代码操作ElasticSearch

## elasticsearch版本
使用的 elasticsearch-7.10.1 做的测试。

## 使用
#### 1. Maven引入
````
<dependency>
	<groupId>com.xnx3.elasticsearch</groupId>
	<artifactId>elasticsearch</artifactId>
	<version>1.0</version>
</dependency>
````

#### 2. 创建ElasticSearch操作的工具类对象
````
ElasticSearchUtil es = new ElasticSearchUtil("192.168.31.134");
````

#### 写入数据
````
//用map定义一条数据
Map<String, Object> map = new HashMap<String, Object>();
map.put("username", "guanleiming");
map.put("age", 13);
//提交保存到elasticsearch的user索引(类似于数据的表)中
es.put(map, "user");
````

#### 查询数据

##### 从user索引(类似于数据的表)中，查询username的值是guanleiming的数据
````
List<Map<String, Object>> list = es.search("user", "username:guanleiming");
````

##### 从user索引(类似于数据的表)中，查询 username的值是guanleiming ，同时 age是13 的数据
````
List<Map<String, Object>> list = es.search("user", "username:guanleiming AND age:13");
````

#### 更多扩展
````
es.getClient().
````
可以通过获取client对象，来使用更多 elasticsearch sdk 本身所具有的功能