package com.xnx3.elasticsearch.bean;

import com.xnx3.elasticsearch.ElasticSearchUtil;

/**
 * 进行group by统计的结果返回
 * <p>服务于 {@link ElasticSearchUtil#groupBy(String, String, org.elasticsearch.index.query.QueryBuilder)}</p>
 * @author 管雷鸣
 */
public class GroupByListItem {
	private String name;	//group by 搜索的列的值
	private long count;		//统计的条数
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	@Override
	public String toString() {
		return "GroupByList [name=" + name + ", count=" + count + "]";
	}
	
}
