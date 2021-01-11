package com.xnx3.elasticsearch.jsonFormat;

import java.util.Map;

/**
 * JSON转换相关
 * @author 管雷鸣
 *
 */
public interface JsonFormatInterface {
	
	/**
	 * 将 {@link Map} 转化为JSON格式字符串
	 * @param params {@link Map}String, Object
	 * @return JSON格式字符串。如果传入的 map为null，那么这里返回 {} 也就是空的JSON字符串。
	 * 		<p>返回如： {"username":"管雷鸣","age":29}</p>
	 */
	public String mapToJsonString(Map<String, Object> params);
}
