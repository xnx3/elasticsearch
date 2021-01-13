package com.xnx3.elasticsearch.jsonFormat;

import java.util.HashMap;
import java.util.Map;
import com.alibaba.fastjson.JSON;

/**
 * 默认的json格式化接口
 * @author 管雷鸣
 *
 */
public class DefaultJsonFormat implements JsonFormatInterface{
	
	/**
     * 将Map<String, Object>转化为json字符串
     * @param params {@link Map}
     */
	public String mapToJsonString(Map<String, Object> params) {
    	if(params == null){
    		params = new HashMap<String, Object>();
    	}
        
        return JSON.toJSONString(params);
	}
	
}
