package com.xnx3.elasticsearch.jsonFormat;

import java.util.Map;

/**
 * 简单的的json格式化接口，只支持map.value的类型是 String、int、long、float、double、boolean 这几个的转换
 * @author 管雷鸣
 *
 */
public class SimpleJsonFormat implements JsonFormatInterface{
	
	/**
     * 将Map<String, Object>转化为json字符串
     * @param params 其中map.value 支持的类型有 String、int、long、float、double、boolean
     */
	public String mapToJsonString(Map<String, Object> params) {
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
	
	
}
