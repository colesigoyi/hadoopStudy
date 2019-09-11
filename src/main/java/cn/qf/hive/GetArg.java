package cn.qf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-11 09:58
 * @ desc:
 **/

public class GetArg extends UDF {
    /**
     * 第一种方法
     * sex=1&height=180&weight=180&sal=50000
     *
    public String evaluate(String arg, String key) {
        String[] fields = arg.split("&");
        Map<String, String> map = new HashMap<String,String>();
        for (String field : fields) {
            String[] kv = field.split("=");
            map.put(kv[0], kv[1]);
        }
        return map.get(key);
    }
     */

    /**
     * 第二种方法
     * sex=1&height=180&weight=180&sal=50000
     * {"sex":"1","height":"180","weight":"180","sal":"50000"}
     */
    public String evaluate(String arg, String key) {
        arg = arg.replace("&",",");
        arg = arg.replace("=",":");
        arg = "{" + arg + "}";
        System.out.println(arg);
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(arg);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        try {
            if (jsonObject != null) {
                return jsonObject.getString(key);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }
    @Test
    public void testJson() {
        String sal = evaluate("sex=1&height=180&weight=180&sal=50000", "sal");
        System.out.println(sal);
    }
}
