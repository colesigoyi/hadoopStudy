package cn.qf.hive;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-11 10:21
 * @ desc:
 **/

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

/**
 * {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}
 * {"movie":"661","rate":"3","timeStamp":"978302109","uid":"1"}
 * {"movie":"914","rate":"3","timeStamp":"978301968","uid":"1"}
 * {"movie":"3408","rate":"4","timeStamp":"978300275","uid":"1"}
 * {"movie":"2355","rate":"5","timeStamp":"978824291","uid":"1"}
 * {"movie":"1197","rate":"3","timeStamp":"978302268","uid":"1"}
 * {"movie":"1287","rate":"5","timeStamp":"978302039","uid":"1"}
 */
public class JsonDemo {
    public String evaluate(String json) {
        //对象的映射器
        ObjectMapper mapper = new ObjectMapper();
        try {
            Moviebean moviebean = mapper.readValue(json, Moviebean.class);
            return moviebean.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    @Test
    public void testJson() {
        String s = evaluate("{\"movie\":\"1193\",\"rate\":\"5\",\"timeStamp\":\"978300760\",\"uid\":\"1\"}");
        System.out.println(s);
    }
}
