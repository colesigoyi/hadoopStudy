package cn.qf.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @ program: hadoopStudy
 * @author  TaoXueFeng
 * @ create: 2019-09-10 20:42
 * @ desc:
 **/

/*
220.181.108.151 ‐ ‐ [31/Jan/2012:00:02:32 +0800] \"GET /home.php?
mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"‐\" \"Mozilla/5.0
(compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\"
 */

/*
 * 220.181.108.151 20120131 120232 GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP 200 Mozilla
 */

public class RegularParserUdf extends UDF {
    /**
    判断字符串是否为空
     */
    public String evaluate(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return null;
        }
        String ip = "^[0-9]+.[0-9]+.[0-9]+.[0-9]+";
        String time = "\\[[0-9]+\\/[a-zA-Z]+\\/[0-9:]+";



        return null;
    }
}
