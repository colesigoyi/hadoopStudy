package cn.qf.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ program: mavenStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-03 10:18
 * @ desc: HDFS工具类
 **/

public class HDFSUtils {
    private static FileSystem fs;
    private static Configuration configuration;
    static {
        try {
            configuration = new Configuration();
            fs = FileSystem.get(new URI("hdfs://10.211.55.27:9000"),
                    configuration, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    public static FileSystem getFs() {
        return fs;
    }
    public static void close(FileSystem fs) {
        try {
            if (fs != null) {
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static FileSystem set(String key, String value) {
        configuration.set(key, value);
        try {
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            return fs;
        }
    }
}
