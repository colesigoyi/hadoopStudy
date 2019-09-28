package cn.qf.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-27 14:56
 * @ desc:
 **/

public class Test3 {
    private Admin admin;
    private Connection connection;
    public static TableName tName = TableName.valueOf("bd1901:t_student_course");
    public static TableName tStudent = TableName.valueOf("bd1901:t_student");
    public static TableName tCourse = TableName.valueOf("bd1901:t_course");
    public static Random ra = new Random();


    @Before
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "10.211.55.28:2181,10.211.55.29:2181,10.211.55.30:2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }
    //@Test
    public void create() throws IOException{
        if(admin.tableExists(tName)){
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("sc".getBytes());
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
    }

    //@Test
    public void insert() throws IOException{
        Table table = connection.getTable(tName);
        List<Put> putList = new ArrayList<Put>();
        String stu1 = "student2019092715563211947";
        String stu2 = "student2019092715563226573";
        String stu3 = "student2019092715563233514";

        Put put1 = new Put("student2019092715563211947_course2019092716023326198".getBytes());
        put1.addColumn("sc".getBytes(), "student".getBytes(), stu1.getBytes());
        Put put2 = new Put("student2019092715563211947_course2019092716023346509".getBytes());
        put2.addColumn("sc".getBytes(), "student".getBytes(), stu1.getBytes());
        Put put3 = new Put("student2019092715563211947_course2019092716023355677".getBytes());
        put3.addColumn("sc".getBytes(), "student".getBytes(), stu1.getBytes());
        Put put4 = new Put("student2019092715563226573_course2019092716023326198".getBytes());
        put4.addColumn("sc".getBytes(), "student".getBytes(), stu2.getBytes());
        Put put5 = new Put("student2019092715563226573_course2019092716023346509".getBytes());
        put5.addColumn("sc".getBytes(), "student".getBytes(), stu2.getBytes());
        Put put6 = new Put("student2019092715563233514_course2019092716023364007".getBytes());
        put6.addColumn("sc".getBytes(), "student".getBytes(), stu3.getBytes());
        putList.add(put1);
        putList.add(put2);
        putList.add(put3);
        putList.add(put4);
        putList.add(put5);
        putList.add(put6);
        table.put(putList);
    }

    //第一题
    @Test
    public void findCourseByStudent() throws IOException{
        Table table = connection.getTable(tName);
        String stuId="student2019092715563211947";
        List<String> courseIds =new ArrayList<String>();
        Scan scan = new Scan();
        RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(stuId+"_"));
        scan.setFilter(rf1);
        ResultScanner rs1 = table.getScanner(scan);
        Iterator<Result> it = rs1.iterator();
        while(it.hasNext()){
            Result result = it.next();
            byte[] rowKey = result.getRow();
            courseIds.add(new String(rowKey,"utf8"));
        }

        for(String id : courseIds){
            String courseId = id.split("_")[1];
            Table  courseTable = connection.getTable(tCourse);
            Get get = new Get(courseId.getBytes());
            Result result= courseTable.get(get);
            byte[] name = result.getValue("sc".getBytes(), "name".getBytes());
            System.out.println("courseID:"+courseId+" courseName："+new String(name,"utf8"));
        }
    }

    //第二题
    @Test
    public void findStudentByCourse() throws IOException{
        Table table = connection.getTable(tName);
        String courseId="course2019092716023326198";
        List<String> studentIds =new ArrayList<String>();
        Scan scan = new Scan();
        RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("_"+courseId));
        scan.setFilter(rf1);
        ResultScanner rs1 = table.getScanner(scan);
        Iterator<Result> it = rs1.iterator();
        while(it.hasNext()){
            Result result = it.next();
            byte[] rowKey = result.getRow();
            studentIds.add(new String(rowKey,"utf8"));
        }

        for(String id : studentIds){
            String stuId = id.split("_")[0];
            Table  stuTable = connection.getTable(tStudent);
            Get get = new Get(stuId.getBytes());
            Result result= stuTable.get(get);
            byte[] name = result.getValue("sc".getBytes(), "name".getBytes());
            System.out.println("studentID:"+courseId+" studentName："+new String(name,"utf8"));
        }

    }
    //第三题
    @Test
    public void changeCourseOfStudent() throws IOException{
        String stuId="student2019092715563287261";
        String courseId="course2019092716023388994";
        String scId=stuId+"_"+courseId;
        Delete delete = new Delete(scId.getBytes());
        Table table = connection.getTable(tName);
        table.delete(delete);

        List<Put> putList = new ArrayList<Put>();
        String stu = "student2019092715563288408";
        Put put1 = new Put("student2019092715563288408_course2019092716023391230".getBytes());
        put1.addColumn("sc".getBytes(), "student".getBytes(), stu.getBytes());
        putList.add(put1);
        table.put(putList);
    }

    @After
    public void after() throws IOException {
        if(connection!=null){
            connection.close();
        }
    }

}