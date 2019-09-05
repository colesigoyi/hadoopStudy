package cn.qf.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @ program: mavenStudy
 * @author  TaoXueFeng
 * @ create: 2019-09-02 18:32
 * @ desc:
 **/

public class Demo1Test {
    FileSystem fs = HDFSUtils.getFs();

    /**
     * 上传
     */
    @Test
    public void uploadInit()
            throws IOException, URISyntaxException, InterruptedException {

        fs.copyFromLocalFile(new Path("/Users/taoxuefeng/Desktop/Test.txt"),
                new Path("/wordcount/input/1.txt"));
        HDFSUtils.close(fs);
    }

    /**
     * 下载
     */
    @Test
    public void downloadInit() throws IOException, URISyntaxException, InterruptedException {

        fs.copyToLocalFile(new Path("/wordcount/input/1.txt"),
                new Path("/Users/taoxuefeng/Desktop/111.txt"));
        HDFSUtils.close(fs);
    }

    /**
     * 创建文件夹
     */
    @Test
    public void makirInit() throws IOException {
        fs.mkdirs(new Path("/testdemo"));
        HDFSUtils.close(fs);
    }
    /**
    可以获的块信息
     */
    @Test
    public void lsInit() throws IOException {
        //1、获取到要递归读取的文件目录的迭代器
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/wordcount"), true);
        //2、迭代
        while (locatedFileStatusRemoteIterator.hasNext()) {
            //3、获取文件的状态对象
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            //4、获取当前的文件的路径
            Path path = fileStatus.getPath();
            System.out.println(path.toString());
            //5、获取块信息（块位置）
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //6、遍历数组获取到块的位置信息对象
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println(Arrays.toString(blockLocation.getNames()));
            }
        }
        HDFSUtils.close(fs);
    }
    /**
    只能获取当前目录的子目录，没有递归的参数选择
     */
    @Test
    public void ls2Init() throws IOException {
        //1、获取指定目录中的文件状态对象
        FileStatus[] fileStatuses = fs.listStatus(new Path("/wordcount"));
        //2、遍历文件状态对象
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
        HDFSUtils.close(fs);
    }
    /**
     * 创建文件并写入数据
     */
    @Test
    public void touchInit() throws IOException {
        //1、创建一个文件
        //boolean newFile = fs.createNewFile(new Path("/wordcount/touchDemo.txt"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/wordcount/input/11/touchDemo.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
        bw.write("创建文件并写入测试");
        bw.flush();
        bw.close();
    }
    /**
     * 获取磁盘信息
     */
    @Test
    public void getSource() throws IOException {
        FsStatus fsStatus = fs.getStatus();
        System.out.println(fsStatus.getCapacity());
        System.out.println(fsStatus.getRemaining());
        System.out.println(fsStatus.getUsed());
    }

    /**
     * 获取虚拟机信息
     * @throws IOException
     */
    @Test
    public void getClusterNodes() throws IOException {
        //1、将普通的文件系统对象转变为分布式文件系统对象（HDFS）
        DistributedFileSystem dfs = (DistributedFileSystem) this.fs;
        //2、获取到所有的datanode信息对象
        DatanodeInfo[] dataNodeStats = dfs.getDataNodeStats();
        //3、遍历
        for (DatanodeInfo datanodeInfo : dataNodeStats) {
            System.out.println(datanodeInfo.getDfsUsed());
            System.out.println(datanodeInfo.getHostName());
            System.out.println(datanodeInfo.getAdminState());
            System.out.println(datanodeInfo.getName());
        }
    }

    /**
     * 使用流处理上传文件
     */
    @Test
    public void upload2Init() throws IOException {
        //1、获取向HDFS写数据的输出流
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/wordcount/input/load2.txt"));
        //2、读取本地的数据
        FileInputStream inputStream = new FileInputStream("");
        //3、将读取的内容用输出流按字节来写
        IOUtils.copy(inputStream, fsDataOutputStream);
    }

    /**
     * 使用流处理模拟cat操作
     * @throws IOException
     */
    @Test
    public void download2Init() throws IOException {
        //1、获取向HDFS写数据的输出流
        FSDataInputStream fsDataInputStream = fs.open(new Path("/wordcount/input/1.txt"));
        org.apache.hadoop.io.IOUtils.copyBytes(fsDataInputStream,System.out, 4096);
        fsDataInputStream.close();
    }
}
