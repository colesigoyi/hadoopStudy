package cn.qf.hbase;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.junit.Test;

import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-17 14:29
 * @ desc:
 **/

public class Demo4_Filter {
    /**
     * 需求:select * from t_user where age <= 19
     */
    @Test
    public void singleColmunFilter() {
        //获取单值过滤器
        SingleColumnValueFilter singleColumnValueFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "age".getBytes(),
                        CompareFilter.CompareOp.LESS_OR_EQUAL,
                        "19".getBytes()
                );
        //如果行里没有age,就不把它认为是可以查询的
        singleColumnValueFilter.setFilterIfMissing(true);
        HBaseUtils.show(singleColumnValueFilter);
    }
    /**
     * 过滤器列
     * 需求:select * from t_user where age <= 19 and sex = 'woman'
     * FilterList.Operator
     *      MUST_PASS_ALL : and
     *      MUST_PASS_ONE : or
     */
    @Test
    public void filterList() {
        //创建过滤器列(and)
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        SingleColumnValueFilter ageFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "age".getBytes(),
                        CompareFilter.CompareOp.LESS_OR_EQUAL,
                        "19".getBytes()
                );
        SingleColumnValueFilter sexFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "sex".getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        "woman".getBytes()
                );
        //如果行里没有age,就不把它认为是可以查询的
        ageFilter.setFilterIfMissing(true);
        sexFilter.setFilterIfMissing(true);

        filterList.addFilter(ageFilter);
        filterList.addFilter(sexFilter);

        //获取scan对象
        Scan scan = new Scan();
        //只查询base_info
        scan.addFamily("base_info".getBytes());
        scan.setFilter(filterList);
        //获取到扫描器
        Table table = HBaseUtils.getTable("ns1:t_user");
        try {
            ResultScanner scanner = null;
            if (table != null) {
                scanner = table.getScanner(scan);
            }
            if (scanner != null) {
                HBaseUtils.show(scanner);
            }
            HBaseUtils.close(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     *需求:select * from t_user where name like 'li%'
     */
    @Test
    public void regexStringComparator() {
        //传建一个正则比较器
        RegexStringComparator regexStringComparator =
                new RegexStringComparator("^li.*");
        //创建单列值过滤器
        SingleColumnValueFilter regexFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "name".getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        regexStringComparator
                );
        //如果行里没有age,就不把它认为是可以查询的
        regexFilter.setFilterIfMissing(true);
        HBaseUtils.show(regexFilter);
    }

    /**
     * 子串比较器
     * 名字中带a
     */
    @Test
    public void subStringComparator() {
        //传建一个正则比较器
        SubstringComparator subStringComparator =
                new SubstringComparator("a");
        //创建单列值过滤器
        SingleColumnValueFilter subFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "name".getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        subStringComparator
                );
        //如果行里没有age,就不把它认为是可以查询的
        subFilter.setFilterIfMissing(true);
        HBaseUtils.show(subFilter);
    }
    /**
     * 二进制比较器
     */
    @Test
    public void binaryComparator() {
        //传建一个正则比较器
        BinaryComparator binaryComparator =
                new BinaryComparator("lili".getBytes());
        //创建单列值过滤器
        SingleColumnValueFilter binaryFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "name".getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        binaryComparator
                );
        //如果行里没有age,就不把它认为是可以查询的
        binaryFilter.setFilterIfMissing(true);
        HBaseUtils.show(binaryFilter);
    }


    /**
     * 列簇过滤器
     */
    @Test
    public void familyFilter() {
        //传建一个正则比较器
        BinaryComparator binaryComparator =
                new BinaryComparator("base_info".getBytes());
        //创建单列值过滤器
        FamilyFilter familyFilter =
                new FamilyFilter(
                        CompareFilter.CompareOp.EQUAL,
                        binaryComparator
                );
        HBaseUtils.show(familyFilter);
    }
    /**
     * qualifierFilter
     *列过滤器
     */
    @Test
    public void qualifierFilter() {
        //传建一个正则比较器
        BinaryComparator binaryComparator =
                new BinaryComparator("name".getBytes());
        //创建单列值过滤器
        QualifierFilter qualifierFilter =
                new QualifierFilter(
                        CompareFilter.CompareOp.EQUAL,
                        binaryComparator
                );
        HBaseUtils.show(qualifierFilter);
    }
    /**
     * ColumnPrefixFilter
     * 列前缀过滤器
     */
    @Test
    public void columnPrefixFilter() {
        ColumnPrefixFilter columnPrefixFilter =
                new ColumnPrefixFilter("na".getBytes());
        HBaseUtils.show(columnPrefixFilter);
    }
    /**
     * MultipleColumnPrefixFilter
     */
    @Test
    public void multipleColumnPrefixFilter() {
        byte[][] prefixes = {"na".getBytes(),"se".getBytes()};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter =
                new MultipleColumnPrefixFilter(prefixes);
        HBaseUtils.show(multipleColumnPrefixFilter);
    }

    /**
     * ColumnRangeFilter
     * 列范围过滤器
     */
    @Test
    public void columnRangeFilter() {
        ColumnRangeFilter columnRangeFilter =
                new ColumnRangeFilter("age".getBytes(),true,
                        "name".getBytes(),true);
        HBaseUtils.show(columnRangeFilter);
    }

    /**
     * 行键过滤器
     */
    @Test
    public void rowKey() {
        BinaryComparator binaryComparator =
                new BinaryComparator("001".getBytes());
        RowFilter rowFilter = new RowFilter(
                CompareFilter.CompareOp.EQUAL,
                binaryComparator
        );
        HBaseUtils.show(rowFilter);
    }
    /**
     * FirstKeyOnlyFilter
     * 每一行第一列范围过滤器
     */
    @Test
    public void firstKeyOnlyFilter() {
        FirstKeyOnlyFilter firstKeyOnlyFilter =
                new FirstKeyOnlyFilter();
        HBaseUtils.show(firstKeyOnlyFilter);
    }

}
