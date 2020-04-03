package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.collections.Lists;

import java.io.IOException;
import java.util.List;

public class HBaseUtils {

    /**
     * 操作 Hbase 数据库
     * <p>
     * 1 获取连接
     * 2 获取客户端对象
     * 3 创建 myuser表，有两个列族 f1 f2
     */
    @Test
    public void createTable() throws IOException {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102:2181,hadoop103:2181,hadoop104:2181");

        //创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        //操作：建表 删除表 修改表 -》 获取管理员

        Admin admin = connection.getAdmin();
        //添加表名信息

        HTableDescriptor myuser = new HTableDescriptor(TableName.valueOf("myuser6"));

        //给表添加列族

        myuser.addFamily(new HColumnDescriptor("f1"));
        myuser.addFamily(new HColumnDescriptor("f2"));

        //创建表
        admin.createTable(myuser);

        admin.close();
        connection.close();

    }


    private Connection connection;
    private Table table;


    /**
     * 创建表后获取表的连接
     *
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        //获取连接

        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum", "hadoop102:2181,hadoop103:2181,hadoop104:2181");

        connection = ConnectionFactory.createConnection(configuration);

        Admin admin = connection.getAdmin();

        table = connection.getTable(TableName.valueOf("myuser"));
    }


    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }


    /**
     * 单个元素添加
     *
     * @throws IOException
     */
    @Test
    public void putData() throws IOException {

        Put put = new Put("0001".getBytes());

        put.addColumn("f1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
        put.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("beijing"));
        table.put(put);
    }

    /**
     * 添加list
     */
    @Test
    public void putList() throws IOException {

        Put put = new Put("0002".getBytes());
        //向f1列族添加数据
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
        //向f2列族添加数据
        put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap , show me the code"));

        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));


        List<Put> puts = Lists.newArrayList(put, put2, put3, put4, put5, put6);

        table.put(puts);
    }

    @Test
    public void getDateByRowkey() throws IOException {
        //通过get对象，指定rowKey
        Get get = new Get("0003".getBytes());


        get.addFamily("f1".getBytes());//限定查询f1列族下所有的值
        get.addColumn("f2".getBytes(), "say".getBytes());//查询 f2 列族 say 列的值
        //通过get查询，返回 0003 行，f1 列族、f2：say列的所有cell值，封装到一个 Result 对象
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        for (Cell cell : cells) {
            //cell 单元格
            //获得rowkey
            byte[] rowKey_bytes = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] cellValue = CellUtil.cloneValue(cell);

            if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {

                System.out.println(Bytes.toString(rowKey_bytes));
                System.out.println(Bytes.toString(family));
                System.out.println(Bytes.toString(qualifier));
                System.out.println(Bytes.toInt(cellValue));//age 或 id 为Int 类型
            } else {
                System.out.println(Bytes.toString(rowKey_bytes));
                System.out.println(Bytes.toString(family));
                System.out.println(Bytes.toString(qualifier));
                System.out.println(Bytes.toInt(cellValue));


            }


        }
    }

    /**
     * 查询 rowkey 范围是 0003 到 0006
     */
    @Test
    public void scanData() throws IOException {
        //没有指定 startRow 和 endRow 则进行全表扫描
        Scan scan = new Scan();
        //扫描 f1 列族
        scan.addFamily("f1".getBytes());
        //扫描 f2 列族 phone 列
        scan.addColumn("f2".getBytes(), "phone".getBytes());
        //设置起始 startRow 和 endRow
        scan.setStartRow("0003".getBytes());
        scan.setStopRow("0007".getBytes());

        //设置每批次返回客户端的数据条数
        scan.setBatch(20);
        //从cacheBlock中读取数据
        scan.setCacheBlocks(true);
        scan.setMaxResultSize(4);
        //获取历史2个版本
        scan.setMaxVersions(2);
        //通过getScanner 查询到表的所有数据 是多条数据
        ResultScanner scanner = table.getScanner(scan);
        //遍历ResultScanner 得到每一条数据，每一条都是封装在 result 对象里

        for (Result result : scanner) {
            List<Cell> cells = result.listCells();

            for (Cell cell : cells) {
                //列族
                byte[] family = CellUtil.cloneFamily(cell);
                //列
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                //rowkey
                byte[] row = CellUtil.cloneRow(cell);
                //列的值
                byte[] value = CellUtil.cloneValue(cell);
                //判断 id 和 age 字段，这两个字段是整形值
                if ("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                    System.out.println("数据的 rowkey 为" + Bytes.toString(row));
                } else {
                    System.out.println("数据的 rowkey 为" + Bytes.toString(row));

                }
            }
        }

    }


    /**
     * 查询 rowkey 比 0003 小的所有的数据
     */
    @Test
    public void rowFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("myuser"));

        Scan scan = new Scan();

        //获取比较对象
        BinaryComparator binaryComparable = new BinaryComparator("0003".getBytes());

        /**
         * rowFilter
         * 第一个参数 比较规则
         * 第二个参数 比较对象
         */
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparable);
        //设置 scan
        scan.setFilter(rowFilter);

        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }


    /**
     * 列族过滤器
     * 需要过滤 ,列族名包含 f2
     */
    @Test
    public void familyFilter() throws IOException {

        Table table = connection.getTable(TableName.valueOf("myuser"));

        Scan scan = new Scan();

        SubstringComparator comparator = new SubstringComparator("f2");

        //通过 family 选择器
        FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, comparator);

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        printlReult(scanner);
    }

    /**
     * 列名包含 name
     */
    @Test
    public void qulifierFilter() throws IOException {
        Scan scan = new Scan();

        SubstringComparator comparator = new SubstringComparator("name");

        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, comparator);

        scan.setFilter(qualifierFilter);

        ResultScanner scanner = table.getScanner(scan);

        printlReult(scanner);


    }


    private void printlReult(ResultScanner scanner) {
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断id和age字段，这两个字段是整形值
                if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toInt(value));
                } else {
                    System.out.println("数据的rowkey为" + Bytes.toString(rowkey) + "======数据的列族为" + Bytes.toString(family_name) + "======数据的列名为" + Bytes.toString(qualifier_name) + "==========数据的值为" + Bytes.toString(value));
                }
            }
        }
    }


    /**
     * 值过滤器中包含8的
     *
     * @throws IOException
     */
    @Test
    public void contains8() throws IOException {
        Scan scan = new Scan();
        SubstringComparator substringComparator = new SubstringComparator("8");
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 查询 某一列族 某一列名 下 值为 xx
     * <p>
     * 类似于 select * from myuser where name = 'xxx';
     *
     * @throws IOException
     */
    @Test
    public void singleColumnValueFilter() throws IOException {

        Scan scan = new Scan();

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "zhangsan".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);

        printlReult(scanner);

    }


    /**
     * 不等于 某个值
     *
     * @throws IOException
     */
    @Test
    public void singleColumnValueExcludeFilter() throws IOException {

        Scan scan = new Scan();

        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "zhangsan".getBytes());

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        printlReult(scanner);
    }

    /**
     * 过滤前缀为 0001 的数据
     *
     * @throws IOException
     */
    @Test
    public void prefixFilter() throws IOException {
        Scan scan = new Scan();
        //过滤 rowkey以 00 开头的数据
        PrefixFilter prefixFilter = new PrefixFilter("0001".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     * 查询 f1 列族，列为name 值为 zhangsan
     * <p>
     * rowkey前缀以 00 开头的数据
     *
     * 过滤条件累加查询
     */
    @Test
    public void filterList() throws IOException {

        Scan scan = new Scan();

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "zhangsan".getBytes());

        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());

        FilterList filterList = new FilterList();

        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printlReult(scanner);
    }

    /**
     *
     * 删除数据
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {

        Delete delete = new Delete("0003".getBytes());
        delete.addFamily("f1".getBytes());
        delete.addColumn("f2".getBytes(),"phone".getBytes());

        table.delete(delete);
    }

    /**
     * 删除表
     *
     *
     * @throws IOException
     */
    @Test
    public void deleteTable() throws IOException {
        //获取管理员对象 用于表的删除


        Admin admin = connection.getAdmin();
        //删除表之前禁用表
        admin.disableTable(TableName.valueOf("myuser"));
        admin.deleteTable(TableName.valueOf("myuser"));
    }

    /**
     * 分页过滤器
     *
     */
    @Test
    public void pageFilter () throws IOException {
        int pageNum = 4;
        int pageSize = 2;

        Scan scan = new Scan();


        if(pageNum == 1){//获取第一页数据
            scan.setMaxResultSize(pageSize);
            scan.setStartRow("".getBytes());
            //使用分页过滤器
            PageFilter pageFilter = new PageFilter(pageSize);
            scan.setFilter(pageFilter);
            ResultScanner scanner = table.getScanner(scan);
            printlReult(scanner);
        }else{

            //如果获取的不是第一页数据，先获得此分页的第一个 rowkey 的值

            String startRow = "";
            //扫描多少条
            int scanDatas = (pageNum - 1)*pageSize + 1;

            scan.setMaxResultSize(scanDatas);
            PageFilter pageFilter = new PageFilter(scanDatas);
            scan.setFilter(pageFilter);
            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                byte[] row = result.getRow();
                startRow = Bytes.toString(row);
            }
            scan.setStartRow(startRow.getBytes());
            scan.setMaxResultSize(pageSize);

            PageFilter pageFilter1 = new PageFilter(pageSize);
            scan.setFilter(pageFilter1);

            ResultScanner scanner1 = table.getScanner(scan);
            printlReult(scanner1);
        }




    }



}


