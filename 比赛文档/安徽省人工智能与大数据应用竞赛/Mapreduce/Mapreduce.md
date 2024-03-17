### 各年省赛题目文件链接

链接：https://pan.baidu.com/s/1gNsHPSvegNly-s0b_DJicA?pwd=ztpj 
提取码：ztpj 
--来自百度网盘超级会员V4的分享

### Mapreduce基本原理

MapReduce，其实从运行来说，就是分为大的两个阶段的，一个阶段是MapTask（将大任务拆分为小任务），第二个阶段是ReduceTask（小任务计算结果重组），两个阶段之间，有个Shuffle的过程。

MapTask
整个MapTask分为Read阶段，Map阶段，Collect阶段，溢写（spill）阶段和combine阶段。
Read阶段：MapTask通过用户编写的RecordReader，从输入InputSplit中解析出一个个key/value；
Map阶段：该节点主要是将解析出的key/value交给用户编写map()函数处理，并产生一系列新的key/value；
Collect收集阶段：在用户编写map()函数中，当数据处理完成后，一般会调用OutputCollector.collect()输出结果。在该函数内部，它会将生成的key/value分区（调用Partitioner），并写入一个环形内存缓冲区中；
Spill阶段：即“溢写”，当环形缓冲区满后，MapReduce会将数据写到本地磁盘上，生成一个临时文件。需要注意的是，将数据写入本地磁盘之前，先要对数据进行一次本地排序，并在必要时对数据进行合并、压缩等操作；



Shuffle
Map方法之后，Reduce方法之前的数据处理过程称之为Shuffle。shuffle流程如下：
MapTask收集map()方法输出的kv对，放到环形缓冲区中；
从环形缓冲区不断溢出到本地磁盘文件，可能会溢出多个文件；
多个溢出文件会被合并成大的溢出文件；
在溢出过程及合并的过程中，都要调用Partitioner进行分区和针对key进行排序；ReduceTask根据自己的分区号，去各个MapTask机器上取相应的结果分区数据；
ReduceTask将取到的来自同一个分区不同MapTask的结果文件进行归并排序；
合并成大文件后，shuffle过程也就结束了，进入reduce方法。



ReduceTask
整个ReduceTask分为Copy阶段，Merge阶段，Sort阶段（Merge和Sort可以合并为一个），Reduce阶段。
Copy阶段：ReduceTask从各个MapTask上远程拷贝一片数据，并针对某一片数据，如果其大小超过一定阈值，则写到磁盘上，否则直接放到内存中；
Merge阶段：在远程拷贝数据的同时，ReduceTask启动了两个后台线程对内存和磁盘上的文件进行合并，以防止内存使用过多或磁盘上文件过多；
Sort阶段：按照MapReduce语义，用户编写reduce()函数输入数据是按key进行聚集的一组数据。为了将key相同的数据聚在一起，Hadoop采用了基于排序的策略。由于各个MapTask已经实现对自己的处理结果进行了局部排序，因此，ReduceTask只需对所有数据进行一次归并排序即可；
Reduce阶段：reduce()函数将计算结果写到HDFS上。



### 安装maven/创建maven项目

详见maven文档



### pom依赖文件添加内容

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231115185950.png)

```java
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>Bigdata</artifactId>
    <version>1.0-SNAPSHOT</version>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <dependencies>
        <!-- https://mvnrepository.com/artifact/junit/junit -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.6</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.6</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.6</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.7.6</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>2.7</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-api</artifactId>
            <version>RELEASE</version>
            <scope>compile</scope>
        </dependency>
    </dependencies>


</project>
```





### Mapreduce本地模式运行设置

//是否运行为本地模式，就是看这个参数值是否为local，默认就是local
conf.set("mapreduce.framework.name", "local");



### Mapreduce本地运行设置日志

在resources文件夹内创建log4j.properties文件，加入如下内容

//返回所有运行信息

```java
log4j.rootLogger=info, stdout  
log4j.appender.stdout=org.apache.log4j.ConsoleAppender  
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout  
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n  

log4j.appender.File=org.apache.log4j.DailyRollingFileAppender
log4j.appender.File.File=D:/test/logs.txt
log4j.appender.File.DatePattern='_'yyyy-MM-dd'.txt'
log4j.appender.File.layout=org.apache.log4j.PatternLayout
log4j.appender.File.layout.ConversionPattern=%d %t %-5p [%c] %m%n
```

//返回错误信息

```java
log4j.rootLogger=error, stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d %p [%c] - %m%n
log4j.appender.logfile=org.apache.log4j.FileAppender
log4j.appender.logfile.File=target/spring.log
log4j.appender.logfile.layout=org.apache.log4j.PatternLayout
log4j.appender.logfile.layout.ConversionPattern=%d %p [%c]-%m%n
```



### Mapreduce 两种join操作

#### 1.reduce端join

通过读取文件的文件名区分文件，将多个文件通过相同的key进行聚合。再从reduce端拼接。效率低。

#### 2.map端join

优点：对于数据量特别大的表和数据量较小的表进行join效率很高

核心：分布式缓存机制（Distribute Cache）

通过将小文件放到缓存区，可以在每次map任务开始前读取文件的内容

数据不需要在shuffle阶段进行集合，提高了效率。



**只能在集群模式运行，且需要在代码中设置缓存区的相对路径**

**在本地模式下，可以使用HashMap保存文件的字段，然后根据key值进行拼接。**





### Mapreduce编程案例

#### 订单信息处理（2020安徽省大数据决赛赛题）

##### 数据

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20230927171152.png)

##### 题目一

对订单数据中的订单日期进行格式化，并派生出年，月，日三个字段，对订单数据和退货数据两个数据进行join，合并后数据 在订单数据基础上增加 是否退货一列，并把合并后的数据输出到 dsjjs2020a

```java
package mapreduce2020;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class OrderFormat{
    public static class OrderMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        // 创建HashMap存储数据
        HashMap<String, String> return_map = new HashMap<>();

        // 初始化 读取文件
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // 读取文件
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream open = fs.open(new Path("file:///D:\\data\\input\\return.csv"));
            BufferedReader br = new BufferedReader(new InputStreamReader(open));
            String line;
            while(br.readLine()!=null){
                try {
                    line = br.readLine();
                    String[] split = line.split(",");
                    String order_id = split[0];
                    String return_or_not = split[1];
                    return_map.put(order_id,return_or_not);
                } catch (Exception e){
                        System.out.println(e);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split(",");
            String order_id = split[0];
            // 获取并转换日期，获取年 月 日
            String date = split[1];
            String[] dates = date.split("/");
            String year = dates[0];
            String month = dates[1];
            String day = dates[2];
            String province = split[2];
            String product_id = split[3];
            String type = split[4];
            String sale = split[5];
            String amount = split[6];
            String discount = split[7];
            String profit = split[8];
            String return_or_not = return_map.getOrDefault(order_id, "无信息");
            //写入上下文对象
            String result = order_id + "," + year + "," + month + "," + day + "," + province + ","
                    + product_id + "," + type + "," + sale + "," + amount + "," + discount + "," +
                    profit + "," + return_or_not;
            context.write(new Text(result),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "OrderFormat");
        job.setJarByClass(OrderFormat.class);

        job.setMapperClass(OrderMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        Path input_path = new Path("file:///D:\\data\\input\\Order.csv");
        Path output_path = new Path("file:///D:\\data\\output\\output8");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output_path)){
            fileSystem.delete(output_path,true);
        }
        TextInputFormat.addInputPath(job,input_path);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,output_path);

        job.waitForCompletion(true);
    }

}

```

![1697539998710](C:\Users\顾宇阳\AppData\Roaming\Typora\typora-user-images\1697539998710.png)

##### 题目二

计算order.csv中 各省的总销售额和总利润 要求销售额降序

返回格式（省，销售额，利润）

// 分成两步解决（因为要排序的是销售额，需要以销售额作为key来自定义排序方法。）

1.先将各省的总销售额和总利润计算出来，输出到一个文件里。

```java
package mapreduce2020;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.wml.dom.WMLAnchorElementImpl;

import java.io.IOException;

public class Totalsales {
    public static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String province = split[2];
            String sale = split[5];
            String profit = split[8];
            context.write(new Text(province),new Text(sale + "," + profit));
        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sale_total=0;
            double profit_total=0;
            for (Text value : values) {
                String[] split = value.toString().split(",");
                String sale = split[0];
                String profit = split[1];
                sale_total += Double.parseDouble(sale);
                profit_total += Double.parseDouble(profit);
            }
            String result = key.toString() + "," + sale_total + "," +profit_total;
            context.write(new Text(result),NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Totalsales");
        job.setJarByClass(Totalsales.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path input = new Path("file:///D:\\data\\input\\Order.csv");
        Path output = new Path("file:///D:\\data\\output\\output9");
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }


}

```

![1697539965234](C:\Users\顾宇阳\AppData\Roaming\Typora\typora-user-images\1697539965234.png)

2.读取这个文件，将（省，销售额，利润）封装成一个JavaBean，然后重写排序规则。

```java
package mapreduce2020;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderCount {
    public static class OrderBean implements WritableComparable<OrderBean>{
        private String province;
        private double sale;
        private double profit;

        public OrderBean() {
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public double getSale() {
            return sale;
        }

        public void setSale(double sale) {
            this.sale = sale;
        }

        public double getProfit() {
            return profit;
        }

        public void setProfit(double profit) {
            this.profit = profit;
        }

        @Override
        public int compareTo(OrderBean o) {
            //根据销售额降序排列
            double result = this.sale - o.getSale();
            if (result>0){
                return -1;
            }else if (result<0){
                return 1;
            }else return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(province);
            dataOutput.writeDouble(sale);
            dataOutput.writeDouble(profit);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.province = dataInput.readUTF();
            this.sale = dataInput.readDouble();
            this.profit = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return province + "," + sale + "," + profit;
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String province = split[0];
            String sale = split[1];
            String profit = split[2];
            OrderBean orderBean = new OrderBean();
            orderBean.setProvince(province);
            orderBean.setSale(Double.parseDouble(sale));
            orderBean.setProfit(Double.parseDouble(profit));
            context.write(orderBean,NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, " ");
        job.setJarByClass(OrderCount.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path input = new Path("file:///D:\\data\\output\\output9\\part-r-00000");
        Path output = new Path("file:///D:\\data\\output\\output10");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }

}
```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231017185156.png)

##### 题目三

统计order.csv中 每个类别利润最好的前3条记录，信息

返回格式（订单日期，产品id，类别，销售额，利润）

思路：以利润排序——》封装JavaBean对象——》首先按同类排序，相同类型再按利润排序。

​			以类别分组——》相同类别的分为一组，组内顺序为按照利润排序，

​											输出前三。

 ```java
package mapreduce2020;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 封装对象，序列化，重写排序方法
public class OrderTop3 {
    public static class SaleBean implements WritableComparable<SaleBean>{
        private String type;
        private double profit;

        public SaleBean() {
        }

        public SaleBean(String type, double profit) {
            this.type = type;
            this.profit = profit;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public double getProfit() {
            return profit;
        }

        public void setProfit(double profit) {
            this.profit = profit;
        }

        @Override
        public int compareTo(SaleBean o) {
            // 先按照类型排序
            int result;
            result = this.type.compareTo(o.getType());
            // 根据利润倒序排序
            if (result==0){
                if (this.profit - o.getProfit()<0){
                    result = 1;
                }else
                    result = -1;
            }
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(type);
            dataOutput.writeDouble(profit);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.type = dataInput.readUTF();
            this.profit = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return type + "," + profit;
        }
    }

    // 重写分组方法，把类别相同的分为一组
    public static class MyGrouping extends WritableComparator{
        // 获取父类的sortBean
        protected MyGrouping(){
            super(SaleBean.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SaleBean aBean = (SaleBean)a;
            SaleBean bBean = (SaleBean)b;
            // 类型相同的分为一组
            return aBean.getType().compareTo(bBean.getType());
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text,SaleBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String type = split[4];
            String profit = split[8];
            SaleBean saleBean = new SaleBean();
            saleBean.setType(type);
            saleBean.setProfit(Double.parseDouble(profit));
            context.write(saleBean,NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<SaleBean,NullWritable,SaleBean,NullWritable>{
        // 取出前三条 输出

        @Override
        protected void reduce(SaleBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (NullWritable value : values) {
                if (count<3){
                    context.write(key,NullWritable.get());
                    count++;
                }
                else return;
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "");
        job.setJarByClass(OrderTop3.class);
        job.setMapperClass(MyMapper.class);
        job.setGroupingComparatorClass(MyGrouping.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(SaleBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(SaleBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path input = new Path("file:///D:\\data\\input\\Order.csv");
        Path output = new Path("file:///D:\\data\\output\\output11");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);
        job.waitForCompletion(true);

    }
}

 ```



#### 通话信息处理（2021安徽省大数据网络赛赛题）

##### 数据

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20230921221341.png)

##### 题目一

1.**请根据要求把** 通话记录表 转换为 新的格式数据。

要求把 通话记录表 呼叫者手机号,接受者手机号 替换为 姓名，

开始时间与结束时间 转换成时间格式为 yyyy-MM-dd HH:mm:ss，例如2017-03-29 10:58:12；

计算通话时间，并以秒做单位 计算为 **通话时间=结束时间-开始时间**

将呼叫者地址省份编码,接受者地址省份编码 替换成省份名称

 

**1.** **将电话号码替换成人名**

**2.** **将拨打、接听电话的时间戳转换成日期**

**3.** **求出电话的通话时间，以秒做单位**

**4.** **将省份编码替换成省份名称**

**5.** **最后数据的样例:**

邓二,张倩,,2018-03-29 10:58:12,2018-03-29 10:58:42,30秒,黑龙江省,上海市



```java
package mapreduce_calls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;

public class calls_format {

    //编写Map阶段代码
    public static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        // 将userPhone.txt文件数据写入缓存
        // setup在每次map任务开始前执行一次
        // 使用HashMap存放 电话号码 姓名
        HashMap<String, String> user_Phone = new HashMap<String, String>();
        // 使用HashMap存放 省份编码 省份名称
        HashMap<String, String> location = new HashMap<String, String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 读取文件（本地）
            // 获取文件系统 读取文件
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fsDataInputStream = fs.open(new Path("file:///D:\\data\\input\\userPhone.txt"));
            // 获取文件缓冲区 将FSDataInputStream 转换为BufferedReader
            BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
            // 按行读取 写入hashtable
            String line = null;
            while (br.readLine()!=null){
                line = br.readLine();
                String[] split = line.split(",");
                String phoneNum = split[1];
                String name = split[2];
                user_Phone.put(phoneNum,name);
            }
            // 获取省份编码 省份名称
            FSDataInputStream fsDataInputStream1 = fs.open(new Path("file:///D:\\data\\input\\location.txt"));
            BufferedReader br1 = new BufferedReader(new InputStreamReader(fsDataInputStream1));
            line = null;
            while (br1.readLine()!=null){
                line = br1.readLine();
                String[] split = line.split(",");
                String provinceID = split[1];
                String provinceName = split[2];
                location.put(provinceID,provinceName);
            }

            // 读取文件（Hdfs）
            //BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("userPhone.txt")));
            // Hdfs读取文件需要在main中添加 job.addCacheFile(new URI(new Path("输入缓存文件存放的路径")))

        }
        // 重写map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            样例：18620192711,15733218050,1506628174,1506628265,650000,810000
            字段分别为:
            呼叫者手机号,接受者手机号,开始时间戳,结束时间戳,呼叫者地址省份编码,接受者地址省份编码
             */
            String[] split = value.toString().split(",");
            String receiverName = split[1];
            String callerName = split[0];
            String timeStart = split[2];
            String timeEnd = split[3];
            String callerPName = split[4];
            String receiverPName = split[5];
            long lastTime = 0;

            //1.将电话号码替换成人名
//            callerName = user_Phone.get(callerName);
                //由于有不存在的人名，可以设置一个默认值
            callerName = user_Phone.getOrDefault(callerName,"姓名为空");
//            receiverName = user_Phone.get(receiverName);
            receiverName = user_Phone.getOrDefault(receiverName,"姓名为空");

            //2.将拨打、接听电话的时间戳转换成日期
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String timeStartFormat = simpleDateFormat.format(new Date(Long.parseLong(timeStart)));
            String timeEndFormat = simpleDateFormat.format(new Date(Long.parseLong(timeEnd)));

            //3.求出电话的通话时间，以秒做单位
            lastTime = Long.parseLong(timeEnd) - Long.parseLong(timeStart);

            //4.将省份编码替换成省份名称
            callerPName = location.getOrDefault(callerPName,"省份为空");
            receiverPName = location.getOrDefault(receiverPName,"省份为空");

            //5.格式化输出结果
            context.write(
                    new Text(callerName+","
                            +receiverName+","
                            +timeStartFormat+","
                            +timeEndFormat+","
                            +lastTime+"秒"+","
                            +callerPName+","
                            +receiverPName),NullWritable.get());
        }
    }

    // 编写Reduce阶段代码(由于不需要shuffle阶段，所以不需要reduce)
    // 编写main方法
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "callformat");
        job.setJarByClass(calls_format.class);

        // 设置输入输出路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\data\\input\\calls.txt"));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\data\\output\\output3"));

        //设置 map reduce输入输出类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);

        //设置 reduceTask个数 由于没有reduceTask 设置为0 ！！！
        job.setNumReduceTasks(0);

        //等待任务完成
        job.waitForCompletion(true);
    }

}

```

结果文件截图

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20230921232100.png)

##### 题目二

请使用MapReduce统计 calls.txt中的每个手机号码的，呼叫时长和呼叫次数，被叫时长，被叫次数 ，并输出格式 为 手机号码，呼叫时长，呼叫次数，被叫时长，被叫次数

数据格式样例：其中在呼叫时长后面加单位 秒 ；呼叫次数后面加 单位 次；被叫时长后面加单位 秒 ；被叫次数后面加 单位 次

13269361119,65秒,5次,864秒,5次

```java
package mapreduce_calls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class callstotal {
    // 编写Map阶段代码
    public static class callMapper extends Mapper<LongWritable, Text,Text,Text>{
        /*
        18620192711,15733218050,1506628174,1506628265,650000,810000
        呼叫着手机号  接收者手机号    开始时间戳     结束……    呼叫者省份编码 接收者省份编码
         */
        /*
        手机号码，呼叫时长，呼叫次数，被叫时长，被叫次数
         */

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            // 取出呼叫者手机号
            String caller = split[0];
            // 取出接受者手机号
            String receiver = split[1];
            // 取出开始时间
            String start_time = split[2];
            // 取出结束时间
            String end_time = split[3];
            // 计算呼叫时间和被叫时间 (呼叫者呼叫时间 即 被叫者被叫时间)
            long time = Long.parseLong(end_time) - Long.parseLong(start_time);
            // 写入上下文
            context.write(new Text(caller),new Text(time + "," + "caller"));
            context.write(new Text(receiver),new Text(time + "," + "receiver"));
        }
    }

    // 编写Reduce阶段代码
        public static class callReducer extends Reducer<Text,Text,Text, NullWritable>{
        // 重写reduce阶段代码

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int callercount = 0;
            int receivercount = 0;
            int callertime = 0;
            int receivertime = 0;
            for (Text value : values) {
                String[] split = value.toString().split(",");
                if ("caller".equals(split[1])){
                    callertime += Integer.parseInt(split[0]);
                    callercount ++;
                }
                if ("receiver".equals(split[1])){
                    receivertime += Integer.parseInt(split[0]);
                    receivercount ++;
                }
            }
            context.write(new Text(key.toString() + "," + callertime + "," + callercount + "," + receivertime + "," +
                    receivercount),NullWritable.get());
        }
    }

    // 编写job代码
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "callstotal");
        job.setJarByClass(callstotal.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\data\\input\\calls.txt"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\data\\output\\output4"));

        job.setMapperClass(callMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(callReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);

    }
}

```

结果截图：

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20230922170147.png)

##### 题目三

**请使用MapReduce统计 calls.txt**中的 被叫省份中 被叫次数最高的前三条记录

返回格式：省 ，被叫号码，被叫次数

思路：

​			1.要求被叫次数，需要将每条CallsBean的记录按被叫号码分组后统计出来——》（省，被叫号码，被叫次数）

​			2.要求TopN问题，必须自定义排序方法和分组方法

​			封装一个CallsBean（省，被叫号码，被叫次数）

即分为两个mapreduce任务来解决

1. 输出 省份名称 + 被叫号码 + 被叫次数

```java
package mapreduce_calls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class receiver_count {
    
    public static class countMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        HashMap<String,String> map = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream open = fs.open(new Path("file:///D:\\data\\input\\location.txt"));
            BufferedReader br = new BufferedReader(new InputStreamReader(open));
            String line = null;
            while (br.readLine()!=null){
                line = br.readLine();
                if (line==null)
                    continue;
                String[] split = line.split(",");
                // 获取省份编码
                String province_code = split[1];
                // 获取省份名称
                String province_name = split[2];
                map.put(province_code,province_name);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String receiver_Phonenumber = split[1];
            String receiver_province_code = split[5];
            // 通过省份编码取出省份名称
            String province_name = map.getOrDefault(receiver_province_code, "无省份编码");
            // 写入上下文对象
            context.write(new Text(receiver_Phonenumber + "," + province_name),new LongWritable(1));

        }
    }

    public static class callsReducer extends Reducer<Text, LongWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable value : values) {
                count += value.get();
            }
            String result = key.toString() + "," + count;
            context.write(new Text(result),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(receiver_count.class);

        job.setMapperClass(countMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(callsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path input = new Path("file:///D:\\data\\input\\calls.txt");
        Path output = new Path("file:///D:\\data\\output\\output12");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}
```

2.按照省份分组，按照被叫次数排序

```java
package mapreduce_calls;

import mapreduce_stuscore_topclass.CourseBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class callsTop {
    // 封装对象
    public static class CallsBean implements WritableComparable<CallsBean>{
        private String province_name;
        private String call_receiver;
        private long count_call;

        @Override
        public int compareTo(CallsBean o) {
            // 先比较省份名称
            int result = 0;
            result = this.province_name.compareTo(o.province_name);
            // 如果省份名称相同，按照被叫次数降序排列
            if (result==0){
                if (this.count_call - o.count_call<0){
                    return 1;
                }
                else return -1;
            }
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(province_name);
            dataOutput.writeUTF(call_receiver);
            dataOutput.writeLong(count_call);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.province_name = dataInput.readUTF();
            this.call_receiver = dataInput.readUTF();
            this.count_call = dataInput.readLong();
        }

        @Override
        public String toString() {
            return province_name + "," +
                    call_receiver + "," +
                     count_call;
        }

        public CallsBean() {
        }

        public String getProvince_name() {
            return province_name;
        }

        public void setProvince_name(String province_name) {
            this.province_name = province_name;
        }

        public String getCall_receiver() {
            return call_receiver;
        }

        public void setCall_receiver(String call_receiver) {
            this.call_receiver = call_receiver;
        }

        public long getCount_call() {
            return count_call;
        }

        public void setCount_call(long count_call) {
            this.count_call = count_call;
        }
    }

    // 重写分组方法
    public static class callsGrouping extends WritableComparator{
        // 继承父类获取对象 true表示允许创建实例
        protected callsGrouping(){super(CallsBean.class,true);}
        // 重写compare方法

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            // 类型转换
            CallsBean aBean = (CallsBean) a;
            CallsBean bBean = (CallsBean) b;
            // 比较 相同省名分为一组
            return aBean.getProvince_name().compareTo(bBean.getProvince_name());
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text,CallsBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String province_name = split[1];
            String receiver_phonenumber = split[0];
            String count_calls = split[2];
            CallsBean callsBean = new CallsBean();
            callsBean.setProvince_name(province_name);
            callsBean.setCall_receiver(receiver_phonenumber);
            callsBean.setCount_call(Long.parseLong(count_calls));
            context.write(callsBean,NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<CallsBean,NullWritable,CallsBean,NullWritable>{
        @Override
        protected void reduce(CallsBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int count=0;
            // 迭代对象，获取前三
            for (NullWritable value : values) {
                if (count<3){
                    context.write(key,NullWritable.get());
                    count++;
                }else return;
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "");
        job.setJarByClass(callsTop.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(CallsBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(callsGrouping.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(CallsBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path input = new Path("file:///D:\\data\\output\\output12\\part-r-00000");
        Path output = new Path("file:///D:\\data\\output\\output13");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}

```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018092449.png)



#### 贷款数据处理（2021年安徽省大数据与人工智能应用竞赛）

##### 	数据

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018093109.png)

##### 	题目一

请使用mapreduce 对数据进行处理，贷款金额进行汇率转化，贷款余额查询处理，并把结果保存为 Foreign_chang文件。

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018093828.png)

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018093557.png)

思路：

###### map join方式

1.将币种id替换为币种名称——》map端join或者reduce端join	此处为了方便使用map端join

2.获取币种id对应的汇率	计算贷款金额_人民币，贷款余额 _人民币等信息

​	用两个HashMap<>	key:币种id	value:币种名称

​										 key:币种id	value:对应汇率

```java
package mapreduce_huilv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class huilvFormat {
    public static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        // 名称Hashmap
        HashMap<String,String> current_name_map = new HashMap<>();
        //  汇率Hashmap
        HashMap<String,Double> current_huilv_map = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fsDataInputStream = fs.open(new Path("file:///D:\\data\\input\\huilv.csv"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line;
            line = br.readLine();
            while (line!=null){
                line = br.readLine();
                if (line==null){
                    return;
                }
                String[] split = line.split(",");
                String current_id = split[0];
                String current_huilv = split[1];
                String current_name = split[2];
                current_huilv_map.put(current_id,Double.parseDouble(current_huilv));
                current_name_map.put(current_id,current_name);
            }
            System.out.println(current_name_map);
            System.out.println(current_huilv_map);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("0")){
                return;
            }
            String[] split = value.toString().split(",");
            String project_id = split[0];
            String industry = split[1];
            String project_type = split[2];
            String current_id = split[3];
            double loan = Double.parseDouble(split[4]);
            double loan_got = Double.parseDouble(split[5]);
            double loan_paid = Double.parseDouble(split[6]);
            double interest = Double.parseDouble(split[7]);
            // 获取币种名称
            String current_name = current_name_map.getOrDefault(current_id,"无id");
            System.out.println(current_name);
            // 获取汇率
            Double current_huilv = current_huilv_map.getOrDefault(current_id,1.0);

            //计算贷款余额
            double loan_remained = loan_got - loan_paid;


            //计算 贷款金额 实际提款额 已还本金额 利息金额 贷款余额 的rmb形式
            double loan_rmb = loan * current_huilv;
            double loan_got_rmb = loan_got * current_huilv;
            double loan_paid_rmb = loan_paid * current_huilv;
            double interest_rmb = interest * current_huilv;
            double loan_remained_rmb = loan_remained * current_huilv;

            //拼接字符串 输出
            ArrayList<String> result_list = new ArrayList<>();
            result_list.add(project_id);
            result_list.add(industry);
            result_list.add(project_type);
            result_list.add(current_name);  // 在这里用币种名称代替币种id
            result_list.add(Double.toString(loan));
            result_list.add(Double.toString(loan_rmb));
            result_list.add(Double.toString(loan_got));
            result_list.add(Double.toString(loan_got_rmb));
            result_list.add(Double.toString(loan_paid));
            result_list.add(Double.toString(loan_paid_rmb));
            result_list.add(Double.toString(interest));
            result_list.add(Double.toString(interest_rmb));
            result_list.add(Double.toString(loan_remained));
            result_list.add(Double.toString(loan_remained_rmb));
            String result = String.join(",",result_list);

            context.write(new Text(result),NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //连接集群
        Configuration conf = new Configuration();

        //构建job任务
        Job job = Job.getInstance(conf, "");

        //Map端
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //Reduce端
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        Path input = new Path("file:///D:\\data\\input\\Foreign_Government_Loans.csv");
        Path output = new Path("file:///D:\\data\\output\\output14");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);


        //提交任务
        job.waitForCompletion(true);
    }
}

```

###### reduce join方式

思路：用文件名区分两个文件，将币种id作为key来map

​			将map的value加上标识符来标识其来自哪个文件

​			（key，#文本1）或（key，@文本2）

​			reduce聚合后结果为

​			（key，文本1，文本2）或（key，文本2，文本1）

​			取出文本1和2内相应的字段，进行拼接输出。

```java
package mapreduce_huilv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Vector;

public class huilv {
    // Map类
    public static class Map extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fileSplit = (FileSplit)inputSplit;
            String name = fileSplit.getPath().getName();
            //去除第一行
            if (key.toString().equals("0")){
                return;
            }
            if ("Foreign_Government_Loans.csv".equals(name)){
                String[] split = value.toString().split(",");
                String bzid = split[3];
                context.write(new Text(bzid),new Text("#"+value.toString()));
            }else {
                String[] split = value.toString().split(",");
                String bzid = split[0];
                context.write(new Text(bzid),new Text("@" + value.toString()));
            }
        }
    }


    // Reduce类
    public static class Reduce extends Reducer<Text,Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> va = new Vector<String>();
            Vector<String> vb = new Vector<String>();

            for (Text value : values) {
                if (value.toString().startsWith("#")){
                    va.add(value.toString().substring(1));
                }else {
                    vb.add(value.toString().substring(1));
                }
            }

            for (int i=0;i<va.size();i++){
                for (int j=0;j<vb.size();j++){
                    String s = va.get(i);
                    String s1 = vb.get(j);

                    //切分提取
                    String[] split = s.split(",");
                    String[] split1 = s1.split(",");
                    //切分文件一
                    String xmxh = split[0];
                    String hy = split[1];
                    String xmlb = split[2];
                    String bzid = split[3];
                    double dkje = Double.parseDouble(split[4]);
                    double sjtke = Double.parseDouble(split[5]);
                    double yhbj = Double.parseDouble(split[6]);
                    double lxje = Double.parseDouble(split[7]);
                    //切分文件二
                    String bzmc = split1[2];
                    double hl = Double.parseDouble(split1[1]);
                    //汇率转换
                    double dkje_rmb = dkje * hl;
                    double sjtke_rmb = sjtke * hl;
                    double dkye = sjtke - yhbj;
                    double yhbj_rmb = yhbj * hl;
                    double lxje_rmb = lxje * hl;
                    double dkye_rmb = dkye * hl;
                    //拼接字符串
                    String out = xmxh + "," + hy + "," + bzmc + "," + dkje + "," + dkje_rmb + "," + sjtke + "," + sjtke_rmb
                            + "," + yhbj + "," + yhbj_rmb + "," + lxje_rmb + "," + dkye + "," + dkye_rmb;


                    context.write(new Text(out),NullWritable.get());
                }
            }
        }
    }

    //jobMain
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //连接集群
        Configuration conf = new Configuration();

        //构建job任务
        Job job = Job.getInstance(conf, "huilv");

        //Map端
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Reduce端
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //添加文件的输入路径
        FileInputFormat.addInputPath(job,new Path("file:///D:\\data\\input\\huilv"));

        //添加文件的输出路径
        FileOutputFormat.setOutputPath(job,new Path("file:///D:\\data\\output\\output1"));


        //提交任务
        job.waitForCompletion(true);
    }

}

```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018195708.png)

##### 	题目二

请使用mapreduce对 贷款表Foreign_Government_Loans.csv中的贷款异常数据 处理，并根据不同的规则增加不同的标识

异常数据判断规则 
规则1）实际提款金额>贷款金额 并对数据 增加一个标识 “超额提款” 
规则2）本金已经还完，还产生利息的数据 增加一个标识 “超额付息”
	实际提款金额-已还本金额=0 但是 利息金额>0 的数据也是异常数据。



思路：直接在map端逐行读取数据，计算后拼接输出即可。

​			也可在reduce端计算——》好处是减少资源浪费，增加计算速度。

```java
package mapreduce_huilv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class huilvError {
    public static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //去除第一行
            if (key.toString().equals("0")){
                return;
            }
            String[] split = value.toString().split(",");
            String project_id = split[0];
            String industry = split[1];
            String project_type = split[2];
            String current_id = split[3];
            double loan = Double.parseDouble(split[4]);
            double loan_got = Double.parseDouble(split[5]);
            double loan_paid = Double.parseDouble(split[6]);
            double interest = Double.parseDouble(split[7]);
            //把错误标识设置为正常
            String errorflag = "正常";
            // 判断是否超额提款
            if (loan_got>loan){
                errorflag = "超额提款";
            }
            // 判断是否超额付息
            if (loan_got-loan_paid==0 && interest>0){
                errorflag = "超额付息";
            }

            //拼接字符串 输出
            ArrayList<String> result_list = new ArrayList<>();
            result_list.add(project_id);
            result_list.add(industry);
            result_list.add(project_type);
            result_list.add(Double.toString(loan));
            result_list.add(Double.toString(loan_got));
            result_list.add(Double.toString(loan_paid));
            result_list.add(Double.toString(interest));
            result_list.add(errorflag);

            String result = String.join(",",result_list);

            context.write(new Text(result),NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //连接集群
        Configuration conf = new Configuration();

        //构建job任务
        Job job = Job.getInstance(conf, "");

        //Map端
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //Reduce端
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        Path input = new Path("file:///D:\\data\\input\\Foreign_Government_Loans.csv");
        Path output = new Path("file:///D:\\data\\output\\output15");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);


        //提交任务
        job.waitForCompletion(true);
    }
}

```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018195620.png)

##### 	题目三

请使用MapReduce统计Foreign_Government_Loans表中的 项目类别中 哪三个贷款额最高的项目

返回格式：项目类别 ，币种ID，贷款额合计

思路：TopN问题 ——》自定义分组

​			排序问题——》自定义排序

以类别分组 以贷款额排序即可

```java
package mapreduce_huilv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LoanTop3 {

    public static class LoanBean implements WritableComparable<LoanBean> {
        private String project_type;
        private String id;
        private double loan;

        public LoanBean() {
        }

        public String getProject_type() {
            return project_type;
        }

        public void setProject_type(String project_type) {
            this.project_type = project_type;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public double getLoan() {
            return loan;
        }

        public void setLoan(double loan) {
            this.loan = loan;
        }

        public int compareTo(LoanBean o) {
            int result = 0;
            //对类别排序
            result = project_type.compareTo(o.getProject_type());
            //如果类别相同，则按照贷款额倒序排序
            if (result==0){
                if (this.getLoan()-o.getLoan()>0){
                    result=-1;
                }else {
                    result=1;
                }
            }
            return result;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(project_type);
            dataOutput.writeUTF(id);
            dataOutput.writeDouble(loan);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.project_type = dataInput.readUTF();
            this.id = dataInput.readUTF();
            this.loan = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return project_type + ',' +
                    id + ',' +
                    loan ;
        }

    }

    public static class Map extends Mapper<LongWritable, Text,LoanBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //跳过文件第一行
            if ("0".equals(key.toString())){
                return;
            }
            String[] split = value.toString().split(",");
            String type = split[2];
            String id = split[3];
            String loan = split[4];
            LoanBean loanBean = new LoanBean();
            loanBean.setProject_type(type);
            loanBean.setId(id);
            loanBean.setLoan(Double.parseDouble(loan));
            context.write(loanBean,NullWritable.get());
        }
    }

    public static class Grouping extends WritableComparator{
        public Grouping() {
            super(LoanBean.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            LoanBean aBean = (LoanBean)a;
            LoanBean bBean = (LoanBean)b;
            int result = 0;
            //类别相同的分为一组
            result = aBean.getProject_type().compareTo(bBean.getProject_type());
            return result;

        }
    }


    public static class Reduce extends Reducer<LoanBean,NullWritable,LoanBean,NullWritable>{
        @Override
        protected void reduce(LoanBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (NullWritable value : values) {
                if (count<3){
                    context.write(key,NullWritable.get());
                }
                count++;
            }

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.output.textoutputformat.separator",",");
        Job job = Job.getInstance(configuration, "top3");
        job.setJarByClass(LoanTop3.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\data\\input\\Foreign_Government_Loans.csv"));
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\data\\output\\output2"));

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(LoanBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(LoanBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(Grouping.class);
        job.waitForCompletion(true);

    }

}

```



![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018200836.png)



#### 学生成绩处理（2022年安徽省大数据与人工智能应用竞赛）

数据

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018201151.png)

##### 题目一

统计每个学生每门科目考试成绩的平均分

输出格式：姓名,课程,平均分

思路：现在map端求出每个人每科的总分 再除以科目数

注意！：以|分隔需要转义 split时应为"\\\\|"

```java
package mapreduce_stuscore_topclass;
import mapreduce2020.OrderFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class student_avg {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int count = 0;
            String[] split = value.toString().split(",");
            String stu_name_class = split[0];
            String[] split1 = stu_name_class.split("\\|");
            String stu_name = split1[0];
            String class_name = split1[1];
            String score_1 = split1[2];
            count++;
            double score_1d = Double.parseDouble(score_1);
            String[] other_scores = split[1].split(",");
            for (String other_score : other_scores) {
                //求总分
                score_1d += Double.parseDouble(other_score);
                //记录科目数量
                count++;
            }
            double avg_score = score_1d/count;
            context.write(new Text(stu_name+"," +class_name+"," + avg_score),NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "");
        job.setJarByClass(OrderFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        Path input_path = new Path("file:///D:\\data\\input\\stu_score_sub.csv");
        Path output_path = new Path("file:///D:\\data\\output\\output16");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(output_path)){
            fileSystem.delete(output_path,true);
        }
        TextInputFormat.addInputPath(job,input_path);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,output_path);

        job.waitForCompletion(true);
    }
}

```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231018212004.png)



##### 题目二

统计每门课程的最高分，并且按不同的课程存入不同的结果文件，要求一门课程一个结果文件

思路：由于要分文件输出，需要重写分区方法Partition

​			统计最高分——》以课程作为key 以成绩作为value

​			封装对象，重写排序方法，重写分组方法。

```java
package mapreduce_stuscore_topclass;

import mapreduce2020.OrderFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class classtop {
    public static class ClassBean implements WritableComparable<ClassBean>{
        private String class_name;
        private double score;

        @Override
        public int compareTo(ClassBean o) {
            //先排序课程名
            int result = 0;
            result = this.class_name.compareTo(o.class_name);
            //课程名相同倒序排序成绩
            if (result==0){
                if (this.score>o.score){
                    return -1;
                }else return 1;
            }
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(class_name);
            dataOutput.writeDouble(score);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.class_name = dataInput.readUTF();
            this.score = dataInput.readDouble();
        }

        public ClassBean() {
        }

        public String getClass_name() {
            return class_name;
        }

        public void setClass_name(String class_name) {
            this.class_name = class_name;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return class_name + "," +
                    score ;
        }
    }

    public static class MyPartitioner extends Partitioner<ClassBean, NullWritable>{

        @Override
        public int getPartition(ClassBean classBean, NullWritable nullWritable, int i) {
            if("Chinese".equals(classBean.getClass_name())){
                return 0;
            }else if ("English".equals(classBean.getClass_name())){
                return 1;
            }else
                return 2;
        }
    }

    public static class MyGrouping extends WritableComparator{
        // 一定要记得还有个true 才能创捷实例
        protected MyGrouping(){super(ClassBean.class,true);}

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            //按照课程名分组
            ClassBean aBean = (ClassBean)a;
            ClassBean bBean = (ClassBean)b;
            return aBean.getClass_name().compareTo(bBean.getClass_name());
        }
    }

    public static class MyMapper extends Mapper<LongWritable,Text,ClassBean,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String stu_name_class = split[0];
            String[] split1 = stu_name_class.split("\\|");
            String class_name = split1[1];
            String score_1 = split1[2];
            String[] other_scores = split[1].split(",");
            ClassBean classBean = new ClassBean();
            classBean.setClass_name(class_name);
            classBean.setScore(Double.parseDouble(score_1));
            context.write(classBean,NullWritable.get());
            for (String other_score : other_scores) {
                classBean.setClass_name(class_name);
                classBean.setScore(Double.parseDouble(other_score));
                context.write(classBean,NullWritable.get());
            }
        }

        public static class MyReducer extends Reducer<ClassBean,NullWritable,ClassBean,NullWritable>{
            @Override
            protected void reduce(ClassBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                //输出每门课程的第一条数据
                context.write(key,NullWritable.get());
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "");
            job.setJarByClass(OrderFormat.class);

            job.setMapperClass(MyMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(ClassBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(ClassBean.class);
            job.setOutputValueClass(NullWritable.class);


            //设置分组方法
            job.setGroupingComparatorClass(MyGrouping.class);

            job.setPartitionerClass(MyPartitioner.class);
            //分成三组所以发送3个reducetask
            job.setNumReduceTasks(3);


            Path input_path = new Path("file:///D:\\data\\input\\stu_score_sub.csv");
            Path output_path = new Path("file:///D:\\data\\output\\output17");
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(output_path)){
                fileSystem.delete(output_path,true);
            }
            TextInputFormat.addInputPath(job,input_path);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job,output_path);

            job.waitForCompletion(true);
        }
    }


}
```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231019175945.png)

##### 题目三

统计每门课程参考学生成绩最高的前三名学生，并将最后的结果以sub_top3作为文件名保存

思路：其实是求每门课程成绩前三，按照课程分组，按照成绩排序输出即可

```java
package mapreduce_stuscore_topclass;

import mapreduce2020.OrderFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class class_studentTop3 {
        public static class ClassTopBean implements WritableComparable<ClassTopBean> {
            private String stu_name;
            private String class_name;
            private double score;


            @Override
            public int compareTo(ClassTopBean o) {
                // 根据课程名排序
                int result = 0;
                result = this.class_name.compareTo(o.class_name);
                // 课程名相同根据成绩排序
                if (result==0){
                    if (this.score-o.score<0){
                        return 1;
                    }else return -1;
                }
                return result;
            }

            @Override
            public void write(DataOutput dataOutput) throws IOException {
                dataOutput.writeUTF(stu_name);
                dataOutput.writeUTF(class_name);
                dataOutput.writeDouble(score);
            }

            @Override
            public void readFields(DataInput dataInput) throws IOException {
                this.stu_name = dataInput.readUTF();
                this.class_name = dataInput.readUTF();
                this.score = dataInput.readDouble();
            }

            public ClassTopBean() {
            }

            public String getStu_name() {
                return stu_name;
            }

            public void setStu_name(String stu_name) {
                this.stu_name = stu_name;
            }

            public String getClass_name() {
                return class_name;
            }

            public void setClass_name(String class_name) {
                this.class_name = class_name;
            }

            public double getScore() {
                return score;
            }

            public void setScore(double score) {
                this.score = score;
            }

            @Override
            public String toString() {
                return class_name + "," +
                        stu_name + "," +
                        + score;
            }
        }

        public static class MyGrouping extends WritableComparator {
            // 一定要记得还有个true 才能创捷实例
            protected MyGrouping(){super(ClassTopBean.class,true);}

            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                //按照课程名分组
                ClassTopBean aBean = (ClassTopBean) a;
                ClassTopBean bBean = (ClassTopBean) b;
                return aBean.getClass_name().compareTo(bBean.getClass_name());
            }
        }

        public static class MyMapper extends Mapper<LongWritable, Text, ClassTopBean,NullWritable> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] split = value.toString().split(",");
                String stu_name_class = split[0];
                String[] split1 = stu_name_class.split("\\|");
                String student_name = split1[0];
                String class_name = split1[1];
                String score_1 = split1[2];
                String[] other_scores = split[1].split(",");
                ClassTopBean classTopBean = new ClassTopBean();
                classTopBean.setStu_name(student_name);
                classTopBean.setClass_name(class_name);
                classTopBean.setScore(Double.parseDouble(score_1));
                context.write(classTopBean,NullWritable.get());
                for (String other_score : other_scores) {
                    classTopBean.setClass_name(class_name);
                    classTopBean.setScore(Double.parseDouble(other_score));
                    context.write(classTopBean,NullWritable.get());
                }
            }

            public static class MyReducer extends Reducer<ClassTopBean,NullWritable, ClassTopBean,NullWritable> {
                @Override
                protected void reduce(ClassTopBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                    //输出前三条数据
                    int count = 0;
                    for (NullWritable value : values) {
                        if (count<3){
                            context.write(key,NullWritable.get());
                            count++;
                        }
                    }
                }
            }

            public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "");
                job.setJarByClass(OrderFormat.class);

                job.setMapperClass(MyMapper.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapOutputKeyClass(ClassTopBean.class);
                job.setMapOutputValueClass(NullWritable.class);

                job.setReducerClass(MyReducer.class);
                job.setOutputKeyClass(ClassTopBean.class);
                job.setOutputValueClass(NullWritable.class);
                //设置分组方法
                job.setGroupingComparatorClass(MyGrouping.class);

                Path input_path = new Path("file:///D:\\data\\input\\stu_score_sub.csv");
                Path output_path = new Path("file:///D:\\data\\output\\output18");
                FileSystem fileSystem = FileSystem.get(conf);
                if (fileSystem.exists(output_path)){
                    fileSystem.delete(output_path,true);
                }
                TextInputFormat.addInputPath(job,input_path);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextOutputFormat.setOutputPath(job,output_path);

                job.waitForCompletion(true);
            }
        }
    }


```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231019182156.png)



#### 气温数据处理（2023年安徽省大数据与人工智能应用竞赛网络赛）

数据及字段说明

现有国内某城市天气数据，字段间通过制表符分隔，第一个是日期，第二个是时间，第三个是气温。

注意：字段间通过制表符进行切分！ 

数据：weather.txt

##### 题目一

统计每年每个月份的最高气温

思路：每月最高气温 排序气温 按照月份分组

```java
package mapreduce_2023;

import mapreduce_calls.callsTop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxDegree {
    public static class DegreeBean implements WritableComparable<DegreeBean>{
        private String year;
        private String month;
        private int degree;

        @Override
        public int compareTo(DegreeBean o) {
            int result = 0;
            // 先按照年份分组
            result = this.year.compareTo(o.year);
            // 年份相同按照月份分组
            if (result==0){
                result = this.month.compareTo(o.month);
                // 月份相同按照气温倒序排序
                if (result==0){
                    if (this.degree - o.degree<0){
                        return 1;
                    }else return -1;
                }
            }
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(year);
            dataOutput.writeUTF(month);
            dataOutput.writeInt(degree);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.year = dataInput.readUTF();
            this.month = dataInput.readUTF();
            this.degree = dataInput.readInt();
        }

        public DegreeBean() {
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }

        public String getMonth() {
            return month;
        }

        public void setMonth(String month) {
            this.month = month;
        }

        public int getDegree() {
            return degree;
        }

        public void setDegree(int degree) {
            this.degree = degree;
        }

        @Override
        public String toString() {
            return year + "\t" +
                    month + "\t" +
                   degree;
        }
    }

    public static class MyGrouping extends WritableComparator{
        protected MyGrouping(){super(DegreeBean.class,true);}

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DegreeBean aBean = (DegreeBean)a;
            DegreeBean bBean = (DegreeBean)b;
            return aBean.getMonth().compareTo(bBean.getMonth());
        }
    }



    public static class MyMapper extends Mapper<LongWritable, Text,DegreeBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString()==null){
                return;
            }
            try {
                String[] split = value.toString().split("\t");
                String date = split[0];
                String[] split1 = date.split("-");
                String year = split1[0];
                String month = split1[1];
                String degree = split[2];
                DegreeBean degreeBean = new DegreeBean();
                degreeBean.setYear(year);
                degreeBean.setMonth(month);
                degreeBean.setDegree(Integer.parseInt(degree));
                context.write(degreeBean,NullWritable.get());
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

    public static class MyReducer extends Reducer<DegreeBean,NullWritable,DegreeBean,NullWritable>{
        @Override
        protected void reduce(DegreeBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //输出第一项
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "");
        job.setJarByClass(MaxDegree.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(DegreeBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(MyGrouping.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(DegreeBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path input = new Path("file:///D:\\data\\input\\weather.txt");
        Path output = new Path("file:///D:\\data\\output\\output19");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}

```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231019185003.png)

##### 题目二

统计每年每个月份最大的三个天气

输出格式：月份 [气温1，气温2，气温3]

思路：以月份分组	排序温度	取出前三	格式化输出

```java
package mapreduce_2023;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class MonthTop3 {
    public static class MonthBean implements WritableComparable<MonthBean>{
        private String month;
        private int degree;

        @Override
        public int compareTo(MonthBean o) {
            int result = 0;
            //根据月份排序
            result = this.month.compareTo(o.month);
            //月份相同根据温度倒序排序
            if (result==0){
                if (this.degree - o.degree<0){
                    result = 1;
                }else result = -1;
            }
            return result;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(month);
            dataOutput.writeInt(degree);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.month = dataInput.readUTF();
            this.degree = dataInput.readInt();
        }

        public String getMonth() {
            return month;
        }

        public void setMonth(String month) {
            this.month = month;
        }

        public int getDegree() {
            return degree;
        }

        public void setDegree(int degree) {
            this.degree = degree;
        }

        public MonthBean() {
        }

        @Override
        public String toString() {
            return month + "," +
                    degree;
        }
    }

    public static class MyGrouping extends WritableComparator{
        protected MyGrouping(){super(MonthBean.class,true);}

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            MonthBean aBean = (MonthBean) a;
            MonthBean bBean = (MonthBean) b;
            return aBean.getMonth().compareTo(bBean.getMonth());
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text,MonthBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] split = value.toString().split("\t");
                String date = split[0];
                String[] split1 = date.split("-");
                String month = split1[1];
                String degree = split[2];
                MonthBean monthBean = new MonthBean();
                monthBean.setMonth(month);
                monthBean.setDegree(Integer.parseInt(degree));
                context.write(monthBean,NullWritable.get());
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

    public static class MyReducer extends Reducer<MonthBean,NullWritable,Text,NullWritable>{
        // 取出前三条的温度 取出月份名称

        @Override
        protected void reduce(MonthBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String month = null;
            // 保存三条温度
            ArrayList<String> degreeList = new ArrayList<>();
            int count = 0;
            for (NullWritable value : values) {
                if (count<3){
                    month = key.toString().split(",")[0];
                    degreeList.add(key.toString().split(",")[1]);
                    count++;
                }
            }
            String result=month + "[";
            for (String s : degreeList) {
                result = result + s + ",";
            }
            // 由于多加了一个逗号，通过字符串剪切移除
            String substring = result.substring(0, result.length()-1);
            result = substring + "]";

            context.write(new Text(result),NullWritable.get());
        }
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "");
        job.setJarByClass(MonthTop3.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(MonthBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(MyGrouping.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path input = new Path("file:///D:\\data\\input\\weather.txt");
        Path output = new Path("file:///D:\\data\\output\\output20");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}

```

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231019191519.png)

