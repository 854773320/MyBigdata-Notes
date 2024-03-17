##### hive 问题记录

###### hive设置中文支持方法

进入元数据库	修改hive数据库中的如下字段

```sql
#修改字段注释字符集
alter table COLUMNS_V2 modify column COMMENT varchar(256) character set utf8;
#修改表注释字符集
alter table TABLE_PARAMS modify column PARAM_VALUE varchar(20000) character set utf8;
#修改分区参数，支持分区建用中文表示
alter table PARTITION_PARAMS modify column PARAM_VALUE varchar(20000) character set utf8;
alter table PARTITION_KEYS modify column PKEY_COMMENT varchar(20000) character set utf8;
#修改索引名注释，支持中文表示
alter table INDEX_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;
#修改视图，支持视图中文
ALTER TABLE TBLS modify COLUMN VIEW_EXPANDED_TEXT mediumtext CHARACTER SET utf8;
ALTER TABLE TBLS modify COLUMN VIEW_ORIGINAL_TEXT mediumtext CHARACTER SET utf8;
```



###### hive 导入文件分隔符有多个字段的解决方法

```hive
// 以field.delim 后的字符作为分隔符
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="$$");
```



hive 导入文件跳过首行

- 已建表执行

  ```sql
  alter table xxx set TBLPROPERTIES ('skip.header.line.count'='1');
  ```

- 新建表

  ```sql
  create table xxx
  (
       rank int,
       src string,
       name string,
       box_office string,
       avg_price int,
       avg_people int,
       begin_date string
  ) row format delimited fields terminated by ','
  TBLPROPERTIES ('skip.header.line.count'='1');	
  ```

```sql
select
area.province_name,
area.city_name,
sum(ord.paymoney) as Paymoney,
sum(
case when ord.state=='5' then ord.paymoney 
when ord.state=='6' then ord.paymoney
when ord.state=='7' then ord.paymoney
else 0 end 
)as pay_amount,
sum(ord.orderid) as sumOrderid
from
data_area area
join data_address ad on area.street_code=ad.street_id
join data_order ord on ord.orderid=ad.orderMainId
group by area.province_name,area.city_name;
```

```
 create table data_order(
 orderid string,
 ordertime string,
 orderBelongType string,
 saleOrgId string,
 orderCustomerType string,
 customerId string,
 customerName string,
 paymoney double,
 state string,
 subOrder string
 )ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="$$");
 
 
 create table data_address(
 crt_date string,
 orderMainId string,
 provicne_id string,
 city_id string,
 region_id string,
 street_id string 
 )ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="$$");
 
 
 create table data_area (
 province_code string,
 province_name string,
 city_code string,
 city_name string,
 zone_code string,
 zone_name string,
 street_code string,
 street_name string
 )ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="$$") 
 
load data local inpath'/usr/local/data/data_order.txt' into table data_order;
load data local inpath'/usr/local/data/data_address.txt' into table data_address;
load data local inpath'/usr/local/data/data_area.txt' into table data_area;
```

###### 设置hive本地模式运行(每次启动需要手动设置)

set hive.exec.mode.local.auto=true;