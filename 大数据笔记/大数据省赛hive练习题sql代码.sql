设置hive本地模式运行
set hive.exec.mode.local.auto=true;

///***
2021年大数据省赛题目
***///
create table data_order(
    orderid string,
    ordertime string,
    orderBelongType string,
    saleOrgId string,
    orderCustomerType char(20),
    customid string,
    customerName string,
    paymoney double,
    state string,
    subOrder string 
)ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="$$");


create table data_address(
    crt_date string,
    orderMainId string,
    provicne_id string,
    city_id string,
    region_id string,
    street_id string
)ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="$$")
TBLPROPERTIES ('skip.header.line.count'='1');

create table data_area(
    province_code string,
    province_name string,
    city_code string,
    city_name string,
    zone_code string,
    zone_name string,
    street_code string,
    street_name string
)ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="$$")
TBLPROPERTIES ('skip.header.line.count'='1');


select *
from
(select provicne_id,street_id,paymoney,state,subOrder
from data_order
join data_address
on orderid=orderMainId)t1
join data_area
on street_id=street_code
limit 10;

/*
使用所建的表，按照省市分组，统计 有效订单（subOrder=1）中的 订单总金额（sum），
已支付的订单总金额（订单状态state =5、6、7 这三类的paymoney汇总），订单个数（count）（6分）
*/
// 统计有效订单 及有效订单总金额
select provicne_id,city_name,round(sum(paymoney))sales_amount,count(provicne_id) ordernums
from 
(
    select *
    from
    (select provicne_id,street_id,paymoney,state,subOrder
    from data_order
    join data_address
    on orderid=orderMainId)t1
    join data_area
    on street_id=street_code
)t2
where subOrder=1
group by provicne_id,city_name
limit 10;

// 统计已支付订单总金额
select provicne_id,city_name,sum(paymoney) pay_amount
from 
t1
group by provicne_id,city_name
where state in (5,6,7)

// 连接两张表
select province_name,t3.city_name,sales_amount,pay_amount,ordernums
from
(
    select provicne_id,city_name,round(sum(paymoney))sales_amount,count(provicne_id) ordernums
    from 
    (
    select *
    from
    (select provicne_id,street_id,paymoney,state,subOrder
    from data_order
    join data_address
    on orderid=orderMainId)t1
    join data_area
    on street_id=street_code
    )t2
    where subOrder=1
    group by provicne_id,city_name
)t3
join
(
    select provicne_id,province_name,city_name,round(sum(paymoney)) pay_amount
    from 
    (
        select *
        from
        (select provicne_id,street_id,paymoney,state,subOrder
        from data_order
        join data_address
        on orderid=orderMainId)t1
        join data_area
        on street_id=street_code
    )t2
    where state in (5,6,7)
    group by provicne_id,province_name,city_name
)t4
on t3.provicne_id=t4.provicne_id and t3.city_name=t4.city_name


/*
使用所建的表统计每个省订单数最多的3个市（6分）。 输出的字段 包含 省份名称，城市名称，订单个数
*/
select *
from
(
    select province_name,city_name,order_amount,rank() over(partition by province_name order by order_amount desc) ranks
    from
    (select province_name,city_name,count(city_name) order_amount
    from data_order
    join data_address
    on orderid=orderMainId
    join data_area
    on provicne_id=province_code
    group by province_name,city_name
    )t1
)t2
where ranks<=3;







///***
2020年大数据省赛题目
***///
create table student(
    s_id int,
    s_name string,
    s_sex string,
    s_age int,
    s_dept string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table cource(
    c_id int,
    c_name string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

create table sc(
    s_id int,
    c_id int,
    score int
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

load data local inpath '/usr/local/data/student.txt' overwrite into table student;
load data local inpath '/usr/local/data/sc.txt' overwrite into table sc;
load data local inpath '/usr/local/data/course.txt' overwrite into table cource;


select t1.c_name,round(t1.avg_score)
from
(select c_name,avg(score) as avg_score
from sc
left join
cource
on sc.c_id=cource.c_id
group by c_name)as t1


select max(score)
from sc
join cource
on cource.c_id=sc.c_id
where sc.c_id=1;


select *
from student
left join sc
on student.s_id=sc.s_id
left join cource
on cource.c_id=sc.c_id;

select *
from
(select student.s_name,cource.c_name,sc.score,rank() over(partition by c_name order by score desc) rank_stu
from student
left join sc
on student.s_id=sc.s_id
left join cource
on cource.c_id=sc.c_id
)t1
where rank_stu <= 2;


/*
2022年大数据省赛代码
*/

create table weibo_article(
    id Bigint,
    source String,
    reposts_count Bigint,
    attitude_count Bigint
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

load data local inpath '/usr/local/data/weibo_article.csv' into table weibo_article

create table comments(
    comment_id Bigint,
    comment_time String,
    like_count Bigint,
    weibo_id Bigint
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

load data local inpath '/usr/local/data/comments.csv' into table comments

//统计评论次数最多的前10条微博
    // 评论数据存在重复问题要去重
select distinct(comment_id),comment_time,like_count,weibo_id
from comments

    //去重后join并排序
select weibo_id,count(weibo_id) comment_cnt
from weibo_article
join
(
    select distinct(comment_id),comment_time,like_count,weibo_id
    from comments
)t1
on id=weibo_id
group by weibo_id
order by comment_cnt desc;

//统计微博来源为“微博 weibo.com”其微博评论点赞数最高的前三条评论

select weibo_id,comment_id,like_count,rank() over (partition by weibo_id order by like_count desc) top_rank
from weibo_article
join
(
    select distinct(comment_id),comment_time,like_count,weibo_id
    from comments
)t1
on id=weibo_id
where source="微博 weibo.com"


select weibo_id,comment_id,like_count,top_rank
from
(
    select weibo_id,comment_id,like_count,rank() over (partition by weibo_id order by like_count desc) top_rank
    from weibo_article
    join
    (
        select distinct(comment_id),comment_time,like_count,weibo_id
        from comments
    )t1
    on id=weibo_id
    where source="微博 weibo.com"
)t2
where top_rank<3


//统计每条微博连续小时段内微博评论点赞数之和,并最终将点赞数之和小于等于5的记录过滤掉

//先提取出小时字段  按照微博id和时间排序

select *
from
(
    select distinct(weibo_id),substring(comment_time,1,2) hour_str,like_count
    from comments
    order by weibo_id,hour_str
)t1



// 利用开窗函数 按照微博id和时间分组 累加计算每条微博每个小时的点赞数之和
select weibo_id,hour_str,sum(like_count) over (partition by weibo_id,hour_str order by weibo_id,hour_str rows between unbounded preceding and current row) like_total
from
(
    select distinct(weibo_id),substring(comment_time,1,2) hour_str,like_count
    from comments
    order by weibo_id,hour_str
)t1



//  利用开窗函数 取出每组最后一行 的累加结果
 
select weibo_id,hour_str,like_cnt,hour_str-lag(hour_str,1,0) over (partition by weibo_id order by weibo_id,hour_str) flag
from
(select weibo_id,hour_str,like_total,last_value(like_total) over (partition by weibo_id,hour_str order by hour_str range between unbounded preceding and unbounded following) like_cnt
from
(
    select weibo_id,hour_str,sum(like_count) over (partition by weibo_id,hour_str order by weibo_id,hour_str rows between unbounded preceding and current row) like_total
    from
    (
        select distinct(weibo_id),substring(comment_time,1,2) hour_str,like_count
        from comments
        order by weibo_id,hour_str
    )t1
)t2
)t3
group by weibo_id,hour_str,like_cnt


select weibo_id,concat(first_time,"~",last_time) hour_diff,like_cnt
from
(select weibo_id,hour_str,like_cnt,flag,
first_value(hour_str) over (partition by weibo_id,flag order by hour_str) first_time,
last_value(hour_str) over (partition by weibo_id,flag order by hour_str range between unbounded preceding and unbounded following) last_time
from 
(
    select weibo_id,hour_str,like_cnt,hour_str-row_number() over (partition by weibo_id order by hour_str)  flag
    from
    (select weibo_id,hour_str,like_total,last_value(like_total) over (partition by weibo_id,hour_str order by hour_str range between unbounded preceding and unbounded following) like_cnt
    from
    (
        select weibo_id,hour_str,sum(like_count) over (partition by weibo_id,hour_str order by weibo_id,hour_str rows between unbounded preceding and current row) like_total
        from
        (
            select distinct(weibo_id),substring(comment_time,1,2) hour_str,like_count
            from comments
            order by weibo_id,hour_str
        )t1
    )t2
    )t3
    group by weibo_id,hour_str,like_cnt
)t4
)t5
where like_cnt>5;
