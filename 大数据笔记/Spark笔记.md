Spark笔记

#### 

#### pycharm远程连接集群

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029165951.png)

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029170051.png)



#### pycharm远程python环境配置

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029170932.png)

#### ![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029171036.png)



#### pycharm远程连接数据库

连接mysql

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029170240.png)



![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029170615.png)



#### pycharm sftp远程文件传输协议配置

![](https://unis-0328-markdown.oss-cn-hangzhou.aliyuncs.com/typora_screenshot/20231029170826.png)

#### 在pycharm远程编程时找不到JAVA_HOME的解决方案

方案一

在编译器中使用os模块添加JAVA_HOME的路径

```python
import os
os.environ['JAVA_HOME'] = '/usr/local/soft/jdk1.8.0_171'
```

方案二

把/etc/profile 中配置的环境路径复制到 /etc/bashrc下

（因为编译器无法读取profile文件，但是可以读取bashrc文件）

 /etc/profile 系统环境变量

/etc/bashrc 用户环境变量





#### Spark算子

spark算子分为两类

- transformatition算子（转化算子）
  - 用来进行数据处理，类型转换等操作
  - 以线程的方式执行，本身不进行计算操作
  - 必需配合action算子才能进行数据的计算
- action算子（执行算子）
  - 用来触发数据的计算



##### 常用Transformation算子

1. map算子

   ```python
   # map 算子    转化数据为k-v类型  对于列表数据进行处理
   # lambda x:x+1 x会接受rdd中的每一个元素数据 x+1就是对rdd的每个元素的计算过程
   # 执行完成后返回一个新的rdd
   from pyspark import SparkContext
   
   sc = SparkContext()
   rdd = sc.parallelize([1, 2, 3, 4, 5, 6])
   rdd2 = rdd.map(lambda x: x + 1)
   # 使用action算子触发计算
   # collect 是一个action算子，可以触发计算，然后返回列表
   res = rdd2.collect()
   print(res)
   
   #返回
   [2, 3, 4, 5, 6, 7]
   ```

   

2. flatmap算子

   ```python
   from pyspark import SparkContext
   
   sc = SparkContext()
   rdd1 = sc.parallelize([[1, 2, 3], ['a', 'b', 'c']])
   rdd_str = sc.parallelize(['a,b,c,d'])
   # flatmap 算子    主要用于处理嵌套列表
   # 相当于一个for循环，会循环遍历每一个元素的所有值
   # 此时 x是【1，2，3】和【‘a’,'b','c'】    flatmap遍历的结果即每个列表中的每个元素
   rdd3 = rdd1.flatMap(lambda x: x)
   # 此时 x 是 【’a,b,c,d‘】 此时 x为’a,b,c,d‘ 通过字符串切分 转换为 【'a','b','c','d'】
   rdd4 = rdd_str.flatMap(lambda x: x.split(","))
   res2 = rdd3.collect()
   res3 = rdd4.collect()
   print(f'flatmap算子{res2}')
   print(f'flatmap算子2{res3}')
   
   #返回
   flatmap算子[1, 2, 3, 'a', 'b', 'c']
   flatmap算子2['a', 'b', 'c', 'd']
   ```

   

3. filter算子

   ```python
   # filter 算子作用是过滤数据，在lambda中写判断逻辑，会返回符合判断逻辑的数据
   rdd_filter1 = sc.parallelize([1,2,3,4,5])
   rdd_filter2 = sc.parallelize(['a','b','a','c'])
   
   rdd_filtered1 = rdd_filter1.filter(lambda x:x>2)
   rdd_filtered2 = rdd_filter2.filter(lambda x:x=='a' or x=='c')
   
   res_filter1 = rdd_filtered1.collect()
   res_filter2 = rdd_filtered2.collect()
   print(f'filter算子{res_filter1}')
   print(f'filter算子{res_filter2}')
   
   # 返回
   filter算子[3, 4, 5]
   filter算子['a', 'a', 'c']
   ```

   

4. groupby算子

   ```python
   # groupby算子通过hash取余的方式进行分区，相同的余数会被分为一组，会返回一个rdd类型的可迭代对象 （key，value） ——> （0，iterable（'java','java'））
   rdd_groupby = sc.parallelize(['java','python','java','scala','python'])
   rdd_groupby1 = rdd_groupby.groupBy(lambda x:hash(x)%3)
   # 此时rdd的值不能直接读取，需要使用mapvalues获取value 转换为 list 再次读取
   res_groupby = rdd_groupby1.mapValues(lambda x:list(x)).collect()
   print(f'groupby算子{res_groupby}')
   
   # 返回
   groupby算子[(0, ['python', 'python']), (2, ['scala']), (1, ['java', 'java'])]
   ```

5. groupbykey算子

   ```python
   # 对kv类型按照数据 key 分组，相同key分为一起
   rdd_kv = sc.parallelize([('a',1),('b',2),('c',3),('a',1),('b',2)])
   # groupbykey 会直接返回一个按照key分组的新的rdd
   rdd_kv = rdd_kv.groupByKey().mapValues(lambda x:list(x))
   res_kv = rdd_kv.collect()
   print(f'groupbykey算子{res_kv}')
   
   # 返回
   groupbykey算子[('b', [2, 2]), ('c', [3]), ('a', [1, 1])]
   ```

6. reduceBykey算子

   ```python
   # 对kv类型的数据先进行分组，再进行聚合计算
   # 相同分组内的数据会进行聚合计算
   # reducebykey会将相同key值的数据放在一起，然后对每个key中的value数据进行累加计算
   # lambda需要接受两个参数，后面编写累加计算
   rdd_kv = sc.parallelize([('a',1),('b',2),('c',3),('a',1),('b',2)])
   reduce_rdd = rdd_kv.reduceByKey(lambda x,y:x+y)
   res_reducebykey = reduce_rdd.collect()
   print(res_reducebykey)
   
   # 返回
   [('b', 4), ('c', 3), ('a', 2)]
   ```

7. sortByKey算子

   ```python
   # sortByKey会将数据按照key值升序排序
   res_sortbykey = rdd_kv.sortByKey().collect()
   print(res_sortbykey)
   # 降序
   # res_sortbykey = rdd_kv.sortByKey(ascending=false).collect()
   # 返回
   [('a', 1), ('a', 1), ('b', 2), ('b', 2), ('c', 3)]
   ```

8. sortby算子

   ```python
   # sortby排序算子 可以指定按照哪个数据进行排序
   # sortby会将rdd中的元素数据传递给函数使用
   # lambda需要一个接受值x，接受rdd中的每个元素
   # 如果元素是k，v类型，可以通过下标方式指定按照哪种排序
   res_sortby = rdd_kv.sortBy(lambda x:x[1]).collect()
   print(res_sortby)
   # 返回
   [('a', 1), ('a', 1), ('b', 2), ('b', 2), ('c', 3)]
   ```

   

##### 常用action算子

```python
# 触发计算过程
# action算子一旦触发，那就不能再进行rdd操作了
# collect算子，触发计算，获取所有计算结果

# reduce算子，传递一个计算逻辑，对数据进行累加计算
# 可以不需要转化算子，直接累加。但是不能处理kv类型的数据
res = rdd.reduce(lambda x,y:x+y)
print(res)
# count 获取rdd元素个数
res = rdd.count()
print(res)
# take 取指定数量的元素数据
# 获取前3个数据
res = rdd.take(3)
print(res)
```



##### 高级transformation算子

union算子、join（left，right，outer）算子

```python
from pyspark import SparkContext

# 多个rdd操作
sc = SparkContext()

rdd1 = sc.parallelize([1,2,3,4])
rdd2 = sc.parallelize([5,6,7,8])

rdd_kv1 = sc.parallelize([('a',1),('b',2),('c',3)])
rdd_kv2 = sc.parallelize([('c',4),('d',5),('e',6)])

# rdd 合并操作
# rdd1合并rdd2,合并后返回新的rdd 合并之后不会进行去重操作 用distinct去重
union_rdd = rdd1.union(rdd2)
union_rdd2 = rdd_kv1.union(rdd_kv2)

res = union_rdd.collect()
res2 = union_rdd2.collect()
print(f'unionall 合并结果：{res}')
print(f'unionall kv 合并结果：{res2}')

# kv形式的rdd进行join关联 通过key值进行关联
# 内关联 相同k值的数据会保留
join_rdd = rdd_kv1.join(rdd_kv2)
res3 = join_rdd.collect()
print(f'rdd内关联结果：{res3}')
# 左关联  左边rdd的数据会被保留下来 如果右边rdd有对应的key值数据会显示 没有对应key值会显示为空
left_join_rdd = rdd_kv1.leftOuterJoin(rdd_kv2)
res4 = left_join_rdd.collect()
print(f'rdd左关联结果：{res4}')
# 右关联   与左关联相反
right_join_rdd = rdd_kv1.rightOuterJoin(rdd_kv2)
res5 = right_join_rdd.collect()
print(f'rdd左关联结果：{res5}')

```

```python
# 结果如下
unionall 合并结果：[1, 2, 3, 4, 5, 6, 7, 8]
unionall kv 合并结果：[('a', 1), ('b', 2), ('c', 3), ('c', 4), ('d', 5), ('e', 6)]
rdd内关联结果：[('c', (3, 4))]
rdd左关联结果：[('b', (2, None)), ('c', (3, 4)), ('a', (1, None))]
rdd左关联结果：[('c', (3, 4)), ('e', (None, 6)), ('d', (None, 5))]
```



##### 高级action算子

first、top、takeOrdered、takeSample

```python
from pyspark import SparkContext

sc = SparkContext()
rdd = sc.parallelize([1,2,3,8,5,7,6])

# action 算子使用
# first 取第一个值
res = rdd.first()
print(f'first结果{res}')
# top 先进行rdd的数据排序，排序规则为从大到小排序，排序后可以指定取出对应数量的元素数据
res = rdd.top(3) # 取出排序后最大的三个数
print(f'top结果{res}')
# takeOrdered 跟top差不多，但是为从小到大排序
res = rdd.takeOrdered(3)
print(f'takeOrdered结果{res}')
# 随机取值
# takesample
# 第一个参数——》是否允许取重复值
# 第二个参数——》指定取值个数
# 第三个参数——》一个随机数种子，可以指定任意值，没有任何区别
res = rdd.takeSample(True,6,16531513)
print(f'takesample结果{res}')
```

```python
# 结果如下
first结果1
top结果[8, 7, 6]
takeOrdered结果[1, 2, 3]
takesample结果[1, 5, 3, 1, 8, 7]
```



#####  分区算子

###### 迭代器

```python
# 迭代器配合next 可以一次取一个值进行计算
data = [1,2,3,4]
# python自带的迭代器
# 只能依次取值，不能进行其它的计算
iter_obj = iter(data)
# 配合next方法取值
print(next(iter_obj))
print(next(iter_obj))
print(next(iter_obj))
# 自定义迭代器（生成器）
# 自定义迭代器可以完成数值的计算
# 固定的格式
def func(data):
    # data 接收迭代的数据
    # 该数据一般都是可以遍历的
    for i in data:
        sum_data = i+1
        yield sum_data #关键词，类似return作用，将结果数据依次返回

func_iter = func(data)

# 配合next使用
print(next(func_iter))
print(next(func_iter))
print(next(func_iter))

# 结果
1
2
3
2
3
4
```

