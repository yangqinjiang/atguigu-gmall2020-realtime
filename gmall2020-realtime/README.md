phoenix:
```
创建省份表
create table gmall2020_province_info (id varchar primary key,info.name
varchar,info.area_code varchar,info.iso_code varchar)SALT_BUCKETS = 3;
```

```
创建用户表
create table gmall2020_user_info (id varchar primary key ,user_level varchar, birthday varchar,
gender varchar, age_group varchar , gender_name varchar)SALT_BUCKETS = 3;
```

```
创建品牌表
create table gmall2020_base_trademark (id varchar primary key ,tm_name varchar);
```

```
创建分类表
create table gmall2020_base_category3 (id varchar primary key ,name
varchar ,category2_id varchar);
```

```
创建 SPU 表
create table gmall2020_spu_info (id varchar primary key ,spu_name varchar);
```


```
创建商品表
create table gmall2020_sku_info (id varchar primary key , spu_id varchar, price
varchar, sku_name varchar, tm_id varchar, category3_id varchar, create_time varchar,
category3_name varchar, spu_name varchar, tm_name varchar ) SALT_BUCKETS = 3;
```
## 利用 maxwell-bootstrap 初始化数据
```
➢ --user maxwell
数据库分配的操作 maxwell 数据库的用户名
➢ --password 123456
数据库分配的操作 maxwell 数据库的密码
➢ --host
数据库主机名
➢ --database
数据库名
➢ --table
表名
➢ --client_id
maxwell-bootstrap 不具备将数据直接导入 kafka或者 hbase 的能力，通过--client_id
指定将数据交给哪个 maxwell 进程处理，在 maxwell 的 conf.properties 中配置
```
初始化省份表
bin/maxwell-bootstrap --user maxwell --password 123456 --host hadoop102 --database gmall2020 --table base_province --client_id maxwell_1
初始化用户表
bin/maxwell-bootstrap --user maxwell --password 123456 --host hadoop102 --database gmall2020 --table user_info --client_id maxwell_1
初始化品牌数据
bin/maxwell-bootstrap --user maxwell --password 123456 --host hadoop102 --database gmall2020 --table base_trademark --client_id maxwell_1
初始化分类数据
bin/maxwell-bootstrap --user maxwell --password 123456 --host hadoop102 --database gmall2020 --table base_category3 --client_id maxwell_1
初始化 SPU 数据
bin/maxwell-bootstrap --user maxwell --password 123456 --host hadoop102 --database gmall2020 --table spu_info --client_id maxwell_1
初始化商品数据
bin/maxwell-bootstrap --user maxwell --password 123456 --host hadoop102 --database gmall2020 --table sku_info --client_id maxwell_1
```
es索引模板: 订单宽表
PUT _template/gmall2020_order_info_template
{
"index_patterns": ["gmall2020_order_info*"],
"settings": {
"number_of_shards": 3
},
"aliases" : {
"{index}-query": {},
"gmall2020_order_info-query":{}
},
"mappings": {
"_doc":{
"properties":{
"id":{
"type":"long"
},
"province_id":{
"type":"long"
},
"order_status":{
"type":"keyword"
},
"user_id":{
"type":"long"
},
"final_total_amount":{
"type":"double"
},
"benefit_reduce_amount":{
"type":"double"
},
"original_total_amount":{
"type":"double"
},
"feight_fee":{
"type":"double"
},
"expire_time":{
"type":"keyword"
},
"create_time":{
"type":"keyword"
},
"create_date":{
"type":"date"
},
"create_hour":{
"type":"keyword"
},
"if_first_order":{
"type":"keyword"
},
"province_name":{
"type":"keyword"
},
"province_area_code":{
"type":"keyword"
},
"province_iso_code":{
"type":"keyword"
},
"user_age_group":{
"type":"keyword"
},
"user_gender":{
"type":"keyword"
}
}
}}}
```