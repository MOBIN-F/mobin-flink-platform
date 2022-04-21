介绍：
>redis connector提供对redis的写入及维表支持

写入：
>写入支持redis的所有数据类：SET(字符串)、LPUSH(数组)、HSET(哈希)、SADD(集合)、ZADD(有序集合)

SET场景：【redis对应命令：set 1 100】
```
CREATE TABLE redistest (
id STRING,
age STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='SET',
'password'='mobin',
'mode'='cluster'
);
INSERT INTO redistest VALUES('1','200');
```
lpush场景：【redis对应命令：lpush alist 200】
```
CREATE TABLE redistest (
id STRING,
age STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='lpush',
'password'='mobin',
'mode'='cluster'
);
INSERT INTO redistest VALUES('alist','200');
```
hset场景：【redis对应命令：hset op:abc test mobin】
```
CREATE TABLE redistest (
hashkey STRING,
edcode STRING,
`value` STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='HSET',
'password'='mobin',
'mode'='cluster'
);
--hget db:info:16 edcode
INSERT INTO redistest VALUES('op:abc','test','mobin');
```
sadd场景：【redis对应命令：sadd saddtest 100】
```
CREATE TABLE redistest (
id STRING,
age STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='sadd',
'password'='mobin',
'mode'='cluster'
);
INSERT INTO redistest VALUES('saddtest','100');
```
zadd场景：【redis对应命令：zadd zaddtest 88 ff】
```
CREATE TABLE redistest (
key STRING,
`element` STRING,
score STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='zadd',
'password'='mobin',
'mode'='cluster'
);
INSERT INTO redistest VALUES('zaddtest','88','ff');
```
维表：
维表场景仅支持GET及HGET
get场景：
```
CREATE TABLE test1(
user_id STRING,
procTime AS PROCTIME()
) with (
'connector' = 'filesystem', 
'path' = 'file:///tmp/get.txt', 
'format' = 'csv'
);

CREATE TABLE redistest (
id STRING,
school STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='GET',
'password'='mobin',
'mode'='cluster'
);

CREATE TABLE printTable (
id STRING,
school STRING
) WITH (
'connector' = 'print'
);

INSERT INTO printTable
SELECT id,school
FROM test1 a
LEFT JOIN
redistest FOR SYSTEM_TIME AS OF a.procTime AS b
ON CAST(a.user_id AS STRING) = b.id;
```
hget场景：
```
CREATE TABLE test1(
edcode STRING,
procTime AS PROCTIME()
) with (
'connector' = 'filesystem', 
'path' = 'file:///tmp/hget.txt', 
'format' = 'csv'
);

CREATE TABLE redistest (
edcode STRING,
str STRING
) WITH (
'connector' = 'redis',
'host'='localhost',
'port' = '8080',
'command'='HGET',
'additional-key'='op:info', --需要指定hask_key
'password'='mobin',
'mode'='cluster'
);

CREATE TABLE printTable (
edcode STRING,
str STRING
) WITH (
'connector' = 'print'
);

INSERT INTO printTable
SELECT b.edcode,str
FROM test1 a
LEFT JOIN
redistest FOR SYSTEM_TIME AS OF a.procTime AS b
ON a.edcode = b.edcode;
```

|参数|说明|是否必填|默认值	|备注|
|----|---|---|---|---|
|connector|维表类型|是| |固定值：redis
|host|Redis连接地址|是|
|port|Redis连接端口|否|6379|
|password|Redis密码|否|
|command	|Redis操作类型|否| |作为sink时支持：set、hset、lpush、sadd、zadd <br>作为维表时支持：get、hget
|additional-key|hash类型的hash key|否| |仅对command=HGET/HSET时生效
|ttl-sec|key有效时间|否|
|lookup.cache.max-rows|客户端缓存行数|否| |仅对维表生效|
|lookup.cache.ttl-sec|客户端缓存有效时间|否| |仅对维表生效|
|lookup.max-retries|客户端查询重试次数|否| |仅对维表生效|
|connection.max-idle|连接池中空闲连接的最大数量|否|15|
|connection.min-idle|连接池中空闲连接的最小数量|否| 3
|connection.max-redirections|重连次数|否|10
|connection.timeout|连接超时时间|否|10000|
|connection.max.total|连接池中总连接的最大数量| |15|
|format|当表的第二列字段为json串的某个key时则返回key对应的值，否则返回null<br>比如，redis value值是{"myname":"abc","age":12}，当format='json'且第二列字段名为【myname】时返回abc，否则返回空|否| |仅对维表场景下效，当前仅支持format='json'
