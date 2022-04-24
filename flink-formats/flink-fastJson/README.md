# 说明
该fastJson format是基于阿里的fastjson进行json解析
相较于Flink原生的json format：
1. 支持带反斜杠的json解析，如
```
{
"id": "1",
"desc": "{\"name\":\"mobin\"}"
}
```
2. 支持UNNE数组后元素为NULL解析
```
{"arrs":[null,null,null,123]}
```