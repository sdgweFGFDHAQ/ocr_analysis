# ocr使用文档

## 代码运行

### （T4服务器）process_1_front.py

```
# 调用AI的华为云接口识别图片是否为店面照
# nohup python -u process_1_front.py > process_1_front.log 2>&1 &
```

### （4090服务器 conda ocr）process_2_ocr.py

```
# 调用paddleocr库进行ocr
使用run.sh封装，运行命令：# bash run.sh > runsh.log 2>&1 &
paddleocr缺点：GPU无法多进程、CPU多进程无效；运行一段时间内存溢出严重。
通过shell脚本多次运行py程序，且保证每个程序每识别5K条数据自动结束，并重启识别下一个5K条。
```

### （无限制）process_3_clean.py

```
# 对csv对象的ocr文本字段进行清洗
# nohup python -u process_3_clean.py > process_3_clean.log 2>&1 &
```

## 数据格式

### down_di_store_data.py

```postgresql
-- 下载数据
SELECT id,
       name,
       state,
       address,
       appcode,
       visit_num_6m,
       photos,
       filepath
FROM standard_db.di_store
WHERE channeltype_new = ''
  and id not in (select id from standard_db.di_store_channeltype_new)
  and (filepath <> '' or photos <> '')
  and visit_num_6m > 0
```

### process_1_front.py

```
包含图片的原始字段：'photos','filepath'
新增店面照字段：'front_path'
新增店内照字段：'inside_path'
```

### process_2_ocr.py

```
新增店面照ocr字段：'ocr_front_text'
新增店面照ocr字段：'ocr_inside_text'
```

### process_3_clean.py

```
新增ocr清洗字段：'ocr_clean'
新增ocr实体提取字段：'ocr_entity'

```

### （T4服务器）upload2hive.py

```
读取csv文件，通过海豚上传数据到数据湖仓
```