# GFS 东北半球数据

全要素场 + 保持原有顺序 + 区域裁剪

原任务：串行，6分钟

## 说明



## 实现

### 业务版本

```bash
./gribpost.exe -s ./gmf.gra.${start_time}${forecast_hour}.grb2 \
    | ./gribpost.exe -i ./gmf.gra.${start_time}${forecast_hour}.grb2 \
    -domain 89.875:0.125:0:180 \
    -dx 0.25 -dy 0.25 -nx 721 -ny 360 \
    -grib2 ./ne_gmf.gra.${start_time}${forecast_hour}.grb2
```

### 串行方法



### 并行方法



## 测试



## 结论



## 参考

