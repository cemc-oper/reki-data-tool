# CMA-GFS 东北半球数据

全要素场 + 保持原有顺序 + 区域裁剪 + (可选抽取)

原任务：串行，6分钟

## 说明

为了兼容 wgrib2 工具，需要关闭 ecCodes 的常量场 (constant field) 编码功能，请设置如下环境变量：

```bash
export ECCODES_GRIB_LARGE_CONSTANT_FIELDS=1
```

## 实现

### 命令行程序

#### gribpost.exe 版本

业务版本 V3.3

```bash
./gribpost.exe -s ./gmf.gra.${start_time}${forecast_hour}.grb2 \
    | ./gribpost.exe -i ./gmf.gra.${start_time}${forecast_hour}.grb2 \
    -domain 89.875:0.125:0:180 \
    -dx 0.25 -dy 0.25 -nx 721 -ny 360 \
    -grib2 ./ne_gmf.gra.${start_time}${forecast_hour}.grb2
```

00:07:51

#### wgrib2 版本

业务版本 V3.3

```bash
wgrib2 ./gmf.gra.${start_time}${forecast_hour}.grb2  \
    -small_grib 0:180 0.125:89.875 \
     ./ne_gmf.gra.${start_time}${forecast_hour}.grb2
```

00:02:11

注意：`wgrib2` 生成的数据南北反向，与 CMA-GFS 相反。

### 串行方法



### 并行方法



## 测试



## 结论



## 参考

