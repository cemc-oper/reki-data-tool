#!/bin/bash
#SBATCH -J reki
#SBATCH -p {{ partition }}
#SBATCH -o {{ job_name }}.%j.out
#SBATCH -e {{ job_name }}.%j.err
#SBATCH --comment=Grapes_gfs_post
#SBATCH --no-requeue

# 使用 gribpost.exe 裁剪区域

set -x
set -e
date

# 参数
run_dir={{ run_dir }}
origin_file_path={{ origin_file_path }}
origin_file_name={{ origin_file_name }}
target_file_name={{ target_file_name }}


# 进入目录
test -d ${run_dir} || mkdir -p "${run_dir}"
cd ${run_dir}

rm -rf ${origin_file_name}
rm -rf ${target_file_name}

# 拷贝原始文件
cp ${origin_file_path} ${origin_file_name}

# 链接程序

ln -sf /g1/u/nwp/PRODS/bin/gribpost.exe .
ln -sf /g1/u/nwp/PRODS/share/cma.grib2 .

# 裁剪区域
./gribpost.exe -s ./${origin_file_name} \
    | ./gribpost.exe -i ./${origin_file_name} \
    -domain 89.875:0.125:0:180 \
    -dx 0.25 -dy 0.25 -nx 721 -ny 360 \
    -grib2 ./${target_file_name}

date