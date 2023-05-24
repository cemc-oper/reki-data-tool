#!/bin/bash
#SBATCH -J reki
#SBATCH -p {{ partition }}
#SBATCH -o {{ job_name }}.%j.out
#SBATCH -e {{ job_name }}.%j.err
#SBATCH --comment=Grapes_gfs_post
#SBATCH --no-requeue

# 使用 wgrib2 裁剪区域

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

module load mathlib/wgrib2/2.0.6/intel compiler/intel/composer_xe_2017.2.174

wgrib2 ./${origin_file_name} \
    -small_grib 0:180 0.125:89.875 \
     ./${target_file_name}