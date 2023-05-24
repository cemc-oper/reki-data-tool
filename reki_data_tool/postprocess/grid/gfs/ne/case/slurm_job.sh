#!/bin/bash
#SBATCH -J reki
#SBATCH -p {{ partition }}
{%- if is_parallel %}
#SBATCH -N {{ nodes }}
#SBATCH --ntasks-per-node={{ ntasks_per_node }}
{%- endif %}
#SBATCH -o {{ job_name }}.%j.out
#SBATCH -e {{ job_name }}.%j.err
#SBATCH --comment=Grapes_gfs_post
#SBATCH --no-requeue

set -x

unset GRIB_DEFINITION_PATH

echo "script start..."
date

JOB_ID=${SLURM_JOB_ID:-0}
echo "job id: ${JOB_ID}"

echo "load anaconda3..."
date
. /g1/u/wangdp/start_anaconda3.sh

echo "load nwpc-data..."
date
conda activate py39-dev

export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export PYTHONUNBUFFERED=1

ulimit -s unlimited

echo "enter work directory..."
work_directory={{ work_directory }}
test -d ${work_directory} || mkdir -p ${work_directory}
cd ${work_directory}

echo "run script..."
date

{%- if is_parallel %}
module load compiler/intel/composer_xe_2017.2.174
module load mpi/intelmpi/2017.2.174
# module load apps/eccodes/2.17.0/intel

export I_MPI_PMI_LIBRARY=/opt/gridview/slurm17/lib/libpmi.so

srun --mpi=pmi2 python -m {{ model_path }} {{ options }}
{%- else  %}
python -m {{ model_path }} {{ options }}
{%- endif %}

echo "script done"
date

set +x