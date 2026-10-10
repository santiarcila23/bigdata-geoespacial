## Resultados benchmark: agregaci├│n por grilla

`first_s` = primera ejecuci├│n (en fr├¡o); `median_s` = mediana de todas las repeticiones.

| engine   | config      |   workers |   cores |   runs |   first_s |   median_s |   min_s |   peak_mem_mb_total |   cells |    rows |
|:---------|:------------|----------:|--------:|-------:|----------:|-----------:|--------:|--------------------:|--------:|--------:|
| dask     | dask-2w-2c  |         2 |       2 |      3 |     2.41  |      0.339 |   0.337 |               513.6 |    2569 | 1419554 |
| dask     | dask-4w-4c  |         4 |       4 |      3 |     1.202 |      0.372 |   0.37  |               674.8 |    2569 | 1419554 |
| spark    | spark-1w-2c |         1 |       2 |      3 |    10.837 |      1.52  |   0.996 |               561.3 |    2569 | 1419554 |
| spark    | spark-2w-4c |         2 |       4 |      3 |    15.224 |      1.732 |   1.339 |               947.8 |    2569 | 1419554 |

### Ejecuciones individuales

engine      config  workers  cores  run  seconds  cells    rows  peak_mem_mb_total  peak_mem_mb_max_worker
 spark spark-1w-2c        1      2    1   10.837   2569 1419554              423.9                     NaN
 spark spark-1w-2c        1      2    2    1.520   2569 1419554              502.4                     NaN
 spark spark-1w-2c        1      2    3    0.996   2569 1419554              561.3                     NaN
 spark spark-2w-4c        2      4    1   15.224   2569 1419554              773.8                     NaN
 spark spark-2w-4c        2      4    2    1.339   2569 1419554              852.1                     NaN
 spark spark-2w-4c        2      4    3    1.732   2569 1419554              947.8                     NaN
  dask  dask-2w-2c        2      2    1    2.410   2569 1419554              507.9                   264.6
  dask  dask-2w-2c        2      2    2    0.339   2569 1419554              513.2                   266.5
  dask  dask-2w-2c        2      2    3    0.337   2569 1419554              513.6                   266.8
  dask  dask-4w-4c        4      4    1    1.202   2569 1419554              672.0                   259.5
  dask  dask-4w-4c        4      4    2    0.372   2569 1419554              673.5                   260.6
  dask  dask-4w-4c        4      4    3    0.370   2569 1419554              674.8                   261.4
