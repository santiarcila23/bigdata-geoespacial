"""Lee las líneas JSON del benchmark por stdin y escribe una tabla Markdown."""
import json
import sys

import pandas as pd

rows = [json.loads(l) for l in sys.stdin if l.strip().startswith("{")]
if not rows:
    sys.exit("No hay resultados de benchmark")
df = pd.DataFrame(rows)

summary = (df.groupby(["engine", "config", "workers", "cores"])
             .agg(runs=("run", "count"),
                  first_s=("seconds", "first"),
                  median_s=("seconds", "median"),
                  min_s=("seconds", "min"),
                  peak_mem_mb_total=("peak_mem_mb_total", "max"),
                  cells=("cells", "max"),
                  rows=("rows", "max"))
             .reset_index()
             .sort_values(["engine", "workers"]))

print("## Resultados benchmark: agregación por grilla\n")
print("`first_s` = primera ejecución (en frío); `median_s` = mediana de todas las repeticiones.\n")
print(summary.to_markdown(index=False))
print("\n### Ejecuciones individuales\n")
print(df.to_string(index=False))
