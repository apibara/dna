# Apibara 🤝 Parquet

_Create original datasets in one command._

**Use cases**

- Create datasets locally and explore them with Python or SQL (DuckDB).
- Integrate with Big Data tools.
- Aggregate large amount of data quickly.

**Usage**

Set the `PARQUET_OUTPUT_DIR` environment variable to the path of the directory
where the integration will write data to.

```
mkdir /path/to/my/data
export PARQUET_OUTPUT_DIR=/path/to/my/data
```

Run the script with `apibara run`. After the indexer has processed a hundred
blocks, you will start seeing `.parquet` files appearing in your folder. We can
use [DuckDB](https://duckdb.org/) to quickly analyze this data.

Number of transfer events ingested:

```sql
select count(1) from "/path/to/my/data/*.parquet";
```

Total transfer amount by block:

```sql
select
    block_number, sum(amount) as total_transfer
from
    "/path/to/my/data/*.parquet"
group by
    block_number
order by
    total_transfer desc;
```

You can combine DuckDB with
[Youplot](https://github.com/red-data-tools/YouPlot/) to create beautiful plots
from the command line.

```bash
duckdb -s "copy(select date_trunc('week', block_timestamp::timestamp) as date, sum(amount) as amount from '/tmp/example/*.parquet' group by date_trunc('week', block_timestamp::timestamp) order by amount desc) to '/dev/stdout' with (format 'csv', header)"  | uplot bar -d, -t "Top weekly transfer volume"
```

![Top weekly transfer volume](https://github.com/apibara/dna/assets/282580/1dc70fd9-3b33-40f6-aab4-921c441ee3ad)
