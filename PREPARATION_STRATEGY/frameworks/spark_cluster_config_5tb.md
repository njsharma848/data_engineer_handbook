# Spark Distributed Hardware Configuration Framework

> A practical guide for sizing and configuring Spark clusters to process **5 TB** of data.
> Covers cluster sizing, memory tuning, shuffle optimization, and real-world configuration templates.

---

## TABLE OF CONTENTS

```
01. Quick Reference: 5TB Cluster Sizing
02. Core Concepts: Spark Resource Model
03. Cluster Sizing Methodology
04. Memory Configuration Deep Dive
05. CPU & Parallelism Configuration
06. Shuffle & Network Optimization
07. Storage & I/O Configuration
08. Configuration Templates by Workload Type
09. Cloud Provider Sizing (AWS EMR / Azure HDInsight / GCP Dataproc)
10. Dynamic Allocation & Auto-Scaling
11. Monitoring & Tuning Checklist
12. Common Configuration Mistakes
13. Interview Questions & Answers
```

---

## 1. Quick Reference: 5TB Cluster Sizing

### Recommended Starting Configuration

```
DATA SIZE:          5 TB (compressed on disk, ~15-20 TB uncompressed in memory)
CLUSTER SIZE:       20-30 worker nodes
INSTANCE TYPE:      Memory-optimized (e.g., r5.4xlarge / E16s_v3 / n2-highmem-16)
CORES PER NODE:     16 vCPUs
MEMORY PER NODE:    128 GB RAM
TOTAL CLUSTER:      320-480 vCPUs, 2.5-3.8 TB RAM
STORAGE PER NODE:   500 GB - 1 TB NVMe SSD (for shuffle/spill)
```

### Key Configuration Summary

| Parameter                                  | Value             | Why                                   |
|--------------------------------------------|-------------------|---------------------------------------|
| `spark.executor.memory`                    | `90g`             | ~75% of node memory for executor      |
| `spark.executor.cores`                     | `5`               | Sweet spot for HDFS throughput        |
| `spark.executor.memoryOverhead`            | `10g`             | 10% of executor memory for off-heap   |
| `spark.driver.memory`                      | `16g`             | Enough for collect/broadcast          |
| `spark.sql.shuffle.partitions`             | `2000-4000`       | ~1-2 GB per shuffle partition         |
| `spark.default.parallelism`                | `600-1500`        | 2-3x total cores                      |
| `spark.sql.adaptive.enabled`               | `true`            | Auto-optimize at runtime              |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true`      | Reduce small partitions               |
| `spark.serializer`                         | `org.apache.spark.serializer.KryoSerializer` | 10x faster than Java |
| `spark.sql.parquet.filterPushdown`         | `true`            | Read only needed data                 |

---

## 2. Core Concepts: Spark Resource Model

### How Spark Uses Resources

```
CLUSTER
├── DRIVER (1 node)
│   ├── Coordinates job execution
│   ├── Collects results
│   └── Broadcasts small data
│
├── WORKER NODE 1
│   ├── EXECUTOR 1 (JVM process)
│   │   ├── Core 1 → Task (1 partition)
│   │   ├── Core 2 → Task (1 partition)
│   │   ├── ...
│   │   ├── Core 5 → Task (1 partition)
│   │   └── Memory
│   │       ├── Execution Memory (shuffle, join, sort, aggregation)
│   │       ├── Storage Memory (cached RDDs/DataFrames)
│   │       └── User Memory (UDFs, data structures)
│   └── EXECUTOR 2 (if multiple per node)
│
├── WORKER NODE 2
│   └── ...
└── WORKER NODE N
```

### Memory Layout Per Executor

```
Total Container Memory (e.g., 100g)
├── spark.executor.memoryOverhead (10g) ← Off-heap: Python, JVM overhead, NIO buffers
└── spark.executor.memory (90g) ← JVM Heap
    ├── Reserved Memory (300 MB) ← Internal Spark bookkeeping
    └── Usable Memory (89.7g)
        ├── Unified Memory (60%) = ~54g
        │   ├── Storage Memory ← Cached data (can borrow from execution)
        │   └── Execution Memory ← Shuffles, joins, sorts (can evict storage)
        └── User Memory (40%) = ~36g
            └── User data structures, UDFs, metadata
```

### Key Formulas

```
Usable Memory = (spark.executor.memory - 300MB)
Unified Memory = Usable Memory × spark.memory.fraction (default 0.6)
Storage Memory = Unified Memory × spark.memory.storageFraction (default 0.5)
Execution Memory = Unified Memory - Storage Memory (can borrow more)

Total Parallelism = Number of Executors × Cores per Executor
Ideal Partition Count = 2-4 × Total Parallelism
Target Partition Size = 128 MB - 1 GB (for shuffle partitions)
```

---

## 3. Cluster Sizing Methodology

### Step-by-Step Sizing for 5 TB

#### Step 1: Estimate Data Expansion

```
Raw Data on Disk:           5 TB (Parquet/ORC compressed)
Compression Ratio:          3-4x (Parquet typical)
Uncompressed in Memory:     15-20 TB
Intermediate Data (shuffles): 1-3x of input (depending on operations)
Peak Memory Needed:         ~20-40 TB total across all stages
```

#### Step 2: Choose Node Type

| Workload Type     | Recommended Instance    | vCPUs | Memory | Use Case                    |
|-------------------|-------------------------|-------|--------|-----------------------------|
| ETL / Transforms  | r5.4xlarge (AWS)        | 16    | 128 GB | Joins, aggregations         |
| Heavy Shuffle     | r5.8xlarge (AWS)        | 32    | 256 GB | Large joins, wide shuffles  |
| ML / Iterative    | r5.4xlarge + GPU        | 16    | 128 GB | Training, feature eng       |
| Cost-Optimized    | m5.4xlarge (AWS)        | 16    | 64 GB  | Simple ETL, light workloads |

#### Step 3: Calculate Number of Nodes

```
Formula:
  Nodes = Peak Memory / (Memory per Node × Utilization Factor)

For 5 TB ETL with joins:
  Peak Memory ≈ 20 TB (uncompressed + shuffle overhead)
  Utilization Factor = 0.75 (executor gets ~75% of node memory)
  Memory per Node = 128 GB

  Nodes = 20,000 GB / (128 GB × 0.75) = ~208 effective GB/node
  Nodes = 20,000 / 96 = ~21 nodes

  RECOMMENDATION: 20-30 worker nodes (start with 25, tune from there)
```

#### Step 4: Calculate Executors Per Node

```
OPTION A: Fat Executors (Recommended for 5TB)
  Executors per Node:  2-3
  Cores per Executor:  5
  Memory per Executor: 40-55g

OPTION B: Thin Executors (more parallelism)
  Executors per Node:  5
  Cores per Executor:  3
  Memory per Executor: 20-24g

OPTION C: Single Executor Per Node (simplest)
  Executors per Node:  1
  Cores per Executor:  15 (leave 1 for OS)
  Memory per Executor: 110g

WHY 5 CORES PER EXECUTOR?
  - HDFS has optimal throughput with 5 concurrent tasks per executor
  - More than 5 cores → excessive GC pressure
  - Fewer than 3 cores → low utilization
```

#### Step 5: Set Partition Count

```
Total Cores = 25 nodes × 3 executors × 5 cores = 375 cores

Shuffle Partitions:
  Method 1: Based on data size
    5 TB input → aim for 1 GB per shuffle partition → 5000 partitions

  Method 2: Based on cores
    2-4x total cores → 750-1500 partitions

  RECOMMENDATION: Start with 2000, enable AQE to auto-tune
```

---

## 4. Memory Configuration Deep Dive

### Executor Memory Settings

```properties
# Core memory settings
spark.executor.memory=90g
spark.executor.memoryOverhead=10g

# Memory fractions
spark.memory.fraction=0.6           # 60% for execution + storage
spark.memory.storageFraction=0.5    # 50% of unified for storage (can be borrowed)

# Off-heap memory (for Tungsten)
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=10g
```

### When to Increase Memory

| Symptom                           | Cause                          | Fix                                              |
|-----------------------------------|--------------------------------|--------------------------------------------------|
| `OutOfMemoryError: Java heap`     | Executor memory too small      | Increase `spark.executor.memory`                 |
| `Container killed by YARN`        | Off-heap memory exceeded       | Increase `spark.executor.memoryOverhead`          |
| `Spill to disk` in Spark UI      | Not enough execution memory    | Increase `spark.memory.fraction` or total memory |
| Slow cache operations             | Storage memory too small       | Increase `spark.memory.storageFraction`           |
| Driver OOM on `collect()`         | Too much data to driver        | Increase `spark.driver.memory` or avoid collect   |
| GC pause > 10% of task time       | Heap too large for GC          | Reduce executor memory, add more executors        |

### Driver Memory Settings

```properties
# Driver needs enough for:
# - Broadcast variables
# - collect() results
# - Job metadata
spark.driver.memory=16g
spark.driver.memoryOverhead=4g
spark.driver.maxResultSize=4g       # Max size of serialized results
```

### GC Tuning (For Large Heaps > 32g)

```properties
# Use G1GC for large heaps
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16m -XX:+ParallelRefProcEnabled
```

---

## 5. CPU & Parallelism Configuration

### Core Settings

```properties
# Executor cores
spark.executor.cores=5              # 5 is the sweet spot

# Total parallelism
spark.default.parallelism=750       # For RDD operations (2x total cores)
spark.sql.shuffle.partitions=2000   # For DataFrame/SQL operations

# Dynamic partition count (Spark 3.0+)
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.coalescePartitions.initialPartitionNum=4000
spark.sql.adaptive.coalescePartitions.minPartitionSize=64MB
spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB
```

### Partition Sizing Guide

| Data Size per Stage | Recommended Partitions | Target Partition Size |
|---------------------|------------------------|-----------------------|
| < 1 GB              | 8-20                   | 64-128 MB             |
| 1-10 GB             | 20-200                 | 128-256 MB            |
| 10-100 GB           | 200-1000               | 128-512 MB            |
| 100 GB - 1 TB       | 1000-2000              | 256 MB - 1 GB         |
| 1-5 TB              | 2000-5000              | 256 MB - 1 GB         |
| 5-10 TB             | 5000-10000             | 512 MB - 1 GB         |

### Task Scheduling

```properties
# Speculative execution (re-run slow tasks on other nodes)
spark.speculation=true
spark.speculation.interval=100ms
spark.speculation.multiplier=1.5
spark.speculation.quantile=0.75

# Task locality wait times
spark.locality.wait=3s
spark.locality.wait.node=3s
spark.locality.wait.rack=3s
```

---

## 6. Shuffle & Network Optimization

### Why Shuffle Matters for 5 TB

```
Shuffle = redistribution of data across the network
Every JOIN, GROUP BY, SORT triggers a shuffle

For 5 TB data:
  - A single wide join could shuffle 2-10 TB across the network
  - Shuffle is the #1 bottleneck for large datasets
  - Network bandwidth and disk I/O become critical
```

### Shuffle Configuration

```properties
# Shuffle partitions (most important setting for large data)
spark.sql.shuffle.partitions=2000

# Shuffle manager
spark.shuffle.manager=sort                          # Default, use sort-based shuffle

# Shuffle spill settings
spark.shuffle.spill.compress=true                   # Compress spill files
spark.shuffle.compress=true                         # Compress shuffle output

# Shuffle service (external shuffle - prevents data loss on executor failure)
spark.shuffle.service.enabled=true

# Shuffle file buffer
spark.shuffle.file.buffer=1m                        # Default 32k, increase for large shuffles
spark.reducer.maxSizeInFlight=96m                   # Default 48m, controls fetch buffer size

# Sort-merge join threshold
spark.sql.autoBroadcastJoinThreshold=100MB          # Broadcast tables under 100MB (default 10MB)
```

### Adaptive Query Execution (AQE) - Spark 3.0+

```properties
# AQE automatically optimizes shuffle partitions at runtime
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
spark.sql.adaptive.localShuffleReader.enabled=true
```

### Network Settings

```properties
# Network timeout (increase for large shuffles)
spark.network.timeout=600s
spark.rpc.askTimeout=600s
spark.sql.broadcastTimeout=600s

# Max retries for fetch failures
spark.shuffle.io.maxRetries=10
spark.shuffle.io.retryWait=30s
```

### Data Skew Handling

```
PROBLEM: One partition has 100x more data than others
  - 99 tasks finish in 1 minute, 1 task takes 2 hours
  - Overall job time = slowest task time

SOLUTIONS:
```

```python
# Solution 1: Salting (manual)
# Add random prefix to skewed key to distribute it
df_salted = df.withColumn("salt", F.concat(F.col("skewed_key"), F.lit("_"), (F.rand() * 10).cast("int")))
# Join on salted key, then aggregate to remove salt

# Solution 2: Enable AQE skew join handling (Spark 3.0+)
# spark.sql.adaptive.skewJoin.enabled=true
# Spark automatically splits skewed partitions

# Solution 3: Broadcast join (if one side is small enough)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Solution 4: Two-phase aggregation
# Phase 1: Partial aggregation with salted key
df_partial = (df
    .withColumn("salt", (F.rand() * 100).cast("int"))
    .groupBy("key", "salt")
    .agg(F.sum("value").alias("partial_sum"))
)
# Phase 2: Final aggregation without salt
df_final = df_partial.groupBy("key").agg(F.sum("partial_sum").alias("total"))
```

---

## 7. Storage & I/O Configuration

### File Format Recommendations

| Format  | Compression | Read Speed | Write Speed | Splittable | Use Case            |
|---------|-------------|------------|-------------|------------|---------------------|
| Parquet | Snappy      | Fast       | Fast        | Yes        | Default choice      |
| Parquet | ZSTD        | Fast       | Medium      | Yes        | Better compression  |
| ORC     | Zlib        | Fast       | Fast        | Yes        | Hive ecosystem      |
| Delta   | Snappy      | Fast       | Fast        | Yes        | ACID + time travel  |
| Avro    | Snappy      | Medium     | Fast        | Yes        | Schema evolution    |
| CSV     | Gzip        | Slow       | Fast        | No         | Avoid for 5TB!      |
| JSON    | Gzip        | Slow       | Fast        | No         | Avoid for 5TB!      |

### Parquet Configuration

```properties
# Parquet read optimization
spark.sql.parquet.filterPushdown=true           # Push filters to Parquet reader
spark.sql.parquet.mergeSchema=false             # Don't merge schemas (faster)
spark.sql.parquet.enableVectorizedReader=true   # Columnar batch reading

# Parquet write optimization
spark.sql.parquet.compression.codec=snappy      # snappy (fast) or zstd (smaller)
spark.sql.parquet.writeLegacyFormat=false
```

### File Size & Partitioning

```python
# Optimal file sizes for 5 TB
# Target: 128 MB - 1 GB per file (Parquet)

# Writing with optimal file sizes
df.repartition(5000) \
  .write \
  .partitionBy("year", "month") \
  .parquet("s3://bucket/output/")

# Coalesce to reduce small files
df.coalesce(5000) \
  .write \
  .mode("overwrite") \
  .parquet("s3://bucket/output/")
```

### Partition Strategy for 5 TB

```
DATA: 5 TB of event data (2 years, 50 event types)

GOOD partition scheme:
  /data/year=2024/month=01/day=01/  → ~7 GB per day
  Total partitions: ~730 (2 years × 365 days)
  Files per partition: 5-10 (1 GB each)

BAD partition scheme (over-partitioned):
  /data/year=2024/month=01/day=01/hour=00/event_type=click/
  Total partitions: 730 × 24 × 50 = 876,000
  Files per partition: 1-5 KB each → TOO MANY SMALL FILES!

RULE OF THUMB:
  - Partition on columns used in WHERE clauses
  - Aim for 100 MB - 1 GB per partition
  - Total partitions: 1,000 - 50,000 for 5 TB
  - Avoid high-cardinality partition keys (user_id, transaction_id)
```

### Local Storage for Shuffle

```properties
# Use NVMe SSDs for shuffle spill (critical for 5 TB workloads)
spark.local.dir=/mnt/nvme1,/mnt/nvme2   # Multiple disks for parallel I/O

# Disk I/O settings
spark.shuffle.file.buffer=1m
spark.unsafe.sorter.spill.reader.buffer.size=1m
spark.storage.memoryMapThreshold=2m
```

---

## 8. Configuration Templates by Workload Type

### Template 1: ETL / Data Transformation (5 TB)

```properties
# Cluster: 25 nodes × r5.4xlarge (16 vCPUs, 128 GB)
# Use case: Read 5 TB Parquet, transform, write back

spark.master=yarn
spark.submit.deployMode=cluster

# Executor settings
spark.executor.instances=50
spark.executor.cores=5
spark.executor.memory=50g
spark.executor.memoryOverhead=6g

# Driver settings
spark.driver.memory=16g
spark.driver.memoryOverhead=4g
spark.driver.maxResultSize=4g

# Parallelism
spark.default.parallelism=500
spark.sql.shuffle.partitions=2000

# AQE
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# Serialization
spark.serializer=org.apache.spark.serializer.KryoSerializer

# I/O
spark.sql.parquet.filterPushdown=true
spark.sql.parquet.mergeSchema=false

# Shuffle
spark.shuffle.compress=true
spark.shuffle.spill.compress=true
spark.sql.autoBroadcastJoinThreshold=100MB

# Network
spark.network.timeout=600s
```

### Template 2: Heavy Join Workload (5 TB × 2 TB)

```properties
# Cluster: 30 nodes × r5.8xlarge (32 vCPUs, 256 GB)
# Use case: Join 5 TB fact table with 2 TB dimension table

spark.executor.instances=60
spark.executor.cores=5
spark.executor.memory=100g
spark.executor.memoryOverhead=15g

spark.driver.memory=32g
spark.driver.memoryOverhead=8g

# More shuffle partitions for large joins
spark.sql.shuffle.partitions=4000
spark.default.parallelism=1200

# AQE for skew handling
spark.sql.adaptive.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=512MB

# Larger broadcast threshold (if dimension table fits)
spark.sql.autoBroadcastJoinThreshold=2g

# Shuffle optimization
spark.shuffle.file.buffer=1m
spark.reducer.maxSizeInFlight=96m
spark.shuffle.io.maxRetries=10
spark.shuffle.io.retryWait=60s

# Extended timeouts for large shuffles
spark.network.timeout=800s
spark.sql.broadcastTimeout=600s

# Sort-merge join tuning
spark.sql.join.preferSortMergeJoin=true
```

### Template 3: Aggregation Heavy (5 TB → Summary)

```properties
# Cluster: 20 nodes × r5.4xlarge (16 vCPUs, 128 GB)
# Use case: Aggregate 5 TB into summary tables

spark.executor.instances=40
spark.executor.cores=5
spark.executor.memory=50g
spark.executor.memoryOverhead=6g

spark.driver.memory=16g

# Moderate shuffle partitions
spark.sql.shuffle.partitions=1500
spark.default.parallelism=400

# AQE for partition coalescing (aggregation reduces data)
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB

# Serialization
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Filter pushdown (aggregate queries often filter first)
spark.sql.parquet.filterPushdown=true

# Off-heap for large aggregation buffers
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=8g
```

### Template 4: Streaming Micro-Batch (Continuous 5 TB/day)

```properties
# Cluster: 15 nodes × r5.4xlarge (16 vCPUs, 128 GB)
# Use case: Process ~5 TB/day in micro-batches (Structured Streaming)

spark.executor.instances=30
spark.executor.cores=5
spark.executor.memory=40g
spark.executor.memoryOverhead=5g

spark.driver.memory=8g

# Smaller shuffle partitions (micro-batches are smaller)
spark.sql.shuffle.partitions=200
spark.default.parallelism=300

# Streaming-specific
spark.streaming.backpressure.enabled=true
spark.streaming.kafka.maxRatePerPartition=10000
spark.sql.streaming.stateStore.maintenanceInterval=30s

# Checkpointing
spark.sql.streaming.checkpointLocation=s3://bucket/checkpoints/

# Graceful shutdown
spark.streaming.stopGracefullyOnShutdown=true
```

---

## 9. Cloud Provider Sizing (AWS EMR / Azure HDInsight / GCP Dataproc)

### AWS EMR Configuration

```
CLUSTER:
  Master:  1 × m5.2xlarge (8 vCPU, 32 GB)
  Core:    20 × r5.4xlarge (16 vCPU, 128 GB)
  Task:    5-10 × r5.4xlarge (spot instances for cost savings)

STORAGE:
  Input/Output: S3 (s3a://)
  Shuffle:      Instance store NVMe (i3.4xlarge) or EBS gp3

EMR-SPECIFIC SETTINGS:
  spark.hadoop.fs.s3a.connection.maximum=200
  spark.hadoop.fs.s3a.fast.upload=true
  spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer
  spark.hadoop.fs.s3a.multipart.size=104857600
  spark.hadoop.fs.s3a.connection.timeout=200000

COST ESTIMATE (on-demand):
  ~$25-40/hour for the cluster
  ~$200-320 for an 8-hour processing job
  Use spot instances for task nodes → save 60-70%
```

### Azure HDInsight / Databricks

```
CLUSTER:
  Head:    2 × D14_v2 (16 vCPU, 112 GB)
  Worker:  20 × E16s_v3 (16 vCPU, 128 GB)

STORAGE:
  Input/Output: ADLS Gen2 (abfss://)
  Shuffle:      Premium SSD managed disks

AZURE-SPECIFIC:
  spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net=<key>
  spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization=false
```

### GCP Dataproc

```
CLUSTER:
  Master:  1 × n2-highmem-8 (8 vCPU, 64 GB)
  Worker:  20 × n2-highmem-16 (16 vCPU, 128 GB)

STORAGE:
  Input/Output: GCS (gs://)
  Shuffle:      Local SSDs attached to worker nodes

GCP-SPECIFIC:
  spark.hadoop.google.cloud.auth.service.account.enable=true
  spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
```

### Cost Comparison (Approximate for 5 TB ETL)

| Provider       | Instance Type     | Nodes | $/hour   | 8-hr Job Cost | Spot/Preemptible |
|----------------|-------------------|-------|----------|---------------|------------------|
| AWS EMR        | r5.4xlarge        | 25    | ~$30     | ~$240         | ~$90             |
| Azure Databricks | E16s_v3         | 25    | ~$35     | ~$280         | ~$100            |
| GCP Dataproc   | n2-highmem-16     | 25    | ~$28     | ~$224         | ~$80             |

---

## 10. Dynamic Allocation & Auto-Scaling

### Dynamic Allocation Configuration

```properties
# Let Spark request/release executors based on workload
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=10
spark.dynamicAllocation.maxExecutors=100
spark.dynamicAllocation.initialExecutors=25

# When to scale up (pending tasks)
spark.dynamicAllocation.schedulerBacklogTimeout=5s

# When to scale down (idle executors)
spark.dynamicAllocation.executorIdleTimeout=120s
spark.dynamicAllocation.cachedExecutorIdleTimeout=600s

# Required: external shuffle service
spark.shuffle.service.enabled=true
```

### When to Use Dynamic vs Fixed Allocation

| Scenario                           | Fixed Allocation    | Dynamic Allocation   |
|------------------------------------|---------------------|----------------------|
| Predictable, steady workload       | Recommended         | OK                   |
| Variable stages (read → join → agg)| OK                  | Recommended          |
| Cost optimization                  | Less optimal        | Recommended          |
| Multi-tenant cluster               | Required per job    | Recommended          |
| Streaming workload                 | Recommended         | Not recommended      |
| Ad-hoc / interactive queries       | Less optimal        | Recommended          |

---

## 11. Monitoring & Tuning Checklist

### Spark UI Metrics to Watch

```
STAGE VIEW:
  ✅ Check: Task duration distribution (should be uniform)
  ⚠️ Red flag: One task takes 10x longer than others → DATA SKEW

STORAGE VIEW:
  ✅ Check: Cached data fraction and eviction count
  ⚠️ Red flag: High eviction count → increase storage memory

EXECUTORS VIEW:
  ✅ Check: GC time (should be < 10% of task time)
  ⚠️ Red flag: GC time > 20% → reduce executor memory or tune GC

SQL VIEW:
  ✅ Check: Physical plan (joins, exchanges)
  ⚠️ Red flag: BroadcastNestedLoopJoin → missing join condition
  ⚠️ Red flag: SortMergeJoin on small table → enable broadcast

ENVIRONMENT VIEW:
  ✅ Check: All configurations are applied
  ⚠️ Red flag: Default values on critical settings
```

### Tuning Decision Tree

```
JOB IS SLOW?
│
├── Are tasks evenly distributed?
│   ├── NO → DATA SKEW
│   │   ├── Enable AQE skew join
│   │   ├── Salt the skewed key
│   │   └── Use broadcast join if one side is small
│   └── YES → continue
│
├── Is GC time > 10%?
│   ├── YES → MEMORY PRESSURE
│   │   ├── Reduce executor memory (smaller heap = less GC)
│   │   ├── Add more executors instead
│   │   └── Enable off-heap memory
│   └── NO → continue
│
├── Are tasks spilling to disk?
│   ├── YES → NOT ENOUGH EXECUTION MEMORY
│   │   ├── Increase spark.executor.memory
│   │   ├── Increase spark.memory.fraction
│   │   └── Reduce partition count (larger partitions = more memory per task)
│   └── NO → continue
│
├── Are there many small tasks (< 10ms)?
│   ├── YES → TOO MANY PARTITIONS
│   │   ├── Reduce spark.sql.shuffle.partitions
│   │   ├── Enable AQE coalescing
│   │   └── Use coalesce() before write
│   └── NO → continue
│
├── Are shuffle read/write sizes very large?
│   ├── YES → SHUFFLE BOTTLENECK
│   │   ├── Filter earlier in the pipeline
│   │   ├── Use broadcast join where possible
│   │   ├── Increase spark.reducer.maxSizeInFlight
│   │   └── Add more NVMe storage for shuffle
│   └── NO → continue
│
└── Is I/O wait time high?
    ├── YES → STORAGE BOTTLENECK
    │   ├── Use Parquet/ORC instead of CSV/JSON
    │   ├── Enable filter pushdown
    │   ├── Partition data on commonly filtered columns
    │   └── Use column pruning (select only needed columns)
    └── NO → Profile further with Spark event logs
```

### Key Metrics for 5 TB Jobs

| Metric                    | Healthy Range      | Action if Outside            |
|---------------------------|--------------------|------------------------------|
| Task duration uniformity  | < 2x median        | Fix data skew                |
| GC time per executor      | < 10% of task time | Tune memory/GC               |
| Shuffle spill to disk     | 0 (ideally)        | Increase execution memory    |
| Shuffle read/write ratio  | ~ 1:1              | Check for data explosion     |
| Executor utilization      | > 70%              | Reduce idle executors        |
| Stage completion time     | < 30 min/stage     | Check for stragglers/skew    |
| Failed tasks              | < 1%               | Check OOM, network issues    |

---

## 12. Common Configuration Mistakes

### Mistake 1: Default Shuffle Partitions (200)

```properties
# ❌ BAD: Default value for 5 TB
spark.sql.shuffle.partitions=200    # Only 200 partitions for 5 TB!
# Result: Each partition = 25 GB → OOM / extreme spill

# ✅ GOOD: Sized for 5 TB
spark.sql.shuffle.partitions=2000   # Each partition ≈ 2.5 GB
# Or enable AQE to auto-tune
spark.sql.adaptive.enabled=true
```

### Mistake 2: Too Many Cores Per Executor

```properties
# ❌ BAD: All cores in one executor
spark.executor.cores=16    # 16 cores sharing one JVM
spark.executor.memory=110g # One big executor
# Result: Excessive GC, poor HDFS throughput

# ✅ GOOD: Optimal core count
spark.executor.cores=5     # Sweet spot for HDFS
spark.executor.memory=50g  # Manageable heap
```

### Mistake 3: Ignoring Memory Overhead

```properties
# ❌ BAD: No overhead specified (uses default 10% or 384MB min)
spark.executor.memory=120g
# spark.executor.memoryOverhead=???  (defaults to max(384MB, 0.10 * 120g) = 12g)
# Container = 120g + 12g = 132g → might exceed node capacity!

# ✅ GOOD: Explicit overhead
spark.executor.memory=90g
spark.executor.memoryOverhead=10g
# Container = 90g + 10g = 100g → predictable sizing
```

### Mistake 4: Not Using AQE (Spark 3.0+)

```properties
# ❌ BAD: Manual tuning without AQE
spark.sql.shuffle.partitions=5000   # Fixed, may be too many or too few

# ✅ GOOD: Enable AQE for automatic optimization
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

### Mistake 5: Broadcasting Large Tables

```properties
# ❌ BAD: Broadcasting a 5 GB table (OOM risk on driver)
spark.sql.autoBroadcastJoinThreshold=5g

# ✅ GOOD: Conservative broadcast threshold
spark.sql.autoBroadcastJoinThreshold=100MB
# Only broadcast genuinely small lookup tables
```

### Mistake 6: Using CSV/JSON for 5 TB

```python
# ❌ BAD: CSV for 5 TB (not splittable, no column pruning, no pushdown)
df = spark.read.csv("s3://bucket/5tb_data.csv.gz")

# ✅ GOOD: Parquet with partitioning
df = spark.read.parquet("s3://bucket/5tb_data/")
# Benefits: columnar, compressed, splittable, filter pushdown
```

### Mistake 7: collect() on Large Results

```python
# ❌ BAD: Collecting 5 TB to driver
all_data = df.collect()  # Driver OOM guaranteed!

# ✅ GOOD: Write to storage
df.write.parquet("s3://bucket/output/")

# Or aggregate first
summary = df.groupBy("key").agg(F.sum("value")).collect()
```

---

## 13. Interview Questions & Answers

### Q1: How would you size a Spark cluster for 5 TB of data?

**Answer Framework:**
```
1. ESTIMATE DATA EXPANSION
   - 5 TB compressed → ~15-20 TB in memory
   - Add 1-3x for shuffle overhead → ~30 TB peak

2. CHOOSE NODE TYPE
   - Memory-optimized: r5.4xlarge (16 vCPU, 128 GB)
   - Need: 30 TB / (128 GB × 0.75 utilization) ≈ 25 nodes

3. CONFIGURE EXECUTORS
   - 5 cores per executor (HDFS sweet spot)
   - 2-3 executors per node
   - ~50g memory per executor

4. SET PARALLELISM
   - spark.sql.shuffle.partitions = 2000-4000
   - Enable AQE for auto-tuning

5. OPTIMIZE
   - Parquet format with filter pushdown
   - Broadcast small dimension tables
   - NVMe SSDs for shuffle spill
```

### Q2: What happens if shuffle partitions are too few/many?

```
TOO FEW (e.g., 200 for 5 TB):
  - Each partition = 25 GB → doesn't fit in memory
  - Excessive spill to disk → slow
  - OOM errors on executors
  - One slow partition blocks the entire stage

TOO MANY (e.g., 100,000 for 5 TB):
  - Each partition = 50 MB → very small
  - Overhead of scheduling 100K tasks
  - Too many small files on write
  - Network overhead from shuffle metadata

JUST RIGHT (2000-5000 for 5 TB):
  - Each partition = 1-2.5 GB → fits in memory
  - Good parallelism
  - Reasonable file sizes on write
```

### Q3: How do you handle data skew in a 5 TB join?

```
DETECT:
  - Spark UI shows one task taking 10x+ longer
  - Shuffle read size for one partition >> median

SOLUTIONS (in order of preference):
  1. AQE Skew Join (Spark 3.0+): Auto-splits skewed partitions
  2. Broadcast Join: If one side < 1-2 GB
  3. Salting: Add random prefix to skewed key, join, then aggregate
  4. Two-Phase Aggregation: Pre-aggregate with salt, then final aggregate
  5. Filter + Union: Process skewed keys separately
  6. Bucket Tables: Pre-sort and bucket on join key (Hive tables)
```

### Q4: Why 5 cores per executor?

```
HDFS THROUGHPUT:
  - HDFS client is optimized for ~5 concurrent threads
  - More than 5 → diminishing returns, increased contention

GC PRESSURE:
  - More cores per executor → more concurrent tasks → more objects
  - More objects → more GC pauses → longer stop-the-world events

MEMORY SHARING:
  - 5 tasks share one executor's memory
  - Each task gets ~memory/5 for execution
  - Balance between parallelism and per-task memory

EXCEPTION:
  - If tasks are CPU-bound (ML training), 3-4 cores might be better
  - If tasks are memory-heavy (wide joins), 3 cores with more memory
```

### Q5: Explain Spark's Unified Memory Model

```
JVM Heap (spark.executor.memory)
├── Reserved Memory (300 MB)
└── Usable Memory
    ├── Unified Memory Pool (spark.memory.fraction × usable)
    │   ├── Storage Memory (cached data)
    │   │   └── Can be evicted if execution needs more
    │   └── Execution Memory (shuffles, joins, sorts)
    │       └── Cannot be evicted by storage
    └── User Memory (remaining)
        └── Internal metadata, UDFs, user data structures

KEY INSIGHT:
  - Execution can borrow from storage (evicts cached data)
  - Storage CANNOT borrow from execution
  - This prevents OOM during shuffles/joins
```

### Q6: When would you use dynamic allocation vs fixed?

```
DYNAMIC ALLOCATION:
  - Multi-stage pipelines (read → transform → join → write)
  - Each stage needs different resources
  - Cost optimization (release idle executors)
  - Shared clusters (fair resource usage)
  - spark.dynamicAllocation.enabled=true

FIXED ALLOCATION:
  - Streaming jobs (consistent resource needs)
  - Single-stage operations
  - When you need predictable performance
  - Benchmark/performance testing
  - spark.executor.instances=50
```

### Q7: Your 5 TB Spark job is OOM. Walk through debugging.

```
STEP 1: IDENTIFY WHERE
  - Driver OOM? → Reduce collect(), increase driver memory
  - Executor OOM? → Continue to step 2

STEP 2: CHECK SPARK UI
  - Storage tab: Is too much data cached?
  - Stages tab: Which stage fails?
  - Tasks tab: Is one task getting too much data? (SKEW)

STEP 3: CHECK MEMORY SETTINGS
  - spark.executor.memory = 50g (enough?)
  - spark.executor.memoryOverhead = 10% (enough for off-heap?)
  - spark.memory.fraction = 0.6 (needs more for execution?)

STEP 4: CHECK SHUFFLE
  - spark.sql.shuffle.partitions = 200? (TOO FEW! Increase to 2000+)
  - Shuffle read per task > 2 GB? (partitions too large)

STEP 5: CHECK FOR SKEW
  - One task has 10x more data?
  - Enable AQE: spark.sql.adaptive.skewJoin.enabled=true

STEP 6: OPTIMIZE QUERY
  - Filter early (before joins)
  - Select only needed columns
  - Broadcast small dimension tables
  - Use Parquet with filter pushdown
```

---

## Quick Cheat Sheet: spark-submit for 5 TB

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 50 \
  --executor-cores 5 \
  --executor-memory 50g \
  --driver-memory 16g \
  --conf spark.executor.memoryOverhead=6g \
  --conf spark.driver.memoryOverhead=4g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.parquet.filterPushdown=true \
  --conf spark.sql.autoBroadcastJoinThreshold=100MB \
  --conf spark.network.timeout=600s \
  --conf spark.shuffle.compress=true \
  --conf spark.shuffle.spill.compress=true \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=10 \
  --conf spark.dynamicAllocation.maxExecutors=100 \
  my_5tb_etl_job.py
```

---

**This framework covers everything you need to configure Spark for 5 TB workloads in interviews and production!**
