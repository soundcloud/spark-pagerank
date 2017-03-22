/**
 * Determines the compute and memory layouts given a Spark configuration.
 *
 * This should be run by applying all of the configuration except for the
 * executor settings (as this would waste resources). E.g.:
 *
 * {{
 * crun -i hadoop-cdh5-spark -- spark-shell \
 *   --master "local" \
 *   --deploy-mode "client" \
 *   --driver-memory 1G \
 *   --num-executors 0 \
 *   --conf "spark.dynamicAllocation.enabled=false" \
 *   --conf "spark.storage.memoryFraction=0.6" \
 *   -i sparkTuning.scala
 *  }}
 */

// TODO(jd): incorporate `spark.task.cpus`
def calculate(numExecutors: Int, numCoresPerExecutor: Int, executorMemory: Double): Unit = {
  val totalCores = numExecutors * numCoresPerExecutor
  val totalMemory = numExecutors * executorMemory

  val conf = sc.getConf

  val safetyMemoryFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
  val storageMemoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
  val unrollMemoryFraction = conf.getDouble("spark.storage.unrollFraction", 0.2)
  val shuffleMemoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)

  val safetyMemory = executorMemory * safetyMemoryFraction
  val storageMemory = safetyMemory * storageMemoryFraction
  val unrollMemory = storageMemory * unrollMemoryFraction
  val shuffleMemory = safetyMemory * shuffleMemoryFraction

  val usableStorageMemory = storageMemory - unrollMemory
  val totalUsableStorageMemory = numExecutors * usableStorageMemory

  val workingMemory = safetyMemory - shuffleMemory - storageMemory
  val workingMemoryPerCore = workingMemory / numCoresPerExecutor
  val workingMemoryPerCoreMB = workingMemoryPerCore * 1024

  println("Compute layout:")
  println(s" total cores: $totalCores")

  println("Memory layout:")
  println(f" total memory: $totalMemory%1.2f GB")
  println(f" total usable storage memory: $totalUsableStorageMemory%1.2f GB")
  println(f" usable storage memory per executor: $usableStorageMemory%1.2f GB")
  println(f" working memory per executor: $workingMemory%1.2f GB")
  println(f" working memory per core: $workingMemoryPerCoreMB%1.2f MB")
}

calculate(numExecutors = 256, numCoresPerExecutor = 6, executorMemory = 24.0)
