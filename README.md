# ParallelMargot

ParallelMargot is an Apache Spark software to distribute the computational load of [MARGOT](https://www.sciencedirect.com/science/article/pii/S0957417416304493), a web server for argumentation mining.

### Getting Started

1. Clone or download ParallelMargot in your computer.
2. Download and extract the Tree Kernels package [SVM-Light](http://disi.unitn.it/moschitti/Tree-Kernel.htm) in the main folder of ParallelMargot (next to this README.md file)
3. Rename the Tree Kernels folder "SVM-Light" and compile its content with `make`.
4. ParallelMargot is a maven project, so you can `cd` into the base directory of this project and run `mvn package` to resolve the dependencies and build everything. You should now find the package `ParallelMargot-1.0-jar-with-dependencies.jar` inside the `ParallelMargot/target` folder.

### Run Batch Processing

Run the `spark.examples.BatchMargot` class inside a "Spark-friendly" IDE (e.g. [IntelliJ IDEA](https://www.jetbrains.com/idea/)) or submit the job through the `spark-submit` command as follows:

```
spark-submit \
			--class spark.examples.BatchMargot \
			--master spark://your-spark-master:7077 \
			/path/to/ParallelMargot/target/ParallelMargot-1.0-jar-with-dependencies.jar \
			<inputDir> \
			<pmDir> \
			<outputFile> \
			ecThr=<evidence-claim-threshold> \
			lThr=<link-threshold>  \ 
			repar=<partitions>  
``` 

where the parameters have the following meaning:

- `<inputDir>` is the directory containing the documents to be processed. It can be either a HDFS (e.g., `hdfs://your-spark-master:8020/path/to/inputDirectory`) or traditional folder.
- `<pmDir>` is the base directory of ParallelMargot project -- or wherever the program can find the [stemmed dictionaries](./dict) and the claim/evidence/link [models](./models).
- `<outputFile>` the output file. Again either inside HDFS or traditional file system.	  
- `<evidence-claim-threshold>` the score threshold to asses that a certain sentence is a claim or an evidence (0 is default value)
- `<link-threshold>` the score threshold to asses that there is a link between a claim and an evidence (0 is default value)
- `<partitions>` the number of partition you want to use. As a rule of thumb this should be equal to the number of cores dedicated to this job in your system.

### Run Stream Processing

Run the `spark.examples.StreamingMargot` class inside a "Spark-friendly" IDE (e.g. [IntelliJ IDEA](https://www.jetbrains.com/idea/)) or submit the job through the `spark-submit` command as follows:

```
spark-submit \
			--class spark.examples.StreamingMargot \
			--master spark://your-spark-master:7077 \
			/path/to/ParallelMargot/target/ParallelMargot-1.0-jar-with-dependencies.jar \
			<hostname> \
			<port>
			<pmDir> \
			<checkpoint> \
			ecThr=<evidence-claim-threshold> \
			lThr=<link-threshold>  \ 
			repar=<partitions>  \
			<batchs> \
			<winMultiplier>
``` 

where the parameters have the following meaning:

- `<hostname>` and `<port>` specify the service which is currently emitting the text you want to analyse.
- `<pmDir>` is the base directory of ParallelMargot project -- or wherever the program can find the [stemmed dictionaries](./dict) and the claim/evidence/link [models](./models).
- `<checkpoint>` is the checkpoint directory. You always need one when you work with Spark Streaming.	  
- `<evidence-claim-threshold>` the score threshold to asses that a certain sentence is a claim or an evidence (0 is default value)
- `<link-threshold>` the score threshold to asses that there is a link between a claim and an evidence (0 is default value)
- `<partitions>` the number of partition you want to use. As a rule of thumb this should be equal to the number of cores dedicated to this job in your system.
- `<batchs>` is Spark's micro-batch period in seconds.
- `<winMultiplier>` is the number of micro-batches over which claims and evidence have to be joined (0 is for no "Scope file" pairing).

