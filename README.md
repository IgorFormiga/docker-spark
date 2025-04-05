# docker-spark

# Running the code (Spark standalone cluster)
You can run the spark standalone cluster by running:
```shell
make run
```
or with 3 workers using:
```shell
make run-scaled
```
You can submit Python jobs with the command:
```shell
make submit app=dir/relative/to/spark_apps/dir
```
e.g. 
```shell
make submit app=src/examples/word_non_null.py
```

There are a number of commands to build the standalone cluster,
you should check the Makefile to see them all. But the
simplest one is:
```shell
make build
```
Remove containers
```shell
make down
```

## Web UIs
The master node can be accessed on:
`localhost:9090`. 
The spark history server is accessible through:
`localhost:18080`.


# Running the code (Spark on Hadoop Yarn cluster)
Before running, check the virtual disk size that Docker
assigns to the container. In my case, I needed to assign
some 70 GB.
You can run Spark on the Hadoop Yarn cluster by running:
```shell
make run-yarn
```
or with 3 data nodes:
```shell
make run-yarn-scaled
```
You can submit an example job to test the setup:
```shell
make submit-yarn-test
```
which will submit the `pi.py` example in cluster mode.

You can also submit a custom job:
```shell
make submit-yarn-cluster app=data_analysis_book/chapter03/word_non_null.py
```

There are a number of commands to build the cluster,
you should check the Makefile to see them all. But the
simplest one is:
```shell
make build-yarn
```

## Web UIs
You can access different web UIs. The one I found the most 
useful is the NameNode UI:
```shell
http://localhost:9870
```

Other UIs:
- ResourceManger - `localhost:8088`
- Spark history server - `localhost:18080`

# About the branch expose-docker-hostnames-to-host
The branch expose-docker-hostnames-to-host contains the 
shell scripts, templates, and Makefile modifications 
required to expose worker node web interfaces. To run 
the cluster you need to do the following. 
1. run
```shell
make run-ag n=3
```
which will generate a docker compose file with 3 worker
nodes and the appropriate yarn and hdfs site files
in the yarn-generated folder.
2. register the docker hostnames with /etc/hosts
```shell
sudo make dns-modify o=true
```
which will create a backup folder with your original
hosts file.

Once you are done and terminate the cluster, restore 
your original hosts file with:
```shell
sudo make dns-restore
```

Referencia: 
- [Setting up a Spark standalone cluster on Docker in layman terms](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b)
