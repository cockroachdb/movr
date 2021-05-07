# MovR

[MovR](https://www.cockroachlabs.com/docs/v21.1/movr.html) is a fictional vehicle-sharing company.

This repo contains the source code for the Python implementation of the MovR workload generator. 

To use the load generator:

- You can clone this repo and run it directly from your local machine (e.g., `python3 loadmovr.py [commands]`)
- Or, you can pull a pre-built image from Docker Hub (e.g., `docker run -it --rm cockroachdb/movr [commands]`). 

For usage details and a brief tutorial, see below.

## CLI commands

The [load generator](./loadmovr.py) takes one of three commands:

[`load`](#initialize-tables-and-insert-generated-data) - Initializes tables in the database, generates data, and inserts the generated data.

[`run`](#run-a-workload) - Generates fake traffic to the database.

[`configure-multi-region`](#configure-the-database-for-multi-region-features) - Converts a single-region database schema into a [multi-region database schema](https://www.cockroachlabs.com/docs/v21.1/multiregion-overview.html).

For help with the commands and their options, use the `--help` flag.

For a simple tutorial on using the CLI with the Docker image and [`cockroach demo`](https://www.cockroachlabs.com/docs/v21.1/cockroach-demo.html), see below.

## Tutorial

### Set up CockroachDB

[Download CockroachDB](https://www.cockroachlabs.com/docs/v21.1/install-cockroachdb.html), and then [start a virtual, geo-distributed cluster](https://www.cockroachlabs.com/docs/v21.1/simulate-a-multi-region-cluster-on-localhost.html):

```
$ cockroach demo --global --nodes 9 --empty --insecure
```

This command opens a SQL shell to a node on the virtual cluster. **Leave this shell open.** Exiting it will shut down the virtual cluster and erase all of the data in it.

The welcome text for the shell should contain something similar to the following: 

```
# Connection parameters:
#   (console) http://127.0.0.1:8080
#   (sql)     postgres://root:unused@?host=%2Fvar%2Ffolders%2Fc8%2Fb_q93vjj0ybfz0fz0z8vy9zc0000gp%2FT%2Fdemo563941050&port=26257
#   (sql/tcp) postgres://root@127.0.0.1:26257?sslmode=disable
```

This is the connection information for the first node of the cluster. You will use it first to connect to the cluster from the application.

From the shell, create a new database for the MovR application:

```
> CREATE DATABASE movr;
```

### Initialize tables and insert generated data

After you start a cluster and create a database in the cluster, you can use the load generator to initialize the tables in the database, generate some row data, and insert the data into the cluster.

Open a new terminal, and use the application's `load` command against the demo cluster:

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --num-users 100 --num-rides 100 --num-vehicles 10 --city="boston" --city="new york" --city="washington dc" --city="los angeles" --city="san francisco" --city="seattle" --city="amsterdam" --city="paris" --city="rome"
```

The application creates the tables in the database, generates 100 users, 100 rides, and 10 vehicles, and inserts them into the tables, for the 9 different cities specified.

### Run a workload

To run a workload against the cluster, use the application's `run` command:

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" run --city "new york" --city "boston"  --city="washington dc"
```

The application starts a fake-traffic workload against the database, with `"new york"`, `"boston"`, and `"washington dc"` as `city` values for all of the rows of data.

### Configure the database for multi-region features

MovR defaults to a single-region schema. You can also run MovR in a [multi-region configuration](https://www.cockroachlabs.com/docs/v21.1/multiregion-overview.html), to take full advantage of the geo-located data. (*This requires CockroachDB 21.1 or later.*)

The `cockroach demo` command that you used to start the demo cluster created a cluster of nodes spread across 3 different regional localities (`us-east1`, `us-west1`, and `eu-west1`), with 3 nodes in each region (9 total nodes).

Note that the URL that you have provided to the application thus far uses the SQL address of just one of the nodes of the 9-node cluster, in just one of the 3 different regions. 

To get the connection information for other nodes in the cluster, run the following command:

```
$ cockroach node status --insecure
```

You should get output similar to the following:

```
  id |     address     |   sql_address   |  build  |             started_at              |             updated_at              |         locality         | is_available | is_live
-----+-----------------+-----------------+---------+-------------------------------------+-------------------------------------+--------------------------+--------------+----------
   1 | 127.0.0.1:64116 | 127.0.0.1:26257 | v21.1.0 | 2021-05-06 22:01:28.409997+00:00:00 | 2021-05-07 14:59:27.248605+00:00:00 | region=us-east1,az=b     | true         | true
   2 | 127.0.0.1:64121 | 127.0.0.1:26262 | v21.1.0 | 2021-05-06 22:01:29.421414+00:00:00 | 2021-05-07 14:59:23.872051+00:00:00 | region=us-west1,az=c     | true         | true
   3 | 127.0.0.1:64117 | 127.0.0.1:26258 | v21.1.0 | 2021-05-06 22:01:29.421406+00:00:00 | 2021-05-07 14:59:23.803062+00:00:00 | region=us-east1,az=c     | true         | true
   4 | 127.0.0.1:64123 | 127.0.0.1:26264 | v21.1.0 | 2021-05-06 22:01:29.421743+00:00:00 | 2021-05-07 14:59:23.871534+00:00:00 | region=europe-west1,az=c | true         | true
   5 | 127.0.0.1:64118 | 127.0.0.1:26259 | v21.1.0 | 2021-05-06 22:01:29.4223+00:00:00   | 2021-05-07 14:59:23.805847+00:00:00 | region=us-east1,az=d     | true         | true
   6 | 127.0.0.1:64122 | 127.0.0.1:26263 | v21.1.0 | 2021-05-06 22:01:29.43241+00:00:00  | 2021-05-07 14:59:23.869873+00:00:00 | region=europe-west1,az=b | true         | true
   7 | 127.0.0.1:64124 | 127.0.0.1:26265 | v21.1.0 | 2021-05-06 22:01:29.423356+00:00:00 | 2021-05-07 14:59:23.870335+00:00:00 | region=europe-west1,az=d | true         | true
   8 | 127.0.0.1:64120 | 127.0.0.1:26261 | v21.1.0 | 2021-05-06 22:01:29.423194+00:00:00 | 2021-05-07 14:59:23.872502+00:00:00 | region=us-west1,az=b     | true         | true
   9 | 127.0.0.1:64119 | 127.0.0.1:26260 | v21.1.0 | 2021-05-06 22:01:29.423646+00:00:00 | 2021-05-07 14:59:23.870798+00:00:00 | region=us-west1,az=a     | true         | true
(9 rows)
```

Start generating traffic to a node in each region, with `city` values for each region.

For example:

(US West)

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26260/movr?sslmode=disable" run --city "los angeles" --city="san francisco" --city "seattle"
```

(EU West)

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26263/movr?sslmode=disable" run --city="amsterdam" --city="paris" --city="rome"
```

If you already have a workload running against the nodes in the US East locality, then you now have three generators, running queries against three different gateway nodes, in three different regions.

If the database is not properly configured for multiple regions, then the network latency should be pretty high for queries in each of these nodes.

Run the following command to update your database to a multi-region schema, with `"us-east-1"` as the primary region:

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" configure-multi-region --primary-region "us-east-1" 
```
