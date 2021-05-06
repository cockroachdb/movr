# MovR

MovR is a fictional vehicle-sharing company.

This repo contains the source code for the Python implementation of the MovR load generator.

## Getting started

First, [download CockroachDB](https://www.cockroachlabs.com/docs/v21.1/install-cockroachdb.html) and [start a local, single-region, 3-node cluster](https://www.cockroachlabs.com/docs/v21.1/cockroach-start.html):

```
$ cockroach start --insecure --store=node1 --listen-addr=localhost:26257 --http-addr=localhost:8080 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=us-east-1 --cache=.25 --max-sql-memory=.25

$ cockroach start --insecure --store=node2 --listen-addr=localhost:26258 --http-addr=localhost:8081 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=us-east-1 --cache=.25 --max-sql-memory=.25

$ cockroach start --insecure --store=node3 --listen-addr=localhost:26259 --http-addr=localhost:8082 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=us-east-1 --cache=.25 --max-sql-memory=.25

$ cockroach init --insecure
```

Then, use the [`cockroach` SQL client](https://www.cockroachlabs.com/docs/v21.1/cockroach-sql.html) to create the `movr` database:

```
$ cockroach sql --url="postgres://root@localhost:26257?sslmode=disable" -e "CREATE DATABASE movr;"
```

To initialize the tables in the database, generate some data, and and insert it into the cluster, run the following command:

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --num-users 100 --num-rides 100 --num-vehicles 10 --city="boston" --city="new york" --city="washington dc" --city="los angeles" --city="san francisco" --city="seattle" --city="amsterdam" --city="paris" --city="rome"
```

The application creates the tables in the database, generates 100 users, 100 rides, and 10 vehicles and inserts them into the tables, for 9 different cities.

To run a workload against the cluster, run the following command:

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" run --city "new york" --city "boston"  --city="washington dc"
```

The application starts a 5-thread workload against the cluster that generates and inserts rows into all tables in the `movr` database, with `"new york"`, `"boston"`, and `"washington dc"` as `city` values.

## Multi-region configuration

MovR defaults to a single-region schema. You can also run MovR in a [multi-region configuration](https://www.cockroachlabs.com/docs/v21.1/multiregion-overview.html). (*This requires CockroachDB 21.1 or later.*)

Add new nodes to the cluster, for each of the new regions you want to add:

(US West)

```
$ cockroach start --insecure --store=node4 --listen-addr=localhost:26260 --http-addr=localhost:8083 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=us-west-1 --cache=.25 --max-sql-memory=.25

$ cockroach start --insecure --store=node5 --listen-addr=localhost:26261 --http-addr=localhost:8084 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=us-west-1 --cache=.25 --max-sql-memory=.25

$ cockroach start --insecure --store=node6 --listen-addr=localhost:26262 --http-addr=localhost:8085 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=us-west-1 --cache=.25 --max-sql-memory=.25
```

(EU West)

```
$ cockroach start --insecure --store=node7 --listen-addr=localhost:26263 --http-addr=localhost:8086 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=eu-west-1 --cache=.25 --max-sql-memory=.25

$ cockroach start --insecure --store=node8 --listen-addr=localhost:26264 --http-addr=localhost:8087 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=eu-west-1 --cache=.25 --max-sql-memory=.25

$ cockroach start --insecure --store=node9 --listen-addr=localhost:26265 --http-addr=localhost:8088 --join=localhost:26257,localhost:26258,localhost:26259 --background --locality=region=eu-west-1 --cache=.25 --max-sql-memory=.25
```

Now the cluster consists of 3 regions (`us-east-1`, `us-west-1`, and `eu-west-1`), with 3 nodes in each region (9 total nodes).

After scaling the database to multiple regions, you can run a workload from each of the new regions:

(US West)

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26258/movr?sslmode=disable" run --city "los angeles" --city="san francisco" --city "seattle"
```

(EU West)

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26259/movr?sslmode=disable" run --city="amsterdam" --city="paris" --city="rome"
```

If you already have a workload running against the nodes in the US East locality, then you now have three generators, running queries against three different gateway nodes, in three different regions.

In order for cockroach to take full advantage of the geo-located data, you need to update the database schema to be [multi-region](https://www.cockroachlabs.com/docs/v21.1/multiregion-overview.html).

Note that you need an enterprise license to use multi-region features. Connect to the cluster and set the license:

```
$ cockroach sql --insecure --url="postgres://root@127.0.0.1:26257" -e "SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Demo'; SET CLUSTER SETTING enterprise.license = 'licensekey';"
```

Run the following command to update your database to a multi-region schema, with `"us-east-1"` as the primary region:

```
$ docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" configure-multi-region --primary-region "us-east-1" 
```
