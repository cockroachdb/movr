# MovR

MovR is a fictional ride sharing company.

## Getting started
First, [download CockroachDB](https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html) and start a local cluster with `cockroach start --insecure --host localhost --background`

Then create the database `movr` with `cockroach sql --insecure --host localhost -e "create database movr;"`

Generating fake data: `docker run -it --rm cockroachdb/movr:20.11.1 --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --num-users 100 --num-rides 100 --num-vehicles 10`

Generating load for cities: `docker run -it --rm cockroachdb/movr:20.11.1 --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" --num-threads 10 run --city "new york" --city "boston"`

## Multi-region configuration
MovR defaults to a single-region schema and single-region queries. You can also run MovR in a multi-region configuration. *This requires CockroachDB 20.1 or later.*

Generating multi-region data: `docker run -it --rm cockroachdb/movr:20.11.1 --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --multi-region`

Running multi-region queries: `docker run -it --rm cockroachdb/movr:20.11.1 --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" run --multi-region`

You can also convert a existing single-region MovR database to a multi-region one without downtime.

Convert to multi-region schema: `docker run -it --rm cockroachdb/movr:20.11.1 --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" configure-multi-region`

By default, the multi-region configuration will not have partitioning enabled, so you will see latency increases if your datacenters are widely distributed.

## Partitioning MovR
MovR can automatically partition multi-region data and apply zone configs using the `partition` command.
Use region-city pairs to map cities to regional partitions and use region-zone pairs to map regional partitions to zones
`docker run -it --rm cockroachdb/movr:20.11.1 --echo-sql --app-name "movr-partition" --url "postgres://root@[ipaddress]/movr?sslmode=disable" partition --region-city-pair us_east:"new york" --region-city-pair central:chicago --region-city-pair us_west:seattle  --region-zone-pair us_east:us-east1 --region-zone-pair central:us-central1 --region-zone-pair us_west:us-west1`

The above `partition` command assumes you loaded MovR with `load --multi-region --city "new york" --city "chicago" --city "seattle"`

If you want to partition by hand (perhaps in a demo), MovR can print the partition commands with the `--preview-queries` command. Example:

```
Partitioning Setting Summary

partition    city
-----------  --------
chicago      chicago
new_york     new york
seattle      seattle

partition    zone where partitioned data will be moved
-----------  -------------------------------------------
new_york     us-east1
chicago      us-central1
seattle      us-west1

reference table    zones where index data will be replicated
-----------------  -------------------------------------------
promo_codes        us-east1
promo_codes        us-central1
promo_codes        us-west1

queries to geo-partition the database
===table and index partitions===
ALTER TABLE vehicles PARTITION BY LIST (city) (PARTITION new_york VALUES IN ('new york' ), PARTITION chicago VALUES IN ('chicago' ), PARTITION seattle VALUES IN ('seattle' ));
ALTER TABLE users PARTITION BY LIST (city) (PARTITION new_york VALUES IN ('new york' ), PARTITION chicago VALUES IN ('chicago' ), PARTITION seattle VALUES IN ('seattle' ));
ALTER TABLE rides PARTITION BY LIST (city) (PARTITION new_york VALUES IN ('new york' ), PARTITION chicago VALUES IN ('chicago' ), PARTITION seattle VALUES IN ('seattle' ));
```


