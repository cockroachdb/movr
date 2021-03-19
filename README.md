# MovR

MovR is a fictional ride sharing company.

## Getting started
First, [download CockroachDB](https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html) and start a local cluster with `cockroach start --insecure --host localhost --background`

Then create the database `movr` with `cockroach sql --insecure --host localhost -e "create database movr;"`

Generating fake data: `docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --num-users 100 --num-rides 100 --num-vehicles 10`

Generating load for cities: `docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" --num-threads 10 run --city "new york" --city "boston"`

## Multi-region configuration
MovR defaults to a single-region schema and single-region queries. You can also run MovR in a multi-region configuration. *This requires CockroachDB 21.1 or later.*

Generating multi-region data: `docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --multi-region`
Running multi-region queries: `docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" run --multi-region`

You can also convert a existing single-region MovR database to a multi-region one without downtime.

Convert to multi-region schema: `docker run -it --rm cockroachdb/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" configure-multi-region`
