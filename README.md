# MovR

MovR is a fictional ride sharing company. This repo contains datasets and load generators. We plan to make this repo public in time for the CockroachDB 2.1 release.

First, start a local database with `cockroach start --insecure --host localhost --background`

Then create the database movr with `cockroach sql --insecure --host localhost -e "create database movr;"`

Generating fake data: `docker run -it --rm natestewart/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" --load --reload-tables --num-users 100 --num-rides 100 --num-vehicles 10`

Generating load for cities: `docker run -it --rm natestewart/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" --city "new york" --city "boston"`


## Simulate a geo-partitioned MovR deployment  

### Setup the cluster and load data
`export ROACHPROD_CLUSTER_NAME="${USER}-test"`

`roachprod create ${FULLNAME} --gce-zones us-west1-b,europe-west2-b,us-east1-b --geo --nodes 9 && crl-stage-binaries ${FULLNAME} all scripts && crl-stage-binaries ${FULLNAME} all release && roachprod start ${FULLNAME} --sequential`

Make a note of the output here; it includes a mapping of hosts to regions. This will be useful when sending certain types of queries to certain regions.

`roachprod pgurl ${FULLNAME} --external` to get urls. Pick a url and use it to replace "[PGURL]" in the line below. 

`docker run -it --rm natestewart/movr --url "[PGURL]/movr?sslmode=disable" --load --enable-ccl-features --reload-tables`

*note we start movr with the `--enable-ccl-features` flag*

### Add partitions
`roachprod ssh ${FULLNAME}:1`

`echo 'constraints: [+region=us-west1]' |  ./cockroach zone set movr.vehicles.us_west --insecure -f -`

`echo 'constraints: [+region=us-west1]' |  ./cockroach zone set movr.users.us_west --insecure -f -`

`echo 'constraints: [+region=us-west1]' |  ./cockroach zone set movr.rides.us_west --insecure -f -`

`echo 'constraints: [+region=us-east1]' |  ./cockroach zone set movr.vehicles.us_east --insecure -f -`

`echo 'constraints: [+region=us-east1]' |  ./cockroach zone set movr.users.us_east --insecure -f -`

`echo 'constraints: [+region=us-east1]' |  ./cockroach zone set movr.rides.us_east --insecure -f -`

`echo 'constraints: [+region=europe-west2]' |  ./cockroach zone set movr.vehicles.eu_west --insecure -f -`

`echo 'constraints: [+region=europe-west2]' |  ./cockroach zone set movr.rides.eu_west --insecure -f -`

`echo 'constraints: [+region=europe-west2]' |  ./cockroach zone set movr.users.eu_west --insecure -f -`

### Send traffic to a specific datacenter
Movr supports 9 cities at the moment. Configure the load generator to send a certain cities traffic to the appropriate datacenter using the `--city` flag. Here's the breakdown of cities to partitions.

**Partition "us_east"**: new york, boston, washington dc

**Partition "us_west"**: san francisco, seattle, los angeles

**Partition "eu_west"**: amsterdam, paris, rome

Example: `docker run -it --rm natestewart/movr --url 'postgres://root@[US EAST DATACENTER]:26257/movr?sslmode=disable' --city "new york" --city "boston" --city "washington dc"`
