# MovR

MovR is a fictional ride sharing company. This repo contains datasets and load generators.

Generating fake data: `docker run -it --rm natestewart/movr --url "postgres://root@192.168.65.1:26257/movr?sslmode=disable" --load --reload-tables --num-users 100 --num-rides 100 --num-vehicles 10`

Generating load for cities: `docker run -it --rm natestewart/movr --url "postgres://root@192.168.65.1:26257/movr?sslmode=disable" --city "new york" --city "boston"`

Note that when using the Docker image, `192.168.65.1` routes to localhost on OSX. [More info here](https://github.com/docker/for-mac/issues/1679)


##Setup geo-partitioned cluster  

### Setup the cluster and load data

`roachprod create ${FULLNAME} --gce-zones us-west1-b,europe-west2-b,us-east1-b --geo --nodes 9 && crl-stage-binaries ${FULLNAME} all scripts && crl-stage-binaries ${FULLNAME} all release && roachprod start ${FULLNAME} --sequential`

`roachprod pgurl ${FULLNAME} --external` to get urls

`./loadmovr.py --url "[PGURL]/movr?sslmode=disable" --load --enable-ccl-features --reload-tables`

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

