# MovR

MovR is a fictional ride sharing company. This repo contains datasets and load generators. We plan to make this repo public in time for the CockroachDB 2.1 release.

First, start a local database with `cockroach start --insecure --host localhost --background`

Then create the database movr with `cockroach sql --insecure --host localhost -e "create database movr;"`

Generating fake data: `docker run -it --rm natestewart/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" load --reload-tables --num-users 100 --num-rides 100 --num-vehicles 10`

Generating load for cities: `docker run -it --rm natestewart/movr --url "postgres://root@docker.for.mac.localhost:26257/movr?sslmode=disable" run --city "new york" --city "boston" --num-threads 20`


## Simulate a geo-partitioned MovR deployment  

### Setup the cluster and load data
To complete this section, you must have [roachprod](https://github.com/cockroachdb/roachprod) configured. This dependency will be removed before this is opensourced.

`export FULLNAME="${USER}-test"`

`export RELEASE_VERSION='v2.1.0-alpha.20180702'`

Note that you need the Cockroach dev license and CRL prod tools downloaded prior to starting. 

`export COCKROACH_DEV_LICENSE='crl-0-EJL04ukFGAEiI0NvY2tyb2FjaCBMYWJzIC0gUHJvZHVjdGlvbiBUZXN0aW5n'`

`ssh-add ~/.ssh/google_compute_engine`

`roachprod create ${FULLNAME} --gce-zones us-west1-b,europe-west2-b,us-east1-b --geo --nodes 9 && crl-stage-binaries ${FULLNAME} all scripts && crl-stage-binaries ${FULLNAME} all release && roachprod start ${FULLNAME} --sequential`

-If you run into an error that reads `crl-stage-binaries: command not found` you likely have not installed CRL production tools. To fix this, clone github.com/cockroachlabs/production, and put the crl-prod folder on your path. You might need to run `. ~/.bash_profile` to activate it and ensure you do have it correctly installed. Run crl- and if the various crl commands show up, you have it correctly installed. Re-run the create roachprod command above. 

Make a note of the output here; it includes a mapping of hosts to regions. This will be useful when sending certain types of queries to certain regions.

`roachprod pgurl ${FULLNAME} --external` to get urls. Pick a url and use it to replace "[PGURL]" in the line below. 

Then create the database movr with `cockroach sql --insecure --url [PGURL] -e "create database movr;"`

`docker run -it --rm natestewart/movr --url "[PGURL]/movr?sslmode=disable" load --enable-geo-partitioning --reload-tables`

*note we start movr with the `--enable-geo-partitioning` flag*

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
Movr defaults to 9 cities. Configure the load generator to send a certain cities traffic to the appropriate datacenter using the `--city` flag. Here's the breakdown of cities to partitions.

**Partition "us_east"**: new york, boston, washington dc

**Partition "us_west"**: san francisco, seattle, los angeles

**Partition "eu_west"**: amsterdam, paris, rome

Example: `docker run -it --rm natestewart/movr --url 'postgres://root@[US EAST DATACENTER]:26257/movr?sslmode=disable' run --city "new york" --city "boston" --city "washington dc"`

### Creating simulations where MovR client demand increases

Create a heptio Kubernetes cluster to run many simultaneous MovR containers using a [CloudFormation Template](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=Heptio-Kubernetes&templateURL=https:%2F%2Faws-quickstart.s3.amazonaws.com%2Fquickstart-heptio%2Ftemplates%2Fkubernetes-cluster-with-new-vpc.template)

![K8s Setup](images/k8s-setup.png?raw=true "Kubernetes setup")

When the K8s cluster is complete, go to the `SSHProxyCommand` field and copy the command to ssh into the K8s master. Be sure to update the path to point to your ssh key.

It will look something like: `SSH_KEY="path/to/pm-team-cf.pem"; ssh -i $SSH_KEY -A -L8080:localhost:8080 -o ProxyCommand="ssh -i \"${SSH_KEY}\" ubuntu@35.170.63.215 nc %h %p" ubuntu@10.0.23.45`

`kubectl run movr --image=natestewart/movr -- --url 'postgres://root@35.197.63.199:26257/movr?sslmode=disable' run --city "los angeles" --city "seattle"`

10x load: `ubuntu@ip-10-0-23-45:~$ kubectl scale deployment movr --replicas=10`

![Increase Load](images/scaling-pods.png?raw=true "Scaling K8s pods")

![Web UI](images/scaling-movr.png?raw=true "Web UI")


## Pre-built datasets

### MovR 1M
This datasset contains 1M users, 1M rides, and 100k vehicles.


Import Users
```
IMPORT TABLE users (id UUID NOT NULL, 
city STRING NOT NULL,
name STRING NULL,
address STRING NULL,
credit_card STRING NULL,
CONSTRAINT "primary" PRIMARY KEY (city ASC, id ASC)
)
CSV DATA (
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.0.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.1.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.2.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.3.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.4.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.5.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.6.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.7.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.8.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/users/n1.9.csv');

```
Import vehicles
```
IMPORT TABLE vehicles (
id UUID NOT NULL,
city STRING NOT NULL,
type STRING NULL,
owner_id UUID NULL,
creation_time TIMESTAMP NULL,
status STRING NULL,
ext JSON NULL,
CONSTRAINT "primary" PRIMARY KEY (city ASC, id ASC),
INDEX vehicles_auto_index_fk_city_ref_users (city ASC, owner_id ASC),
INVERTED INDEX ix_vehicle_ext (ext)
)                                                                                                                                                                 
CSV DATA ('https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/vehicles/n1.0.csv');

```

Import rides
```
IMPORT TABLE rides (
id UUID NOT NULL,
city STRING NOT NULL,
vehicle_city STRING NULL,
rider_id UUID NULL,
vehicle_id UUID NULL,
start_address STRING NULL,
end_address STRING NULL,
start_time TIMESTAMP NULL,
end_time TIMESTAMP NULL,
revenue DECIMAL(10,2) NULL,
CONSTRAINT "primary" PRIMARY KEY (city ASC, id ASC),
INDEX rides_auto_index_fk_city_ref_users (city ASC, rider_id ASC),
INDEX rides_auto_index_fk_vehicle_city_ref_vehicles (vehicle_city ASC, vehicle_id ASC),
FAMILY "primary" (id, city, vehicle_city, rider_id, vehicle_id, start_address, end_address, start_time, end_time, revenue),
CONSTRAINT check_vehicle_city_city CHECK (vehicle_city = city)
) 
CSV DATA (
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.0.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.1.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.2.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.3.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.4.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.5.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.6.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.7.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.8.csv', 
'https://s3-us-west-1.amazonaws.com/cockroachdb-movr/datasets/movr-1m/rides/n1.9.csv');
```

Setup and validate integrity constraints
```
ALTER TABLE vehicles ADD CONSTRAINT fk_city_ref_users FOREIGN KEY (city, owner_id) REFERENCES users (city, id);
ALTER TABLE rides ADD CONSTRAINT fk_city_ref_users FOREIGN KEY (city, rider_id) REFERENCES users (city, id);
ALTER TABLE rides ADD CONSTRAINT fk_vehicle_city_ref_vehicles FOREIGN KEY (vehicle_city, vehicle_id) REFERENCES vehicles (city, id);

ALTER TABLE vehicles VALIDATE CONSTRAINT fk_city_ref_users;
ALTER TABLE rides VALIDATE CONSTRAINT fk_city_ref_users;
ALTER TABLE rides VALIDATE CONSTRAINT fk_vehicle_city_ref_vehicles;

```

### Adding geo-partitioning

If your cluster has an enterprise license key, you can enable geo-partitioning.

```
ALTER TABLE users PARTITION BY LIST (city) (
PARTITION us_west VALUES IN (('san francisco'), ('seattle'), ('los angeles')),
PARTITION eu_west VALUES IN (('amsterdam'), ('paris'), ('rome')),
PARTITION us_east VALUES IN (('new york'), ('boston'), ('washington dc'))
);
```


```
ALTER TABLE vehicles PARTITION BY LIST (city) (
PARTITION us_west VALUES IN (('san francisco'), ('seattle'), ('los angeles')),
PARTITION eu_west VALUES IN (('amsterdam'), ('paris'), ('rome')),
PARTITION us_east VALUES IN (('new york'), ('boston'), ('washington dc'))
);
```

```
ALTER TABLE rides PARTITION BY LIST (city) (
PARTITION us_west VALUES IN (('san francisco'), ('seattle'), ('los angeles')),
PARTITION eu_west VALUES IN (('amsterdam'), ('paris'), ('rome')),
PARTITION us_east VALUES IN (('new york'), ('boston'), ('washington dc'))
);
```
