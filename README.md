# MovR

MovR is a fictional ride sharing company. This repo contains datasets and load generators.

Generating fake data: `docker run -it --rm natestewart/movr --url "postgres://root@192.168.65.1:26257/movr?sslmode=disable" --load --reload-tables --num-users 100 --num-rides 100 --num-vehicles 10`

Generating load for cities: `docker run -it --rm natestewart/movr --url "postgres://root@192.168.65.1:26257/movr?sslmode=disable" --city "new york" --city "boston"`

Note that `192.168.65.1` routes to localhost on OSX. [More info here](https://github.com/docker/for-mac/issues/1679)
