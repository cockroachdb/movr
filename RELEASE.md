# Making a new release

1. Clone the repo
1. Check out the commit for the release
1. In the root of the local repository, run:

    ```
    export TAG=<THE_TAG_BEING_PUSHED>
    docker build -t cockroachdb/movr:latest -t cockroachdb/movr:${TAG} .
    docker push cockroachdb/movr:${TAG}
    ```

1. If this version should also be set to the `latest` tag, run:

    ```
    docker build -t cockroachdb/movr:latest .
    docker push cockroachdb/movr:latest
    ```
