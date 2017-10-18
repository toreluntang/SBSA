# SBSA

## Run Kafka and zookeeper with Docker (Guide not tested yet)
Install docker and swarm.
run "docker swarm init"
run "docker stack deploy -c "docker-compose-single-broker.yml" kafka_zoo

