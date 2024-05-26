docker build -t admin-tools .

docker run --rm -it --network=crypto-net -v $(pwd)/admin-tools:/app admin-tools:latest
