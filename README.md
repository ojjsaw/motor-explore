# motor-explore

### functional test/expore minio client
```
mkdir -p ${PWD}/minio/data

docker run \
   -p 9000:9000 \
   -p 9090:9090 \
   --user $(id -u):$(id -g) \
   --name minio1 \
   -e "MINIO_ROOT_USER=ROOTUSER" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   -v ${PWD}/minio/data:/data \
   quay.io/minio/minio server /data --console-address ":9090"
```

```
pip install minio
```

### Not usable mongodb

```
docker run --rm -d -p 27017:27017 mongo:latest
```

```
python -m pip install motor
```

