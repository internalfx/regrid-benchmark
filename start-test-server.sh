./stop-test-server.sh
docker run -d -p 8080:8080 -p 28015:28015 --name rethinkdbfs-test -v "$PWD/ignored_files/rethinkdb:/data" rethinkdb
