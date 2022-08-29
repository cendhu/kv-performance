cd leveldb;
go build;
./leveldb 2&> results.txt
sh ../summary.sh results.txt


cd pebble;
go build;
./pebble 2&> results.txt
sh ../summary.sh results.txt


cd rocksdb;
go build;
./rocksd 2&> results.txt
sh ../summary.sh results.txt
