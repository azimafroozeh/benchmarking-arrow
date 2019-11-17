java -cp ./target/benchmark-arrow-1.0.jar:./benchmark-arrow-1.0.jar:./target/dependency/* com.github.animeshtrivedi.benchmark.Main $@
#./run.sh -i hdfs://localhost:9000/sql/F1/tpcds.pq/store_sales -o hdfs://localhost:9000/sql/arrow/store_sales/ -d hdfs -t ParquetToArrow -p 4 
