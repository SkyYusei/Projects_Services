var inputFile = sc.textFile("s3://f15-p42/twitter-graph.txt")
var lines = inputFile.flatMap(line => line.split("\n")).distinct()
var vertx1 = lines.map(user => (user.split(" ")(1),1))
var vertx2 = lines.map(user => (user.split(" ")(0),0))
var vertx = vertx1  ++ vertx2
var nodes = vertx.reduceByKey(_ + _)
var num_nodes = nodes.map(user => user._1 + "\t"+user._2)
num_nodes.saveAsTextFile("s3://15619project42ruz/task2Output")


