curator-path-cache-test
=======================

h3. Building

```
mvn clean package
```

h3. Running

```
java -Xmx100M -jar target/tester.jar <options>
```

h3. Options

```
Option                      Description                            
------                      -----------                            
--client-qty [Integer]      number of clients to use for ops       
                              (default: 15)                        
--connection-string         If using an external cluster, the      
                              connection string                    
--delete-percent [Integer]  percentage (0-100) of delete ops vs    
                              write ops (default: 25)              
--help                      prints this help                       
--nodes-per-path [Integer]  number of nodes to have in each path   
                              (default: 3000)                      
--ops-per-second [Integer]  number of ops per second (default: 100)
--path-qty [Integer]        number of PathChildrenCache instances  
                              (default: 5)                         
--server-qty [Integer]      number of ZooKeeper instances for      
                              internal cluster. Pass 0 for         
                              external cluster. (default: 3)       
--test-length [Integer]     test length in seconds (default: 60)   
```
