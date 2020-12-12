## Fault tolerance and replication management in overlay networks
- This documentation picks up after the previous [documentation](documentation.md) which explains how the **chord** overlay network is implemented to handle initial node joins and routing of data storage / retrieval requests.

- This documentation mainly explains the implementation of the **Content Addressable Network** and how both *CAN* and *Chord* handle random node failures as well as joins happening dynamically in the network. Other sections highlight the main programs to be run with relevant configurations, use of the *cluster sharding* deployment model, docker configuration and running the simulator in AWS EC2 instance.


### Running the programs
- Similar to the previous one, there are 2 main programs to be run - `com.server.HttpServer` and `com.simulation.Main`.

- They can be run from the project root directory using -
  `sbt "runMain com.server.HttpServer"` and `sbt "runMain com.simulation.Main"`. The server program can take a while to set up the whole CHORD/CAN topology, depending on the `app.NUMBER_OF_NODES` parameter in the `src/main/resources/configuration/serverconfig.conf` file. The client simulation program should only be run after the overlay network is fully set up.


### Content Addressable Network
- As explained in detail in the [CAN](https://people.eecs.berkeley.edu/~sylvia/papers/cans.pdf) paper, a content addressable network manages nodes by assigning zones in a multidimensional coordinate system and every *zone* is responsible for storing key value pairs (i.e. the actual data), where the key is some data attribute and value is a hashed point in the coordinate system.

- As for the implementation, the CAN is implemented as a 2d-coordinate system whose endpoints are configurable and every node's coordinate is in the following format - (start_X, end_X), (start_Y, end_Y).

- The main actors are the Node itself and a parent actor which is responsible for spawning the nodes when the server starts.

- When the first node joins, it is occupies all of the coordinate system, meaning if the CAN endpoints configured are (1, 100), (1, 100), then the 1st node gets the same set of endpoints associated with it. The coordinate is a 2d array of doubles, having 4 elements.

- Whenever a subsequent node is spawned, the parent gives the new node a reference to a randomly selected existing node actor which is already spawned, and which will be split in half depending on the axis of the split.

- The new node sends a JOIN message having the split-axis to this random node, resulting in that node to split its zone along that axis and handing off either the right half (if X-axis is split) or the top half (if Y-axis is split) to the new node. The split axis alternates on every node join, to avoid making a zone too small compared to the rest.

- When an existing node splits, it finds all nodes from its neighbor map which no longer overlap with its coordinates, and sends a *RemoveNeighbor* message to those nodes, indicating that this node is no longer adjacent to those nodes and must be removed from their neighbour maps.

- Every node also maintains a map of references to coordinates of each of its adjacent/neighboring nodes, which changes as new nodes join / an existing node fails. In addition to this, a node also has a map of references to coordinates of its transitive neighbors / neighbors of its neighbors, which is used for the zone *takeover* mechanism when a node failure is detected.

- Every node is scheduled to send the following messages periodically:
  1. **NotifyNeighboursWithNeighbourMap** -  This message is used to keep adjacent nodes updated with this node's current neighbor set, to prevent a non-neighboring node from receiving a *takeover* message from an adjacent node when this node fails, which can happen when neighboring nodes may split or themselves fail and other nodes occupy their zones. This results in inconsistencies when a node receives a *takeover* message for anothe node which is no longer its neighbor.

  2.  **Replicate** - As explained in the replica management section, this message is to periodically update neighboring nodes with all movies this node currently stores.

### Failures in CAN
- Failure of a node actor is detected using the **death watch** mechanism provided by Akka.

- When a node adds another node to its neighbor map, it invokes `context.watch(neighbor)` on that node, to automatically receive a *Terminated* signal along with the node's reference when that node fails.

- When this occurs, every neighbor node initiates the *takeover* mechanism, which proceeds as follows:
  1.  Every node waits for a specific amount of time before sending a *Takeover* message to the neighbor actors of the failed node. This waiting time is considered to be `Area_of_takeover_node + random_time_between_1_and_10` milliseconds, which ensures that the node with the smallest zone is the first to send a takeover message to the other nodes. The random value reduces the chance for nodes having zones of the same area to send a message at the same time. But in case if a node does happen to receive a takeover message from another node whose zone area is the same before it sends one out, then a unique **nodeId** is used to break the tie. This nodeId is maintained by every node as part of its state

  2. When a node receives a *Takeover* message from another node, it does one of the following:
     2.1 If the sender node has a smaller zone area (sent as part of the message), then this recipient node cancels its takeover attempt and sends back a *TakeoverAck* message back to the sending node.

     2.2 If the sender node has the same zone area, then the recipient node compares its  own nodeId with the nodeId of the sending node. If the recipient node has a smaller nodeId, then it wins and it sends back a *TakeoverNack* message to the sending node, otherwise it loses and sends back a *TakeoverAck* message to the sending node, cancelling its own takeover attempt.

  3. When a node has received a *TakeoverAck* message from all of the neighbors of the failed node, it will send back a *TakeoverFinal* message back to those nodes, indicating that it is going to takeover the zone of the failed node, and that it must be added to those nodes' neighbor maps. Then it will transfer all nodes which were neighbors of the failed node, and which currently overlap with this node's new coordinates, to its neighbor map. Also, it will send a *RemoveNeighbor* message to its old neighbors which do not currently overlap with it's new coordinates.

  4. When a node receives at least one *TakeoverNack* message, it will cancel its own takeover attempt.

  5.  As part of taking over another node's zone, a node first checks whether merging the failed node's coordinates with it's own forms a perfect rectangle or not. If it does form a perfect rectangle then takeover proceeds as explained above. Otherwise it will find one of it's currently active neighbors which does form a perfect rectangle with this node (**not the failed node but the node about to takeover**) and it will tell that node to takeover its (**not failed node's**) zone i.e. merge this node's coordinates with it's own. Then this node will takeover the failed node by modifying its coordinates to those of the failed node's, instead of *merging* and expanding them. The main reason for this kind of implementation is that non-rectangular zones will have incorrect start and end coordinates that collide with an adjacent node's coordinates and can result in improper querying of data present in those zones.

- The node joins / failures are randomized by the parent actor which will spawn / stop random nodes in the CAN periodically as specified by the `app.NODE_JOIN_FAILURE_PERDIOD` parameter in the configuration file, which defines the minimum time in seconds between a join and a failure.

### Replication management in CAN
- As part of storing replica of movies to provide some level of fault tolerance in the network, every node periodically updates its neighbors with all movies it currently has.

- Those neighbors will then update their replica map to include those movies. The replica map is a map of node reference to the set of movies assigned to that node's zone.

-  When a node takeovers a neighboring node's zone, it will transfer all movies in its replica map corresponding to the failed node, to its own set of movies, indicating that it owns those movies now.

- As part of responding to a query request for a movie, a node checks both its own set of movies and the corresponding entry in its replica map to check if the movie replica exists or not, to avoid further routing of requests to neighboring zones. Only if a movie does not exist in either of the sets, will the request be routed to the nearest neighbor.

- This replication is configurable using a parameter named `app.REPLICATION_PERIOD` in the `src/main/resources/configuration/serverconfig.conf` file. This parameter determines the scheduling period of the *Replicate* message in seconds, which is used by every node to send their own movies as replica to all neighbors.

### Data storage / retrieval
- When a movie is to be stored in the CAN, it's name is first hashed using the MD5 hash function to get the coordinates for a point in the CAN coordinate system.

- The hash function is applied twice, once for getting the x-coordinate and once for getting the y-coordinate, such that the hashed values fall within the range of the entire coordinate system.

- When a node receives a request for either storing / retrieving a movie, it hashes the movie name and checks if the hashed point is present in it's zone or not. If it is, then it either stores the movie in its set of movies, or searches its set to check if the movie exists or not and returns the movie details if present. If the point does not lie in it's zone, then it finds the nearest neighbor to that hashed point by computing the Euclidean distance between the point and a random point in the neighboring point's zone, and forwards the request to the corresponding node owning that zone.

- The reason for using a random point is to avoid the scenario where a point can be incorrectly mapped to a zone based on its distance from the centermost point in that zone. This can happen because a point can lie near the edge of a zone but maybe nearer to the centermost point of an adjacent zone (which would only partially overlap with this zone), instead of the centermost point of its own zone. Considering a random point reduces the chances of this happening, because a point will eventually be mapped to the correct zone.

- The CAN parent actor periodically pings every node to get their states, each of which has the following:
  1. 2d-coordinates of the node
  2. Set of movies owned by the node
  3. Map of node references to replica set stored by the node
     The parent node aggregates all states and writes them to a yaml file in
     `src/main/resources/outputs/canState.yml`
     The period is determined by the `app.DUMP_PERIOD_IN_SEC` property of the configuration.

### Modification of chord implementation to handle failures
- Similar to the mechanism of handling failures in CAN, the chord failure handling mechanism uses the **death watch** feature of Akka to detect failure of a successor node.

- The parent actor in the chord actor system periodically spawns / stops  a random node in the chord ring as part of the node join / failure implementation. Similar to CAN, the minimum time between successive joins/failures is specified in the `app.NODE_JOIN_FAILURE_PERIOD` in the configuration file.

- To make it easier for a node to track down the next successor node of its own successor when it fails, the chord implementation is modified to store the next successor pointer with every node, which a node first gets when it joins the ring. Every time a new node joins the chord ring, the new node lets its predecessor know that the new node's successor is that predecessor's next successor (successor of successor) to prevent stale entries from existing.

- When a node in the chord ring receives a *Terminated* signal as a result of its successor node actor stopping, it resets its own successor pointer and initializes the reconstruction of its finger table (and consequently of all nodes' finger table) as follows:
  1. If the node's next successor pointer is to itself, then it is the only node left in the chord ring, so it resets it's successor and predecessor pointers to point to itself, and all its finger entries to point to itself.

  2. Otherwise there is at least one other node still active in the ring. The failure detecting node resets its successor pointer to point to its next successor and will initialize its finger table reconstruction process by sending a new *FindSuccessor* message to its new successor. This message includes the hash of the failed node, so as to ignore sending any message to that failed node as part of the reconstruction process.

  3. This findSuccessor mechanism is similar to the one explained in the previous [documentation](documentation.md), except that the failed node will be ignored in the ring using the failed node hash value passed around. Specifically, this failed node's hash value is used while finding the closest preceding node for the given hash. In this case the closest preceding node is the node which more closely precedes the given hash and one which is not the failed node.

  4. When the detecting node finishes reconstructing its entire finger table, it will let its new successor know that its predecessor should be modified to point to this detecting node, and also tells the new successor to reconstruct its own finger table using the same failed node hash.

  5. The new successor will repeat the same process by contacting its own successor by sending the new *FindSuccessor* message, and once it finishes reconstruction, it will tell its successor to begin its reconstruction process in turn.

  6. This process repeats through all nodes in the ring as they reconstruct their finger tables and in the end the detecting node will be pinged by its predecessor telling it to reconstruct. Here the detecting node uses a flag which it set when it detected the failure to know that its reconstruction is already completed and doesn't need to be performed again. **Note** that at any time only 1 node is stopped, so it cannot happen  that reconstruction of finger tables of multiple nodes happen concurrently in the ring as a result of multiple nodes failing at the same time.

- In the end, every node will have reconstructed its finger tables to reflect the failure of the node in the chord ring, so that future requests are routed properly.

### Replication management in chord
- Similar to how it is done in CAN, every node in the chord ring is programmed to periodically send its own set of movies to its predecessor node which will store in its set of movies, to provide a level of fault tolerance in the network.

- As part of the fault tolerance, when a movie query request arrives at a node, it will check if its own set of movies, which include both its own movies as well it's successor node's movies, to avoid routing the request further in the chord ring.

- This periodic replication time is configurable from the `app.REPLICATION_PERIOD` value, which specifies the minimum time in seconds between 2 replication messages by a single node.

### Simulations
- The simulation actor system has an aggregator actor which collects all successful (movie found) and failed (movie not found) client requests and writes the data to disk in `src/main/resources/outputs/aggregate.txt`, as well as the number of successful and failed requests made by each client in `src/main/resources/outputs/client_data.txt`.

- Simulation parameters can be configured in `src/main/resources/configuration/clientconfig.conf`.

- The type of network required in the server can be changed using the `app.TOPOLOGY` parameter in the configuration file. It can have either "CHORD" or "CAN".

- Different 5 minute simulations were run for the following values of replication and node join/failure periods, and success/fail counts were aggregated for the different topologies used.


- The simulation results found are as follows:

  | 		Replication                 |			Join/Fail			        |			CHORD(Success%, Failure %)			|			CAN(Success%, Failure%)			| 	
  |		    -------------	  	        |			-------------		        |			 -------------			            |			-------------		            |
  |			  NO					    |			  NO						|			(48.9%, 51%)						|			(42.8%, 57.14%)					|																				
  |			  NO			            |			Every 20 seconds			|			(44.13%, 55.86%)			        |			(43%, 57%)			            |
  |			  NO			            |			Every 10 seconds			|			(41.3%, 58.67%)			            |			(24.05%, 75.94%)			    |
  |			Every 10 seconds			|			Every 10 seconds			|			(44%, 56%)			                |			(33.87%, 66.13%)		  	    |
  |			Every 5 seconds			    |			Every 10 seconds			|			(48.48%, 51.51%)			        |			(43.22%, 56.78%)			    |
  |			  NO			            |			Every 5 seconds			    |			(33.92%, 66.07%)			        |			(16.95%, 83.05%)			    |
  |			Every 10 seconds			|			Every 5 seconds			    |			(36.64%, 63.35%)			        |			(25.71%, 74.28%)			    |
  |			Every 5 seconds			    |			Every 5 seconds			    |			(38.4%, 61.6%)			            |			(40.4%, 59.5%)			        |


- Some observations which can be made from the above results are as follows:
  1. For 7 out of 8 rows in the results table it is found that CAN success % is less than CHORD success %. Owing to the **cluster sharding** deployment model used with default configuration, the server nodes are not active/spawned until the 1st client request arrives at the parent node and the node actors actually receive a message. At that point, the entire CAN/CHORD ring starts to setup. It is observed that CAN usually takes more time to setup than the CHORD ring, possibly because of the number of messages being passed around for a single node join (to remove / add to neighbor maps and transfer transitive neighbor maps, etc.), resulting in the http client requests timing out more frequently than it does for CHORD ring client requests. Consequently those timeouts get counted as failed requests.

  2. When no replication is used and nodes start joining / failing more frequently (1st 3 rows of the table), as expected the success % for both CHORD and CAN goes down and the failure % goes up. Because if only the owner nodes store their movies and fail, then there is no way to retrieve those movies unless persistence is incorporated and nodes find a way to retrieve their movies from the database. Also CAN success % drops more rapidly than CHORD's when join/failure scheduling period is reduced to 10 seconds from 20 seconds. From the logs, it is observed that the *takeover* mechanism used by CAN nodes for reclaiming zones take a lot more time than the corresponding finger table reconstruction process implemented in the chord network, possibly because the former involves more messages to be passed around for *takeover*, *takeoverAck* and *takeoverNack* and a node doesn't finalize its takeover attempt unless it receives takeoverAck from all other neighbors of the failed node. Now another node might have a long queue of data storage/retrieval request messages in its mailbox, thus resulting in the takeover process getting delayed and the data requests forwarded to the failed node getting stalled.

  3. Last but not least, for the bottom 5 rows, as replication is incorporated more and more frequently the success % for both CHORD and CAN noticeably goes up. As expected, even though nodes are being joined or failing at a faster rate, the replica set stored by chord predecessor nodes and CAN neighboring nodes result in more successful requests than failed requests. The increase in success % is substantially higher in CAN than in CHORD from the 3rd last row (no replication and join/fail every 5 seconds) to the last row (replication every 5 seconds and join/fail every 5 seconds), possibly because in CAN there are multiple neighboring nodes storing the replica, as compared to just 1 predecessor node storing the replica in the CHORD ring, making the CAN more resilient than the CHORD in such scenarios.
  
   
- The above  results were obtained for one of the runs of each simulation locally. A subsequent run may give slightly different results as client requests are randomized along with the node joins and failures, but shouldn't vary much.
     
### Cluster Sharding
- As part of deploying the actor system in a cluster sharded way, the implementation creates one shard region for each parent node created, for either the CHORD network or the CAN. The parent node then divides the nodes spawned in the network into separate shards. This is configurable from the `app.NUMBER_OF_SHARDS` parameter of the configuration.

- This allows the user to start multiple actor systems as different shard regions (each representing one chord network or CAN) and make the client send requests to either one via a configurable parameter for the entityId, which will be used to map the requests to the correct actor system.

- So a request for storing a movie in 1 shard region will look something like this:
````
curl -H "Content-type: application/json" -X POST -d {"""name""":"""avatar""","""size""":500,"""genre""":"""Action-Sci-fi"""} http://localhost:8080/movies/1
````
where the 1 at the end refers to the entityId which will be used to find the specific parent actor (shard region) to route this request to.
Request for storing the same movie in another shard region will be :

````
curl -H "Content-type: application/json" -X POST -d {"""name""":"""avatar""","""size""":500,"""genre""":"""Action-Sci-fi"""} http://localhost:8080/movies/2
````
thus increasing the replicability as the movie is replicated both within a shard region as well as on other shard regions.

- Request for finding the same movie in the 2nd shard region will be like:
  `curl http://localhost:8080/movies/2/getMovie/avatar`.
  

### Docker Image
- Docker image for the network simulator can be pulled using the following command:
  `docker pull p1998/my-overlay-simulator:1.0`

- The image is configured to use CHORD. The following are to be done to run the simulations:
  1. In one terminal start the container using `docker run image_id_hash`. This will start one shard region for the chord network and the server will be exposed on port 8080 inside the container.

  2. In another terminal, enter the container filesystem in interactive mode using `docker exec -it container_id_hash /bin/sh`. Then run `sbt "runMain com.simulation.Main"`. This will start the client simulation program, and concurrently the chord network starting to setup and subsequent requests and responses can be observed until the simulation ends after 100 seconds.

- The above steps can be seen to execute in the EC2 ssh terminal in the video linked below.
  

### AWS EC2 Deployment
- Aws link : https://youtu.be/82Bon_yYhFs

### Tests for CAN
- 2 tests testing the CAN functionalities can be found in the `com.can.CanNodeTest.scala` class in the tests directory.

- 3 tests mentioned in the previous documentation now test both the CAN and CHORD client and server functionality as a whole, which can be configured by changing the `app.TOPOLOGY` parameter in the `src/main/resources/configuration/test.conf` file.