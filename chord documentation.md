# Homework 3
[Project Documentation](project_documentation.md)

## Running the programs
- There are 2 main programs to be run. One is the driver program which starts the http server and the other is the driver program which starts the simulation and consequently all client actors.

- In one terminal, first start the server program from the project root directory:
`sbt clean compile "runMain com.chord.HttpServer"`
This will start the server instance on port 8080, along with setting up the required number of nodes in the chord ring in the background, as specified in the `src/main/resources/configuration/serverconfig.conf` file. This will be evident from all the logs appearing in the terminal.

- Then once the server has started and all nodes have been initialized, in another terminal, start the client program from the project root directory:
`sbt "runMain com.chord.Main"`
This will start the main simulation program, which runs for a specific amount of time as obtained from the `src/main/resources/configuration/clientconfig.conf` file, then it will create the required number of client actors (specified in the same configuration file) which keep on making POST/GET requests to the server as explained below.

- After the simulation ends, output states of all chord node actors can be found in the `src/main/resources/outputs/chordState.yml` file, and for the client actors, the number of successful / failed requests made by each client can be found in the `src/main/resources/outputs/client_data.txt` file and the overall success % and failure % can be found in the `src/main/resources/outputs/aggregate.txt` file. Server and client logs can be found in the `src/main/resources/logs/` directory named as "backend.log" and "client.log" respectively.

## Chord node join implementation
- All files referenced below lie in the com.chord package.

- The chord algorithm is implemented in the `Server.scala` source file,  which defines the behavior of a chord node actor.

- When the http server program is run, a guardian behavior is setup, in which an actor is first spawned. This actor represents the parent actor (corresponding to file `Parent.scala`) of all chord node actors

- This parent actor is responsible for spawning all child chord node actors and maintains a map mapping a node hash value to the corresponding actor reference for easy access. Whenever a node in the ring is spawned, the parent uses this map in a circular manner to find the new node's immediate successor and predecessor so that its easier to initialize the new node's finger tables as the new node can now directly contact the successor and predecessor using those actor references. 

- For every node, the finger table is represented using the following 2 maps:
		1. **slotToHash** - this is a map from i (0...m-1) to the hash value of the 'i'th finger.
		2. **hashToRef** - this is a map from the hash value of a finger to its actor reference.
	This allows for easier lookups whenever a finger node needs to be contacted during the node join process. 

- When the first node in the chord ring is spawned, the parent actor makes sure that the new node sets itself as its own successor and predecessor, and that all its finger table entries (0....m-1) reference itself initially.

- For all successive nodes spawned, the parent actor finds the immediate successor and predecessor of the new node from the **slotToAddress** map and initializes it.

- When a node is initialized, it first checks whether it is its own successor at the time or not. 

- If it is, then it fills out its entire finger table with all references set to itself, indicating the 1st node in the ring. 

- Otherwise, there is at least 1 other node in the whole ring. So the new node sets its 0th finger as its immediate successor and starts its *initializeFingerTable* process by contacting its immediate successor and telling it to get the successor for the (( *new_node_hash_value* + 2^1)  %  2^m ) key i.e. its finger 1 (0 indexed). 

- The successor will in turn check if the required key is between itself and its own successor. If it is then it will return its own successor using a *SuccessorFound* message to itself, which in turn will send a *NewSuccessorResponse* message to the new node with the successor actor reference. If the required key is not between itself and its successor, then it will find the closest preceding node to this key from its finger table, and forward the search process to that node *asynchronously*, sending in its own reference as the source of the search process along with the new node's reference. What this entails is that if the successor of the key is found somewhere else in the ring later, then it is **this** node (new node's immediate successor) which will receive a *SuccessorFound* message from the node which actually found the successor for the key somewhere else in the ring, and then **this** node will send the *NewSuccessorResponse* message to the new node with the required actor reference. It should be noted that even though this process is asynchronous, the new node's actor reference is passed around in the ring as the search proceeds, so that when the successor is found, this specific actor (new node) can be directly contacted with a *NewSuccessorResponse* message.

	(**Note** : While checking whether the key falls in the immediate interval or not, if the node is its own successor in the ring, then it will check if the requested key is in the interval clockwise from itself to the new node in which case the new node is itself the successor for that key, and otherwise if the key is in the clockwise interval from the new node to this node then this node is the successor for that key and the result is returned appropriately. The same logic is followed in the scenario where the closest preceding node for the key is found to be itself).

- When the new node receives this finger actor reference for (i=1), it will fill up its (i=1) entry in the finger table and repeat the whole procedure described in the previous point for (i=2), then 3, 4 ... and so on till (m-1).

- Once it receives its finger for (i=m-1) it will fill up its last entry in the finger table maps and start the *predecessorUpdate* procedure so that its predecessors in the ring can update their finger tables if required.

- For this, the process is straightforward. The new node starts by invoking its own findPredecessor function for (i=0) which is analogous to the above successor finding procedure except that the key being passed around now is (*new_node_hash_value* - 2^0) % 2^m. When it receives the successor for this key, it will send an *UpdateFingerTable* message to that node asynchronously. That target node will in turn check if the new node falls in between itself and its immediate successor. If so, then it will reset its immediate successor to the new node, update its finger table maps. When all predecessor nodes have been notified then it means that the new node has successfully joined the ring and sets its flags appropriately. Now it can participate in the data storage / retrieval process.

## Chord data storage / retrieval implementation
- Data being used is a simple movie case class with attributes for name, size and genre of the movie. Each movie name is hashed using the md5 function and an unsigned big integer representation is computed for the resulting hash, whose final result modulo 2^m will be the hash of the movie. Every chord node actor maintains its own immutable set of movies.

- The following http routes are defined in the `UserRoutes.scala` class for posting/getting a movie:
	1. `http://localhost:8080/movies`  with the request body having a movie to send in json format.
	2. `http://localhost:8080/movies/getMovie/___` with the suffix of the route having the specific movie name to query.

- The http server main class creates the actor system having a guardian which spawns the parent actor and links all the above routes to this parent actor.

- The parent actor receives either a *FindNodeForStoringData* message with the movie object to store a movie or a *FindSuccessorToFindData* message to find a movie (which may or may not exist as explained below). Upon receiving one of these, the parent actor will randomly select one of its child chord node actors, and forward it the message.

- When a chord node receives one of the above messages, it will compute the movie hash and invoke the findSuccessor function whose implementation is similar to the node join process, in that:
	1. The requested node checks if the key falls in between itself and its successor, and if it does then it will return its successor, otherwise it will find and forward the search process to the closest preceding node for that key.
	2. If its a request for storing a movie, then the successor actor will simply add the movie to its set of movies and send back a *DataStorageResponseSuccess* message to the user routes actor.
	3. If its a request for finding a movie, then the successor actor (who is assumed to have the movie) will check if any movie name in its set of movies matches the requested movie name and if one does, then a *DataResponseSuccess* message having the movie object is sent back, otherwise a *DataResponseFailure* message is sent back.
	
- The class handling user routes will receive the response and will forward that response to the requesting client.

- The parent actor periodically notifies the chord node actors to send their states to this parent actor, which will then collect all states and dump them in the yaml file named `chordState.yml` file in the `src/main/resources/outputs` directory.

## Simulation
- The client side includes the following actors:
	1. **Simulation** - This is the actor responsible for spawning the client actors and running the simulation.
	2. **HttpClient** - This is the main client actor which makes a post / get request as directed by the simulation actor.
	3. **Counter** - This actor is spawned by a particular client actor and tracks the total  number of successful and failed "get" requests made by that client. A successful request finds a movie stored in the chord ring and a failed request is unable to find a movie in the chord ring.
	4. **Aggregator** - This actor aggregates the total number of successful and failed requests made by all clients and writes the output data to disk when the simulation ends.
	
- The main reason for keeping the http server fairly decoupled from the simulation is that, given the number of chord nodes to be created, the time taken for the whole ring to be initialized can take a lot more time than it does for the simulation to spawn client actors and start sending requests to non-existent chord nodes. This will require additional frequent messages to be passed between the simulation actor and one of the actors in the server side to let the simulation know when the ring is setup fully. Another reason is that for dumping server state periodically, either the simulation has to notify the parent actor (which is in a different actor system) every time about the dump procedure, or the parent actor has to explicitly send collected states back to the simulation across the actor system. Thus to minimize the number of inter-actor-system communication, the simulation is mainly responsible for handling the client side, while the server side takes care of dumping states to disk by itself.

- The simulation starts by spawning the required number of client actors as specified by the input configuration file. Then for the specified amount of simulation time, it will repeat the following procedure:
	1. Randomly select a client from the list of clients spawned for the simulation.
	2. It will generate a random number and if this number if "even" then a post request is to be made, otherwise it is a get request. A movie with a random name, size and genre is created and added to a mutable set of movies maintained by the simulation actor.
	3. For a post request, the selected client is sent a *PostMovie* message with the movie which was just generated.
	4. For a get request, a random movie is selected from the set of movies maintained by the simulation actor and a *GetMovie* message is sent . Note that this movie need not be stored in the chord server previously, as a random movie can just be queried without ever storing in the chord due to the coin-flip like method followed to determine the type of request which will be made by the client.
	
- When the client sends a "get" request and receives a response, it checks if the movie was successfully stored or not. If it was, then it will send a *Success* message to its Counter actor otherwise a *Failed* message.

- The Counter actor keeps track of the success/fail counts of its parent client actor, and after the simulation finishes, it receives a *Finish* message from its client and it will then send an aggregate message with the total counts to the Aggregator actor.

- The aggregator actor maintains a map of client actor reference to the list of success and fail counts. When it receives an aggregate message from all of the counters, it will first write the total success/fail counts of each client to disk in the `client_data.txt` file, and then the overall success and fail % to disk in the `aggregate.txt` file, both in the `src/main/resources/outputs` directory.

## Tests
- There are 2 test files - named `ChordNodeTest.scala` and `ClientServerTest.scala` in the scala test's com.chord package.

- The `ChordNodeTest.scala` has 2 tests which test the successor and predecessor pointers of nodes for two scenarios, one in which there is just 1 node in the ring, and the other in which there are 2 nodes.

- The `ClientServerTest.scala` has 3 tests which test the entire client server functionality by means of the client state which is received by the aggregator actor as a result of POST requests followed by GET requests.

- They can be run using `sbt test`, or `sbt testOnly com.chord.ChordNodeTest` and `sbt testOnly com.chord.ClientServerTest`.  The client server tests will start the server instance on the same port number, so its safer to stop the server instance which maybe already running before running these tests.