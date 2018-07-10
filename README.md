# DFS-Cache
Distributed High Performance File System with Client Cooperative Cache


#### Configuration 1 :  Clients 2 to 11 ; 3 to 8 Storage servers; File Size - 0.04814 GB

- File split and blocks distribution
1. In these experiments, different files of same size are distributed across constant number of storage servers ranging from 3 to 8. Also no. files split depend on no. clients trying to read blocks, So to setup these environment if there are 4 clients trying to access blocks  then there are 4 different files that needs to be split and distributed across storage servers.
Shell script:    DFSStorDistribute.sh (Increase the number of storage servers)
```
java Storage_Server 4000 Distribute &
```
2. Meta servers or Meta_WriteBlocks takes three arguments filename to be split based on number of rows/size 15000 (Approx 3 MB ) and no storage server 3..n, blocks are stored at path “/DHPFS/storeblocks” and meta files are generated at “/DHPFS/Metadata” during distribution. After block distribution meta-server listen on port 8888 to serve client meta data request.
```
java Meta_WriteBlocks VLFile.sim 15000 n
java Meta_WriteBlocks VLFile1.sim 15000 n
```
3. Storage servers switch to read mode which listen on specific port to accept client block request
Shell script:   DFSrunStor.sh (Increase the number of storage servers based on client ratio)
java Storage_Server 4000 Read-SIO &


- Client Read Blocks: 

4. After block distribution, each client tries to read different files from constant no. storage servers and average client access time is determined.
```
Shell script:  DFSrun.sh (Increase no. clients)
java Client VLFile.sim SIO &
java Client VLFiless.sim SIO &
```

#### Configuration 2: This involves Client cooperative cache, Client_services, Meta-server/service, and Storage servers.

- 1 Client with cooperative cache, Meta-server, 3 Storage servers

1. File split and block distribution can be performed similar to previous experiments, For cooperative caching experiments assume blocks are distributed between 3..n storage servers and stored at “/DHPFS/storeblocks” (Blk0.sim…Blkn.sim). Then each storage server listens on specific ports to serve block requests from different clients.
```
Shell script: DFSrunStor.sh (Increase the number of storage servers based on client ratio)
java Storage_Server 4000 Read-SIO
```
2.  For client cooperative cache experiments another process Meta_Service runs on meta-server which takes no. storage servers as argument, based on which blockids are mapped to different storage servers (host/ports). Also Meta_Service listen on two ports 7777 and 5555 to serve meta-data requests and updates from different clients. 
```
java Meta_Service  3
```
3. Next in order to measure block access time difference between storage server/disk and client cooperative cache, Client_Service(client1) contacts meta-server to get block location(B-table) and read time is measured when client try to read blocks from storage server, put these blocks in cooperative cache, update meta-server. Then Client_Service1 contacts meta-server(C-cache table) and try to read same block from cooperative cache.
```
java Client_Service 2000 Blk0.sim 
java Client_Service1 2001 Blk0.sim
```

#### Configuration 3: Multiple Clients (2 - 10) with cached blocks, Client_services (2 - 10), (2-10) Storage Servers, block size (160kb).

1. File split and block distribution can be performed similar to previous experiments, For cooperative caching experiments assume blocks are distributed between 2..n storage servers based on no. clients and stored at “/DHPFS/storeblocks” (Blk0.sim…Blkn.sim). Then each storage server listens on specific ports to serve block requests from different clients.
```
Shell script: DFSrunStor.sh (Increase the number of storage servers based on client ratio)
java Storage_Server 4000 Read-SIO
```
2.  For client cooperative cache experiments another process Meta_Service runs on meta-server which takes no. storage servers as argument, based on which blockids are mapped to different storage servers (host/ports). Also Meta_Service listen on two ports 7777 and 5555 to serve meta-data requests and updates from different clients. 
```
java Meta_Service  n (no# storage servers)
```
3. Next Client_Service (client2  ... clientn) tries to read different blocks (Blk2.sim to Blk10.sim) of same size from multiple storage servers (2 to n), put these blocks in client cooperative cache and update meta-server. Then in order to determine average block access time from different client cooperative cache, Client_Services1 (client2 .. clientn) are driven through shell scripts to read blocks from specific client cooperative cache. 
```
java Client_Service 2000 Blk0.sim &
#sleep 4
java Client_Service 2001 Blk1.sim &
#sleep 4
#java Client_Service 2002 Blk2.sim &
```
