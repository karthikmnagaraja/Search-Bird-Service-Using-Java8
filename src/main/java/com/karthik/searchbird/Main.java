package com.karthik.searchbird;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
public class Main {

 public static void main(String[] args) throws IOException, InterruptedException {

     // the port that the distinguished node will run on.
     int distinguishedPort = 50051;

     
     // this is static configuration which can be made dynamic by reading from environment configuration or through service discovery
     List<String> shards = Arrays.asList("localhost:50052", "localhost:50053");

     SearchBirdServer.Builder builder;
     
     List<SearchBirdServer> remoteServerList= new ArrayList<SearchBirdServer>();
     
     
     // launch the distinguished node
     builder = new SearchBirdServer.Builder(shards, distinguishedPort);
     final SearchBirdServer distinguishedNode = builder.build();

     for(int i=0;i<2;i++){
    	 builder = new SearchBirdServer.Builder(shards, distinguishedPort);
         builder.shardNum(i);
         remoteServerList.add(builder.build());
     }
    
     distinguishedNode.start();
     for(SearchBirdServer remoteServer:remoteServerList)  remoteServer.start();

     distinguishedNode.blockUntilShutdown();
 }
}
