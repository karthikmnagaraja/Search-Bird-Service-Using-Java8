package com.karthik.searchbird;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.karthik.searchbird.index.CompositeIndex;
import com.karthik.searchbird.index.Index;
import com.karthik.searchbird.index.RemoteIndex;
import com.karthik.searchbird.index.ResidentIndex;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class SearchBirdServer {
	 private static final Logger logger = LoggerFactory.getLogger(SearchBirdServer.class);
	private final List<String> shards; // addresses of shards. for every service.
    private final Optional<Integer> shard; // if shard, maps to the collection of shards.
    private final Server server;

    private SearchBirdServer(
            List<String> shards,
            Optional<Integer> shard,
            SearchBirdImpl searchbird,
            Server server) {
        this.shards = shards;
        this.shard = shard;
        this.server = server;
    }

    public static class Builder {
        private final List<String> nestedShards; // addresses of shards. for every service.
        private Optional<Integer> nestedShard; // if shard, maps to the collection of shards.
        private int port;

        public Builder(List<String> shards, int distinguishedPort) {
            nestedShards = shards;
            nestedShard = Optional.empty();
            port = distinguishedPort;
        }

         //declaring a shard number overrides the default port.
        public Builder shardNum(int i) {
            nestedShard = Optional.of(i);
            port = Integer.parseInt(nestedShards.get(i).split(":")[1]);
            return this;
        }

        public SearchBirdServer build() {
            Index index;
            if (nestedShard.isPresent())
                index = new ResidentIndex();
            else {
                Collection<Index> remotes = nestedShards.stream()
                        .map(x -> x.split(":"))
                        .map(addr -> new RemoteIndex(addr[0], Integer.parseInt(addr[1])))
                        .collect(Collectors.toList());
                index = new CompositeIndex(remotes);
            }

            SearchBirdImpl searchbird = new SearchBirdImpl(index);
         

            return new SearchBirdServer(
                    nestedShards,
                    nestedShard,
                    searchbird,
                    ServerBuilder.forPort(port).addService(searchbird).build()
            );
        }
    }

    public void start() throws IOException {
        logger.info("Starting search bird  server with node " + shard);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
            	logger.warn("*** shutting down Searchbird!");
                SearchBirdServer.this.stop();
                logger.warn("*** searchbird shut down!");
            }
        });
    }

    public void stop() {
        if (!server.isShutdown())
            server.shutdown();
    }

    public void blockUntilShutdown() throws InterruptedException { server.awaitTermination(); }
}
