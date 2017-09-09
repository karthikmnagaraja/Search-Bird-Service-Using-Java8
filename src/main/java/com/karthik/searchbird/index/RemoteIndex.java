package com.karthik.searchbird.index;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.karthik.GetRequest;
import com.karthik.GetResponse;
import com.karthik.PutRequest;
import com.karthik.PutResponse;
import com.karthik.SearchBirdServiceGrpc;
import com.karthik.SearchBirdServiceGrpc.SearchBirdServiceBlockingStub;
import com.karthik.SearchRequest;
import com.karthik.SearchResponseBatch;

import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;


public class RemoteIndex implements Index {
    private static final Logger logger = LoggerFactory.getLogger(RemoteIndex.class);

    private String host;
    private int port;
    private SearchBirdServiceBlockingStub blockingStub;

    public RemoteIndex(String host, int port) {
        this.host = host;
        this.port = port;
        blockingStub = SearchBirdServiceGrpc.newBlockingStub(ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext(true)
            .build()
        );
    }

    public Optional<String> get(String key) {
        GetRequest req = GetRequest.newBuilder().setKey(key).build();
        GetResponse res = null;
        try {
            res = blockingStub.get(req);
        } catch (StatusRuntimeException e) {
            //logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return Optional.empty();
        }
        if (!res.getFound())
            return Optional.empty();
        else
            return Optional.of(res.getValue());
    	
    }

    public String put(String key, String value) {
        PutRequest req = PutRequest.newBuilder().setKey(key).setValue(value).build();
        PutResponse res = null;
        try {
            res = blockingStub.put(req);
        } catch (StatusRuntimeException e) {
            logger.error( "RPC failed: {0}", e.getStatus());
            return null;
        }
        return res.getPrev();
    	
    }

    public List<String> search(String query) {
        List<String> results = new LinkedList<>();

        try {
            SearchRequest req = SearchRequest.newBuilder().setQuery(query).build();
            Iterator<SearchResponseBatch> res = blockingStub.search(req);
            while (res.hasNext()) {
                results.addAll(res.next().getKeyList());
            }
        } catch (StatusRuntimeException e) {
            logger.error( "RPC failed: {0}", e.getStatus());
            return results;
        }
        return results;
    }
}

