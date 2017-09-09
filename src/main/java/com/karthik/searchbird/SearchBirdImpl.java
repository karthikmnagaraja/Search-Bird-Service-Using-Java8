package com.karthik.searchbird;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.karthik.GetRequest;
import com.karthik.GetResponse;
import com.karthik.PutRequest;
import com.karthik.PutResponse;
import com.karthik.SearchBirdServiceGrpc;
import com.karthik.SearchRequest;
import com.karthik.SearchResponseBatch;
import com.karthik.searchbird.index.Index;

import io.grpc.stub.StreamObserver;
public class SearchBirdImpl extends SearchBirdServiceGrpc.SearchBirdServiceImplBase {
	private Index index;

    public static final int SEARCH_RESPONSE_BATCH_SIZE = 10;
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        Optional<String> result = index.get(request.getKey());

        GetResponse response;
        if (result.isPresent()) {
            response = GetResponse.newBuilder().setFound(true).setValue(result.get()).build();
        } else {
            response = GetResponse.newBuilder().setFound(false).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public SearchBirdImpl(Index index) {
		super();
		this.index = index;
	}

	@Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String result = index.put(request.getKey(), request.getValue());

        PutResponse response;
        if (result != null) {
            response = PutResponse.newBuilder().setHasPrev(true).setPrev(result).build();
        } else {
            response = PutResponse.newBuilder().setHasPrev(false).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void search(SearchRequest request, StreamObserver<SearchResponseBatch> responseObserver) {

        // in a different implementation of querying with pages, this might be an iterator
        // that limits memory to batch size.
        List<List<String>> chunks = Lists.partition(index.search(request.getQuery()), SEARCH_RESPONSE_BATCH_SIZE);

        // we buffer them out to the client as an example of not trying ot send all search results.
        SearchResponseBatch response;
        for (List<String> chunk : chunks) {
            SearchResponseBatch batch = SearchResponseBatch.newBuilder().addAllKey(chunk).build();
            responseObserver.onNext(batch);
        }

        responseObserver.onCompleted();
    }
}
