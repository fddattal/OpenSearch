/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.sdk.model.Aggregations;
import org.opensearch.sdk.model.ClearScrollRequest;
import org.opensearch.sdk.model.ClearScrollResponse;
import org.opensearch.sdk.model.Data;
import org.opensearch.sdk.model.DocumentId;
import org.opensearch.sdk.model.GetIndexMappingsRequest;
import org.opensearch.sdk.model.GetIndexMappingsResponse;
import org.opensearch.sdk.model.GetIndexSettingRequest;
import org.opensearch.sdk.model.GetIndexSettingResponse;
import org.opensearch.sdk.model.GetSystemSettingRequest;
import org.opensearch.sdk.model.GetSystemSettingResponse;
import org.opensearch.sdk.model.Highlighter;
import org.opensearch.sdk.model.Includes;
import org.opensearch.sdk.model.IndexExpression;
import org.opensearch.sdk.model.IndexExpressions;
import org.opensearch.sdk.model.IndexName;
import org.opensearch.sdk.model.IndexNames;
import org.opensearch.sdk.model.MatchAllQuery;
import org.opensearch.sdk.model.MultiSearchRequest;
import org.opensearch.sdk.model.MultiSearchResponse;
import org.opensearch.sdk.model.Query;
import org.opensearch.sdk.model.ResolveIndicesAndAliasesRequest;
import org.opensearch.sdk.model.ResolveIndicesAndAliasesResponse;
import org.opensearch.sdk.model.Score;
import org.opensearch.sdk.model.ScrollRequest;
import org.opensearch.sdk.model.SearchHit;
import org.opensearch.sdk.model.SearchHits;
import org.opensearch.sdk.model.SearchRequest;
import org.opensearch.sdk.model.SearchResponse;
import org.opensearch.sdk.model.SettingValue;
import org.opensearch.sdk.model.Sorts;
import org.opensearch.sdk.model.Source;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;


// TODO input validation
/**
 * SDKClientImpl is the internal implementation of the org.opensearch.sdk.Client
 * interface which is exposed to plugins.
 */
@InternalApi
public class SDKClientImpl implements org.opensearch.sdk.Client {

    private static final Logger AUDIT_LOGGER = LogManager.getLogger("org.opensearch.sdk.Client.AUDIT");

    private final NodeClient nodeClient;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;

    public SDKClientImpl(
            NodeClient nodeClient,
            ClusterService clusterService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            IndexScopedSettings indexScopedSettings
    ) {
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    public CompletionStage<SearchResponse> search(SearchRequest searchRequest) {
        return audit(searchRequest, asyncComplete(() -> {
            CompletableFuture<SearchResponse> result = new CompletableFuture<>();
            nodeClient.search(
                toCore(searchRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        }));
    }

    @Override
    public CompletionStage<SearchResponse> scroll(ScrollRequest scrollRequest) {
        return audit(scrollRequest, asyncComplete(() -> {
            CompletableFuture<SearchResponse> result = new CompletableFuture<>();
            nodeClient.searchScroll(
                toCore(scrollRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        }));
    }

    @Override
    public CompletionStage<ClearScrollResponse> clearScroll(ClearScrollRequest clearScrollRequest) {
        return audit(clearScrollRequest, asyncComplete(() -> {
            CompletableFuture<ClearScrollResponse> result = new CompletableFuture<>();
            nodeClient.clearScroll(
                toCore(clearScrollRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        }));
    }

    @Override
    public CompletionStage<MultiSearchResponse> multiSearch(MultiSearchRequest multiSearchRequest) {
        return audit(multiSearchRequest, asyncComplete(() -> {
            CompletableFuture<MultiSearchResponse> result = new CompletableFuture<>();
            nodeClient.multiSearch(
                toCore(multiSearchRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        }));
    }

    @Override
    public CompletionStage<GetIndexMappingsResponse> getIndexMappings(GetIndexMappingsRequest getIndexMappingsRequest) {
        return audit(getIndexMappingsRequest, blockingComplete(() -> {
            String indexName = getIndexMappingsRequest.getIndexName().getValue();
            MappingMetadata mappingMetadata = clusterService.state().metadata().index(indexName).mapping();
            org.opensearch.sdk.model.MappingMetadata metadata = fromCore(mappingMetadata);
            return GetIndexMappingsResponse.builder()
                .mappingMetadata(metadata)
                .build();
        }));
    }

    @Override
    public CompletionStage<GetIndexSettingResponse> getIndexSetting(GetIndexSettingRequest getIndexSettingRequest) {
        return audit(getIndexSettingRequest, blockingComplete(() -> {
            String indexName = getIndexSettingRequest.getIndexName()
                .getValue();
            String settingKey = getIndexSettingRequest.getSettingKey()
                .getValue();
            IndexMetadata indexMetadata = clusterService.state()
                .metadata()
                .index(indexName);
            Settings settings = indexMetadata.getSettings();
            Setting<?> coreSettingKey = indexScopedSettings.get(settingKey);
            // TODO: null checking
            String settingValue = indexScopedSettings.copy(settings, indexMetadata).getRaw(coreSettingKey);
            return GetIndexSettingResponse.builder()
                .settingValue(SettingValue.builder()
                    .value(settingValue)
                    .build())
                .build();
        }));
    }

    @Override
    public CompletionStage<GetSystemSettingResponse> getSystemSetting(GetSystemSettingRequest getSystemSettingRequest) {
      return audit(getSystemSettingRequest, blockingComplete(() -> {
            String settingKey = getSystemSettingRequest.getSettingKey().getValue();
            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            Setting<?> coreSettingKey = clusterSettings.get(settingKey);
            // TODO null checking
            String settingValue = clusterSettings.getRaw(coreSettingKey);
            return GetSystemSettingResponse.builder()
                .settingValue(SettingValue.builder()
                    .value(settingValue)
                    .build())
                .build();
        }));
    }

    @Override
    public CompletionStage<ResolveIndicesAndAliasesResponse> resolveIndicesAndAliases(ResolveIndicesAndAliasesRequest resolveIndicesAndAliasesRequest) {
      return audit(resolveIndicesAndAliasesRequest, blockingComplete(() -> {

            String[] indexExpressions  = toCore(resolveIndicesAndAliasesRequest.getIndexExpressions());

            IndicesOptions indicesOptions = toCore(resolveIndicesAndAliasesRequest.getIndicesOptions());

            ClusterState state = clusterService.state();

            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                state, indicesOptions, indexExpressions);

            return ResolveIndicesAndAliasesResponse.builder()
                .indexNames(IndexNames.builder()
                    .names(
                        Arrays.stream(concreteIndices)
                            .map(ci ->
                                IndexName.builder()
                                    .value(ci)
                                    .build()
                                )
                            .collect(Collectors.toList())
                    )
                    .build())
                .build();
        }));
    }

    // search

    private org.opensearch.action.search.SearchRequest toCore(SearchRequest request) {

        org.opensearch.action.search.SearchRequest coreRequest = new org.opensearch.action.search.SearchRequest();

        coreRequest.indicesOptions(toCore(request.getIndicesOptions()));

        if (request.getIndexExpressions() != null) { coreRequest.indices(toCore(request.getIndexExpressions())); }
        if (request.getAllowPartialResults() != null) { coreRequest.allowPartialSearchResults(request.getAllowPartialResults()); }
        if (request.getFrom() != null) { source(coreRequest).from(request.getFrom()); }
        if (request.getSize() != null) { source(coreRequest).size(request.getSize()); }
        if (request.getTimeout() != null) { coreRequest.setCancelAfterTimeInterval(toCore(request.getTimeout())); }
        if (request.getIncludes() != null) { source(coreRequest).fetchSource(toCore(request.getIncludes()), null); /* TODO: excludes */ }
        if (request.getTrackScores() != null) { source(coreRequest).trackScores(request.getTrackScores()); }

        source(coreRequest).query(toCore(request.getQuery()));

        if (request.getSorts() != null) { source(coreRequest).sort(toCore(request.getSorts())); }
        if (request.getAggregations() != null) { addAggregations(coreRequest, request); }
        if (request.getHighlighter() != null) { source(coreRequest).highlighter(toCore(request.getHighlighter())); }

        return coreRequest;
    }

    private SearchSourceBuilder source(org.opensearch.action.search.SearchRequest coreRequest) {
        if (coreRequest.source() != null) {
            return coreRequest.source();
        }
        SearchSourceBuilder result = SearchSourceBuilder.searchSource();
        coreRequest.source(result);
        return result;
    }

    private TimeValue toCore(org.opensearch.sdk.model.TimeValue timeValue) {
        throw new UnsupportedOperationException();
    }

    private String[] toCore(Includes includes) {
        throw new UnsupportedOperationException();
    }

    private QueryBuilder toCore(Query query) {
        if (query instanceof MatchAllQuery) {
            return QueryBuilders.matchAllQuery();
        }
        throw new UnsupportedOperationException();
    }

    private SortBuilder<?> toCore(Sorts sorts) {
        throw new UnsupportedOperationException();
    }

    private HighlightBuilder toCore(Highlighter highlighter) {
        throw new UnsupportedOperationException();
    }

    private void addAggregations(org.opensearch.action.search.SearchRequest core, SearchRequest sdk) {
        throw new UnsupportedOperationException();
    }

    private SearchResponse fromCore(org.opensearch.action.search.SearchResponse response) {

        // TODO aggregations, etc.
        return SearchResponse.builder()
            .hits(
                SearchHits.builder()
                    .searchHits(
                        Arrays.stream(response.getHits().getHits())
                            .map(hit ->
                                SearchHit.builder()
                                    .indexName(IndexName.builder().value(hit.getIndex()).build())
                                    .documentId(DocumentId.builder().value(hit.getId()).build())
                                    .score(Score.builder().value(hit.getScore()).build())
                                    .source(Source.builder()
                                        .value(Data.wrap(hit.getSourceAsMap()))
                                        .build())
                                    .build()
                            )
                            .collect(Collectors.toList())
                    )
                    .build()
            )
            .build();
    }

    // scroll

    private org.opensearch.action.search.SearchScrollRequest toCore(ScrollRequest request) {
        // TODO
        throw new UnsupportedOperationException();
    }

    // clear scroll

    private org.opensearch.action.search.ClearScrollRequest toCore(ClearScrollRequest request) {
        // TODO
        throw new UnsupportedOperationException();
    }

    private ClearScrollResponse fromCore(org.opensearch.action.search.ClearScrollResponse response) {
        // TODO
        throw new UnsupportedOperationException();
    }

    // multi search

    private org.opensearch.action.search.MultiSearchRequest toCore(MultiSearchRequest request) {
        // TODO
        throw new UnsupportedOperationException();
    }

    private MultiSearchResponse fromCore(org.opensearch.action.search.MultiSearchResponse response) {
        // TODO
        throw new UnsupportedOperationException();
    }

    // resolve indices and aliases

    private IndicesOptions toCore(org.opensearch.sdk.model.IndicesOptions indicesOptions) {
        // TODO
        return IndicesOptions.lenientExpandOpen();
    }

    private String[] toCore(IndexExpressions indexExpressions) {
        return indexExpressions
            .getExpressions()
            .stream()
            .map(IndexExpression::getValue)
            .toArray(String[]::new);
    }

    // get index mappings

    private org.opensearch.sdk.model.MappingMetadata fromCore(MappingMetadata mappingMetadata) {
        Map<String, Object> map = mappingMetadata.getSourceAsMap();
        return org.opensearch.sdk.model.MappingMetadata.builder()
            .source(Data.wrap(map))
            .build();
    }

    private <T> CompletionStage<T> blockingComplete(CheckedSupplier<T, Exception> resultSupplier) {
        try {
            return success(resultSupplier.get());
        } catch (Exception e) {
            return failure(e);
        }
    }

    private <T> CompletionStage<T> asyncComplete(CheckedSupplier<CompletionStage<T>, Exception> resultSupplier) {
        try {
            return resultSupplier.get();
        } catch (Exception e) {
            return failure(e);
        }
    }

    private <T> CompletionStage<T> success(T result) {
        return CompletableFuture.completedStage(result);
    }

    private <T> CompletionStage<T> failure(Exception e) {
        CompletableFuture<T> result = new CompletableFuture<T>();
        result.completeExceptionally(e);
        return result;
    }

    private <T> ActionListener<T> complteWithActionListener(CompletableFuture<T> future) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T t) {
                future.complete(t);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        };
    }

    private <I, O> CompletionStage<O> audit(I request, CompletionStage<O> responseFuture) {
        return responseFuture.thenApply(response -> {
            AUDIT_LOGGER.info("{} -> {}", request, response);
            return response;
        });
    }
}
