/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sdk.model.ClearScrollRequest;
import org.opensearch.sdk.model.ClearScrollResponse;
import org.opensearch.sdk.model.GetIndexMappingsRequest;
import org.opensearch.sdk.model.GetIndexMappingsResponse;
import org.opensearch.sdk.model.GetIndexSettingRequest;
import org.opensearch.sdk.model.GetIndexSettingResponse;
import org.opensearch.sdk.model.GetSystemSettingRequest;
import org.opensearch.sdk.model.GetSystemSettingResponse;
import org.opensearch.sdk.model.IndexExpression;
import org.opensearch.sdk.model.IndexName;
import org.opensearch.sdk.model.IndexNames;
import org.opensearch.sdk.model.MultiSearchRequest;
import org.opensearch.sdk.model.MultiSearchResponse;
import org.opensearch.sdk.model.ResolveIndicesAndAliasesRequest;
import org.opensearch.sdk.model.ResolveIndicesAndAliasesResponse;
import org.opensearch.sdk.model.ScrollRequest;
import org.opensearch.sdk.model.SearchRequest;
import org.opensearch.sdk.model.SearchResponse;
import org.opensearch.sdk.model.SettingValue;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;


// TODO input validation
/* package private */ class SDKClientImpl implements org.opensearch.sdk.Client {

    private final NodeClient nodeClient;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public SDKClientImpl(
            NodeClient nodeClient,
            ClusterService clusterService,
            IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.nodeClient = nodeClient;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public CompletionStage<SearchResponse> search(SearchRequest searchRequest) {
        return asyncComplete(() -> {
            CompletableFuture<SearchResponse> result = new CompletableFuture<>();
            nodeClient.search(
                toCore(searchRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        });
    }

    @Override
    public CompletionStage<SearchResponse> scroll(ScrollRequest scrollRequest) {
        return asyncComplete(() -> {
            CompletableFuture<SearchResponse> result = new CompletableFuture<>();
            nodeClient.searchScroll(
                toCore(scrollRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        });
    }

    @Override
    public CompletionStage<ClearScrollResponse> clearScroll(ClearScrollRequest clearScrollRequest) {
        return asyncComplete(() -> {
            CompletableFuture<ClearScrollResponse> result = new CompletableFuture<>();
            nodeClient.clearScroll(
                toCore(clearScrollRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        });
    }

    @Override
    public CompletionStage<MultiSearchResponse> multiSearch(MultiSearchRequest multiSearchRequest) {
        return asyncComplete(() -> {
            CompletableFuture<MultiSearchResponse> result = new CompletableFuture<>();
            nodeClient.multiSearch(
                toCore(multiSearchRequest),
                ActionListener.map(
                    complteWithActionListener(result),
                    this::fromCore
                )
            );
            return result;
        });
    }

    @Override
    public CompletionStage<GetIndexMappingsResponse> getIndexMappings(GetIndexMappingsRequest getIndexMappingsRequest) {
        return blockingComplete(() -> {
            String indexName = getIndexMappingsRequest.getIndexName().getValue();
            MappingMetadata mappingMetadata = clusterService.state().metadata().index(indexName).mapping();
            org.opensearch.sdk.model.MappingMetadata metadata = fromCore(mappingMetadata);
            return GetIndexMappingsResponse.builder()
                .mappingMetadata(metadata)
                .build();
        });
    }

    @Override
    public CompletionStage<GetIndexSettingResponse> getIndexSetting(GetIndexSettingRequest getIndexSettingRequest) {
        return blockingComplete(() -> {
            String indexName = getIndexSettingRequest.getIndexName().getValue();
            String settingKey = getIndexSettingRequest.getSettingKey().getValue();
            String settingValue = clusterService.state().metadata().index(indexName).getSettings().get(settingKey);
            return GetIndexSettingResponse.builder()
                .settingValue(SettingValue.builder()
                    .value(settingValue)
                    .build())
                .build();
        });
    }

    @Override
    public CompletionStage<GetSystemSettingResponse> getSystemSetting(GetSystemSettingRequest getSystemSettingRequest) {
      return blockingComplete(() -> {
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
        });
    }

    @Override
    public CompletionStage<ResolveIndicesAndAliasesResponse> resolveIndicesAndAliases(ResolveIndicesAndAliasesRequest resolveIndicesAndAliasesRequest) {
      return blockingComplete(() -> {

            String[] indexExpressions = resolveIndicesAndAliasesRequest.getIndexExpressions()
                .getExpressions()
                .stream()
                .map(IndexExpression::getValue)
                .toArray(String[]::new);

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
        });
    }

    // search

    private org.opensearch.action.search.SearchRequest toCore(SearchRequest request) {
        // TODO
        return null;
    }

    private SearchResponse fromCore(org.opensearch.action.search.SearchResponse response) {
        // TODO
        return null;
    }

    // scroll

    private org.opensearch.action.search.SearchScrollRequest toCore(ScrollRequest request) {
        // TODO
        return null;
    }

    // clear scroll

    private org.opensearch.action.search.ClearScrollRequest toCore(ClearScrollRequest request) {
        // TODO
        return null;
    }

    private ClearScrollResponse fromCore(org.opensearch.action.search.ClearScrollResponse response) {
        // TODO
        return null;
    }

    // multi search

    private org.opensearch.action.search.MultiSearchRequest toCore(MultiSearchRequest request) {
        // TODO
        return null;
    }

    private MultiSearchResponse fromCore(org.opensearch.action.search.MultiSearchResponse response) {
        // TODO
        return null;
    }

    // resolve indices and aliases

    private IndicesOptions toCore(org.opensearch.sdk.model.IndicesOptions indicesOptions) {
        // TODO
        return null;
    }

    // get index mappings

    private org.opensearch.sdk.model.MappingMetadata fromCore(MappingMetadata mappingMetadata) {
        // TODO
        return null;
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
}
