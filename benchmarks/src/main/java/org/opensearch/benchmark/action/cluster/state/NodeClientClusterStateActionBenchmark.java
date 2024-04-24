/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.action.cluster.state;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.Version;
import org.opensearch.action.ActionModule;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.cluster.state.TransportClusterStateAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.SearchModule;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;

@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
@State(Scope.Benchmark)
public class NodeClientClusterStateActionBenchmark {

    @Param({"1", "1000"})
    public int numberOfIndicesInClusterState;

    @Param({"1", "1000"})
    public int numberOfFieldMappingsPerIndex;

    private ThreadPool threadPool;
    private Client client;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Setup
    public void setup() throws IOException, ExecutionException {
        Settings settings = Settings.EMPTY;

        SettingsModule settingsModule = new SettingsModule(settings);

        ClusterSettings clusterSettings = settingsModule.getClusterSettings();

        threadPool = new ThreadPool(settings);
        client = new NodeClient(settings, threadPool);

        ActionModule.DynamicActionRegistry dynamicActionRegistry = new ActionModule.DynamicActionRegistry();
        Supplier<String> localNodeId = () -> "benchmark-dummy-node";

        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());

        TransportService transportService = new TransportService(
            settings,
            mockTransport(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );

        RemoteClusterService remoteClusterService = null;

        clusterService = new ClusterService(settings, clusterSettings, threadPool);

        ClusterState state =
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(DiscoveryNodes.builder()
                    .localNodeId(localNodeId.get())
                    .add(
                        new DiscoveryNode(
                            localNodeId.get(),
                            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                            Version.CURRENT
                        )
                    )
                    .build())
                .metadata(
                    buildClusterStateMetadata(
                        numberOfIndicesInClusterState,
                        numberOfFieldMappingsPerIndex
                    )
                )
                .build();

        clusterService.getClusterApplierService().setInitialState(state);

        indexNameExpressionResolver = new IndexNameExpressionResolver(threadPool.getThreadContext());

        TransportClusterStateAction transportClusterStateAction = new TransportClusterStateAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Set.of()),
            indexNameExpressionResolver
        );

        dynamicActionRegistry.registerUnmodifiableActionMap(Map.of(
            ClusterStateAction.INSTANCE,
            transportClusterStateAction
        ));

        ((NodeClient) client).initialize(dynamicActionRegistry, localNodeId, remoteClusterService, namedWriteableRegistry);
    }

    @TearDown
    public void tearDown() {
        threadPool.shutdown();
    }

    @Benchmark
    public void testGetAllIndexMetadataViaClient(Blackhole blackhole) {
        ClusterStateRequestBuilder clusterStateRequest = client.admin().cluster().prepareState();
        clusterStateRequest.setLocal(true);
        clusterStateRequest.setIndices(Metadata.ALL);
        clusterStateRequest.setNodes(false);
        clusterStateRequest.setBlocks(false);
        clusterStateRequest.setCustoms(false);
        clusterStateRequest.setRoutingTable(false);
        clusterStateRequest.setMetadata(true);
        ActionFuture<ClusterStateResponse> clusterStateResponseFuture = client.admin().cluster().state(clusterStateRequest.request());
        ClusterStateResponse clusterStateResponse = clusterStateResponseFuture.actionGet();
        blackhole.consume(clusterStateResponse);
    }

    @Benchmark
    public void testGetAllIndexMetadataViaClusterService(Blackhole blackhole) {
        blackhole.consume(NodeClientClusterStateActionBenchmark.getIndexMetadata(
                    indexNameExpressionResolver, clusterService, Metadata.ALL));
    }

    private static Map<String, IndexMetadata> getIndexMetadata(
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, String indices) {

        ClusterState state = clusterService.state();

        Set<String> resolvedIndices = Arrays.stream(indexNameExpressionResolver.concreteIndices(state, IndicesOptions.LENIENT_EXPAND_OPEN, indices))
            .map(Index::getName)
            .collect(Collectors.toSet());

        Map<String, IndexMetadata> result = new HashMap<>();

        for (Map.Entry<String, IndexMetadata> entry : state.getMetadata().indices().entrySet()) {
            if (resolvedIndices.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Object> buildFieldMappings(int numberOfFieldMappingsPerIndex) {

        Map<String, Object> fieldMappings = new HashMap<>();

        for (int fieldMappingId = 0; fieldMappingId < numberOfFieldMappingsPerIndex; fieldMappingId++) {
            fieldMappings.put("field" + fieldMappingId, randomFieldMapping());
        }

        return fieldMappings;
    }

    private static Metadata buildClusterStateMetadata(
        int numberOfIndicesInClusterState,
        int numberOfFieldMappingsPerIndex
    ) {
        Map<String, IndexMetadata> indexMetadataClusterState = new HashMap<>();

        Map<String, Object> fieldMappings = buildFieldMappings(numberOfFieldMappingsPerIndex);

        for (int indexId = 0; indexId < numberOfIndicesInClusterState; indexId++) {

            String indexName = "index" + indexId;

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);

            indexMetadata.putMapping(new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, fieldMappings));

            indexMetadata.settings(
                Settings.builder()
                    .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                    .build()
            );

            indexMetadata.numberOfShards(1);
            indexMetadata.numberOfReplicas(1);

            indexMetadataClusterState.put(indexName, indexMetadata.build());
        }

        Metadata metadata = Metadata.builder()
            .indices(indexMetadataClusterState)
            .build();

        return metadata;
    }

    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomBoolean() ? "text" : "keyword");
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else if (randomBoolean()) {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        } else if (randomBoolean()) {
            mappings.put("type", "object");
            mappings.put("dynamic", "strict");
            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> props1 = new HashMap<>();
            props1.put("type", randomFrom("text", "keyword"));
            props1.put("analyzer", "keyword");
            properties.put("subtext", props1);
            Map<String, Object> props2 = new HashMap<>();
            props2.put("type", "object");
            Map<String, Object> prop2properties = new HashMap<>();
            Map<String, Object> props3 = new HashMap<>();
            props3.put("type", "integer");
            props3.put("index", "false");
            prop2properties.put("subsubfield", props3);
            props2.put("properties", prop2properties);
            mappings.put("properties", properties);
        } else {
            mappings.put("type", "keyword");
        }
        return mappings;
    }

    @SafeVarargs
    private static <T> T randomFrom(T... items) {
        int index = ThreadLocalRandom.current().nextInt(items.length);
        return items[index];
    }

    private static boolean randomBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    private static Transport mockTransport() {
        return new Transport() {
            @Override
            public void close() {
                mock();
            }

            @Override
            public void setMessageListener(TransportMessageListener listener) {
                mock();
            }

            @Override
            public BoundTransportAddress boundAddress() {
                mock();
                return null;
            }

            @Override
            public Map<String, BoundTransportAddress> profileBoundAddresses() {
                mock();
                return null;
            }

            @Override
            public TransportAddress[] addressesFromString(String address) throws UnknownHostException {
                mock();
                return new TransportAddress[0];
            }

            @Override
            public List<String> getDefaultSeedAddresses() {
                mock();
                return null;
            }

            @Override
            public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Connection> listener) {
                mock();
            }

            @Override
            public TransportStats getStats() {
                mock();
                return null;
            }

            @Override
            public ResponseHandlers getResponseHandlers() {
                return new ResponseHandlers();
            }

            @Override
            public RequestHandlers getRequestHandlers() {
                return new RequestHandlers();
            }

            @Override
            public Lifecycle.State lifecycleState() {
                mock();
                return null;
            }

            @Override
            public void addLifecycleListener(org.opensearch.common.lifecycle.LifecycleListener listener) {
                mock();
            }

            @Override
            public void removeLifecycleListener(LifecycleListener listener) {
                mock();
            }

            @Override
            public void start() {
                mock();
            }

            @Override
            public void stop() {
                mock();
            }

            private void mock() {
                throw new AssertionError("This is a mock and should not be invoked");
            }
        };
    }
}
