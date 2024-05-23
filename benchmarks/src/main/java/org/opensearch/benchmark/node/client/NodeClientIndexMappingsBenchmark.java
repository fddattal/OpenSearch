/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.node.client;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.Version;
import org.opensearch.action.ActionModule;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.opensearch.action.admin.indices.mapping.get.TransportGetMappingsAction;
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
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.search.SearchModule;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectionProfile;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.TransportStats;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;

@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
@State(Scope.Benchmark)
public class NodeClientIndexMappingsBenchmark {

    @Param({"1_000:1_000", "10_000:10_000"})
    public String config;

    private ThreadPool threadPool;
    private Client client;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    public static void main(String[] args) throws IOException, ExecutionException {
        NodeClientIndexMappingsBenchmark benchmark = new NodeClientIndexMappingsBenchmark();
        Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        benchmark.config = 1000 + ":" + 1000;
        benchmark.setup();
        for (int i = 0; i < 10_000; i++) {
            benchmark.testGetAllIndexMetadataViaClient(blackhole);
            benchmark.testGetAllIndexMetadataViaClusterService(blackhole);
        }
        benchmark.tearDown();
    }

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

        String[] split = this.config.split(":");
        int numberOfIndicesInClusterState = Integer.parseInt(split[0].replaceAll("_", ""));
        int numberOfFieldMappingsPerIndex = Integer.parseInt(split[1].replaceAll("_", ""));

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

        IndicesService indicesService = new IndicesService(
            settings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            threadPool,
            null,
            null,
            null,
            null,
            clusterService,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ) {
            @Override
            public Function<String, Predicate<String>> getFieldFilter() {
                return MapperPlugin.NOOP_FIELD_FILTER;
            }
        };

        TransportGetMappingsAction action = new TransportGetMappingsAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Set.of()),
            indexNameExpressionResolver,
            indicesService
        );

        dynamicActionRegistry.registerUnmodifiableActionMap(Map.of(
            GetMappingsAction.INSTANCE,
            action
        ));

        ((NodeClient) client).initialize(dynamicActionRegistry, localNodeId, remoteClusterService, namedWriteableRegistry);
    }

    @TearDown
    public void tearDown() {
        threadPool.shutdown();
    }

    @Benchmark
    public void testGetAllIndexMetadataViaClient(Blackhole blackhole) {
        Map<String, MappingMetadata> result = client.admin().indices().prepareGetMappings()
                .setLocal(true)
                .setIndices("*")
                .get()
                .getMappings();
        blackhole.consume(result);
    }

    @Benchmark
    public void testGetAllIndexMetadataViaClusterService(Blackhole blackhole) throws IOException {

        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
            clusterService.state(), IndicesOptions.LENIENT_EXPAND_OPEN, "*");

        Map<String, MappingMetadata> result = clusterService.state()
            .metadata()
            .findMappings(concreteIndices, MapperPlugin.NOOP_FIELD_FILTER);

        blackhole.consume(result);
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

        MappingMetadata mappingMetadata = new MappingMetadata(
            MapperService.SINGLE_MAPPING_NAME, buildFieldMappings(numberOfFieldMappingsPerIndex));

        Settings settings = Settings.builder()
            .put(SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();

        for (int indexId = 0; indexId < numberOfIndicesInClusterState; indexId++) {

            String indexName = "index" + indexId;

            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);

            indexMetadata.putMapping(mappingMetadata);

            indexMetadata.settings(settings);

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
