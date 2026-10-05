/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.quarkus.component.langchain4j.ingest;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Expression;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kamelet.KameletComponent;
import org.apache.camel.component.langchain4j.ingest.LangChain4jIngestHeaders;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.spi.Resource;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.support.builder.ExpressionBuilder;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import org.apache.camel.util.URISupport;
import org.jboss.logging.Logger;

import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.emptyOutcomeTail;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.isSimpleExpression;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.kameletUri;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.parseSteps;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.required;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.routeId;

/**
 * Translates the extension's configuration model — build-time and runtime properties and
 * {@code @Ingest} builder methods — into one composition route per pipeline over the
 * langchain4j-ingest Kamelets: the {@code langchain4j-ingest-file-source} Kamelet (or any
 * consumer URI), through the {@code tika-extract-text-action} or {@code docling-convert-action}
 * Kamelet when a parser is configured, into the {@code langchain4j-ingest-sink} Kamelet, whose
 * engine is the {@code camel-langchain4j-ingest} component. The topology lives in the Kamelet
 * catalog; what stays here is the Quarkus DX: CDI bean resolution with its actionable messages
 * and the configuration-level validations. The route steps between the Kamelets — the pre-parse
 * guards and the Kamelet URI assembly — live in {@link IngestCompositionSupport}.
 */
@ApplicationScoped
public class IngestRoutes extends RouteBuilder {

    /**
     * The built-in register capacity, sized above Camel's 1000-entry default so eviction does
     * not re-ingest large directories during normal operation; in-memory, so lost on restart.
     */
    static final int DEFAULT_REGISTER_CAPACITY = 100_000;

    private static final Logger LOG = Logger.getLogger(IngestRoutes.class);

    private final Set<String> verifiedKamelets = new HashSet<>();

    @Inject
    IngestBuildTimeConfig buildTimeConfig;

    @Inject
    IngestRunTimeConfig runTimeConfig;

    @Inject
    IngestBuilderPipelines builderPipelines;

    // these injection points also keep an unnamed store or model bean from being removed as
    // unused - nothing else in the application need inject it
    @Inject
    @Any
    Instance<EmbeddingStore<TextSegment>> storeCandidates;

    @Inject
    @Any
    Instance<EmbeddingModel> modelCandidates;

    @Override
    public void configure() {
        // a pipeline may be declared entirely through runtime properties - the documented
        // minimum is a directory and nothing else - so the two config roots are unioned. Keying
        // off the build-time map alone would make that configuration a silent no-op, since
        // SmallRye only materialises a map key for the mapping whose structure a property matches
        Set<String> builderDeclared = builderPipelines.entries().stream()
                .map(IngestBuilderPipelines.Entry::name)
                .collect(Collectors.toSet());
        Set<String> names = new TreeSet<>(buildTimeConfig.pipelines().keySet());
        names.addAll(runTimeConfig.pipelines().keySet());
        names.removeAll(builderDeclared);

        for (String name : names) {
            // the name is substituted into the sink Kamelet's inner endpoint URI and into
            // registry references, so it is held to the same charset the builder enforces
            if (!name.matches("[A-Za-z0-9._-]+")) {
                throw new IllegalStateException(
                        "Ingestion pipeline name '" + name + "' may only contain letters, digits, '.', '_' and '-'");
            }
            IngestBuildTimeConfig.PipelineBuildTimeConfig pipeline = buildTimeConfig.pipelines().get(name);
            IngestRunTimeConfig.PipelineRunTimeConfig runtime = runTimeConfig.pipelines().get(name);

            if (runtime != null && !runtime.enabled()) {
                LOG.infof("Ingestion pipeline '%s' is disabled", name);
                continue;
            }

            // a consumer URI says "consume from this"; its absence says "read that directory"
            String uri = pipeline == null ? null : pipeline.source().uri().orElse(null);
            if (uri != null && runtime != null && runtime.source().directory().isPresent()) {
                throw new IllegalStateException("Ingestion pipeline '" + name + "' sets both source.uri ('" + uri
                        + "') and source.directory ('" + runtime.source().directory().get() + "'). A pipeline "
                        + "reads one source: keep the URI, or drop it to read the directory.");
            }

            compositionRoute(name, uri, runtime,
                    resolveStore(name, pipeline == null ? null : pipeline.embeddingStore().orElse(null)),
                    resolveModel(name, pipeline == null ? null : pipeline.embeddingModel().orElse(null)),
                    pipeline == null ? IngestBuildTimeConfig.DEFAULT_MAX_SEGMENT_SIZE : pipeline.maxSegmentSize(),
                    pipeline == null ? IngestBuildTimeConfig.DEFAULT_MAX_OVERLAP_SIZE : pipeline.maxOverlapSize(),
                    pipeline == null ? IngestBuildTimeConfig.DEFAULT_EMBEDDING_BATCH_SIZE : pipeline.embeddingBatchSize(),
                    pipeline == null ? IngestBuildTimeConfig.DEFAULT_MAX_DOCUMENT_SIZE : pipeline.maxDocumentSize(),
                    pipeline == null ? null : pipeline.documentSplitter().orElse(null),
                    pipeline == null ? null : pipeline.parser().orElse(null),
                    pipeline == null ? null : pipeline.modality(),
                    pipeline == null ? null : pipeline.contentType().orElse(null));
        }

        for (IngestBuilderPipelines.Entry entry : builderPipelines.entries()) {
            builderPipeline(entry);
        }
    }

    /**
     * The composition resolves its Kamelets through the regular machinery — the component's
     * location list and any application-provided Kamelet apply — so a missing catalog entry is
     * reported here with the fix, instead of surfacing as a route-creation failure.
     */
    private void requireKamelet(String kamelet) {
        if (!verifiedKamelets.add(kamelet)) {
            return;
        }
        KameletComponent component = getContext().getComponent("kamelet", KameletComponent.class);
        String locations = component.getLocation() != null ? component.getLocation() : "classpath:kamelets";
        for (String base : locations.split(",")) {
            Resource resource = PluginHelper.getResourceLoader(getContext())
                    .resolveResource(base.trim() + "/" + kamelet + ".kamelet.yaml");
            if (resource != null && resource.exists()) {
                return;
            }
        }
        throw new IllegalStateException("Kamelet '" + kamelet + "' is not resolvable from '" + locations
                + "'. The pipelines are composed from the camel-kamelets catalog - check the camel-kamelets"
                + " version and the kamelet component's location configuration.");
    }

    /** The builder's runtime view with the configuration-supplied filters overlaid. */
    private static IngestRunTimeConfig.PipelineRunTimeConfig withFilters(
            IngestRunTimeConfig.PipelineRunTimeConfig base,
            IngestRunTimeConfig.PipelineRunTimeConfig.FilterRunTimeConfig filter) {
        return new IngestRunTimeConfig.PipelineRunTimeConfig() {

            @Override
            public boolean enabled() {
                return base.enabled();
            }

            @Override
            public SourceRunTimeConfig source() {
                return base.source();
            }

            @Override
            public FilterRunTimeConfig filter() {
                return filter;
            }
        };
    }

    /** An {@code @Ingest}-declared pipeline: the builder twin of the configuration path. */
    private void builderPipeline(IngestBuilderPipelines.Entry entry) {
        String name = entry.name();
        // configuration can still switch a builder-declared pipeline off, and the check precedes
        // the invocation so a disabled pipeline's method never runs
        IngestRunTimeConfig.PipelineRunTimeConfig external = runTimeConfig.pipelines().get(name);
        if (external != null && !external.enabled()) {
            LOG.infof("Ingestion pipeline '%s' (builder) is disabled", name);
            return;
        }
        // enabled and the filter.* options are what configuration may say about a builder
        // pipeline; anything about its source would be quietly overruled by the @Ingest method,
        // so it is an error instead (source.recursive cannot be told apart from its default, so
        // it alone goes undetected - Source.recursive() is its builder twin)
        if (external != null && (external.source().directory().isPresent()
                || external.source().documentId().isPresent()
                || external.source().idempotentRepository().isPresent()
                || external.source().idempotentRepositoryAutoCreate())) {
            throw new IllegalStateException("Ingestion pipeline '" + name + "' is declared in Java, so its source "
                    + "comes from the @Ingest method. Remove quarkus.camel.langchain4j.ingest." + name + ".source.* , or "
                    + "declare the pipeline in configuration instead.");
        }

        IngestPipeline definition = builderPipelines.definition(entry);
        String uri = "file".equals(definition.sourceType()) ? null : definition.sourceUri();
        IngestRunTimeConfig.PipelineRunTimeConfig runtime = definition.asRunTimeConfig();
        if (external != null) {
            // the builder exposes no filter API, and unlike source.* the filter options
            // conflict with nothing the @Ingest method declares, so configuration supplies
            // them for builder pipelines too
            runtime = withFilters(runtime, external.filter());
        }
        compositionRoute(name, uri, runtime,
                resolveStore(name, definition.embeddingStoreName().orElse(null)),
                resolveModel(name, definition.embeddingModelName().orElse(null)),
                definition.maxSegmentSize(),
                definition.maxOverlapSize(),
                definition.embeddingBatchSize(),
                definition.maxDocumentSize(),
                definition.documentSplitterName().orElse(null),
                definition.parser().orElse(null),
                definition.modality().orElse(null),
                definition.contentType().orElse(null));
    }

    /**
     * One composition route for both declaration styles: the file-source Kamelet (or the
     * configured consumer URI), through a parser action Kamelet when {@code parser} is set, into
     * the sink Kamelet; with {@code modality=media} the sink embeds the payload whole, without a
     * parser action or the splitter options. The document id is normalised into the
     * {@code CamelLangChain4jIngestDocumentId} header before any parse; the parser actions
     * capture it into the exchange property the sink's endpoint resolves with property-over-header
     * precedence, so a crafted document cannot forge its own identity through parser-copied
     * metadata headers.
     */
    private void compositionRoute(String name, String uri,
            IngestRunTimeConfig.PipelineRunTimeConfig runtime,
            EmbeddingStore<TextSegment> store, EmbeddingModel model,
            int maxSegmentSize, int maxOverlapSize, int embeddingBatchSize, int maxDocumentSize,
            String documentSplitterName, String parserName, String modality, String contentType) {

        // both declaration paths validated the label already; from here the parser is typed
        IngestParser parser = IngestParser.fromLabel(parserName);
        // a media document is embedded whole: nothing to parse, nothing to split. The
        // configuration path reports these at build time; this catches the builder path
        boolean media = "media".equals(modality);
        if (media && parser != null) {
            throw new IllegalStateException("Ingestion pipeline '" + name
                    + "' sets modality 'media' together with a parser. A media document is embedded whole and"
                    + " never parsed; remove one of them.");
        }
        if (media && documentSplitterName != null) {
            throw new IllegalStateException("Ingestion pipeline '" + name
                    + "' sets modality 'media' together with a document splitter. A media document is embedded"
                    + " whole and never split; remove one of them.");
        }
        if (!media && contentType != null) {
            // a content type types a media payload; with text it is a sign that media was forgotten
            throw new IllegalStateException("Ingestion pipeline '" + name
                    + "' sets a content type without modality 'media'. A content type types a media payload;"
                    + " set modality(\"media\") or remove it.");
        }
        String storeRef = bindInstance(name, "store", store);
        String modelRef = bindInstance(name, "model", model);
        String documentId = runtime == null ? null : runtime.source().documentId().orElse(null);
        String repositoryRef = repositoryRef(name, runtime, uri == null);

        requireKamelet("langchain4j-ingest-sink");
        if (parser != null) {
            requireKamelet(parser.actionKamelet());
        }

        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("pipelineName", name);
        if (media) {
            // embedded whole, as one vector: the splitter options do not apply
            sink.put("modality", "media");
            if (contentType != null) {
                sink.put("contentType", contentType);
            }
        } else {
            sink.put("maxSegmentSize", String.valueOf(maxSegmentSize));
            sink.put("maxOverlapSize", String.valueOf(maxOverlapSize));
            sink.put("embeddingBatchSize", String.valueOf(embeddingBatchSize));
        }
        if (maxDocumentSize > 0) {
            sink.put("maxDocumentSize", String.valueOf(maxDocumentSize));
        }
        if (documentSplitterName != null) {
            sink.put("documentSplitter", "#bean:" + documentSplitterName);
        }
        if (runtime != null) {
            filterParams(name, runtime.filter(), sink);
        }
        sink.put("embeddingStore", "#bean:" + storeRef);
        sink.put("embeddingModel", "#bean:" + modelRef);

        if (uri == null) {
            String directory = required(name, runtime == null ? null : runtime.source().directory().orElse(null),
                    "source.directory");
            // substituted into the file-source Kamelet's endpoint URI: a '?' or '#' could
            // inject consumer options - delete=true would consume the user's documents
            if (directory.contains("?") || directory.contains("#")) {
                throw new IllegalStateException("Ingestion pipeline '" + name
                        + "': the directory must not contain '?' or '#' (got '" + directory + "')");
            }
            Map<String, Object> source = new LinkedHashMap<>();
            source.put("directory", directory);
            source.put("recursive", String.valueOf(runtime.source().recursive()));
            if (parser == null && !media) {
                // text is read as UTF-8; a parser or a media model receives the raw bytes instead -
                // the format is its business, and a charset conversion would corrupt a binary document
                source.put("charset", "UTF-8");
            }
            source.put("idempotentRepository", "#bean:" + repositoryRef);

            requireKamelet("langchain4j-ingest-file-source");
            ProcessorDefinition<?> route = from(kameletUri("langchain4j-ingest-file-source", source))
                    .routeId(routeId(name));
            if (documentId != null) {
                // override the source's file-name default; captured before any further step
                route = route.setHeader(LangChain4jIngestHeaders.DOCUMENT_ID,
                        documentIdExpression(documentId));
            }
            route = parseSteps(route, name, parser, maxDocumentSize, true);
            // the file consumer discards the reply and the source register already keeps the
            // same file version from being ingested twice, so no repository goes to the sink
            emptyOutcomeTail(route.to(kameletUri("langchain4j-ingest-sink", sink)), name, parser);
            LOG.infof("Ingestion pipeline '%s': source=file:%s", name, directory);
        } else {
            if (!uri.contains(":")) {
                throw new IllegalStateException(
                        "Ingestion pipeline '" + name + "': '" + uri + "' is not a consumer URI");
            }
            ProcessorDefinition<?> route = from(uri).routeId(routeId(name));
            String documentIdHeader;
            if (documentId != null && isSimpleExpression(documentId)) {
                // evaluated against the exchange the consumer delivered, before any parse
                route = route.setHeader(LangChain4jIngestHeaders.DOCUMENT_ID, documentIdExpression(documentId));
                documentIdHeader = LangChain4jIngestHeaders.DOCUMENT_ID;
            } else {
                // a plain header name goes to the actions and the endpoint as-is; unset keeps
                // the component's canonical default (the 3.40 rename, #9162)
                documentIdHeader = documentId != null ? documentId : IngestHeaders.DOCUMENT_ID;
                // the actions substitute the name into a simple expression, so it is held to a
                // charset that cannot break out of it; anything else is a simple expression
                if (!documentIdHeader.matches("[A-Za-z0-9._-]+")) {
                    throw new IllegalStateException("Ingestion pipeline '" + name + "': source.document-id '"
                            + documentIdHeader + "' is not a plain header name - it may only contain letters,"
                            + " digits, '.', '_' and '-'; write anything else as a $simple{...} expression");
                }
            }
            if (IngestHeaders.DOCUMENT_ID.equals(documentIdHeader)) {
                // the 3.39 name is still read as a fallback: normalised into the canonical
                // header before the actions and the sink, warned once per pipeline
                route = route.process(legacyDocumentIdFallback(name));
            }
            sink.put("documentIdHeader", documentIdHeader);
            IdempotentRepository register = repositoryRef == null
                    ? null
                    : getContext().getRegistry().lookupByNameAndType(repositoryRef, IdempotentRepository.class);
            route = parseSteps(route, name, parser, maxDocumentSize, false, documentIdHeader, register);
            if (repositoryRef != null) {
                // deduplication by document id happens inside the sink's producer: a duplicate
                // is answered SKIPPED, a blank delivery releases its claim
                sink.put("idempotentRepository", "#bean:" + repositoryRef);
            }
            route.to(kameletUri("langchain4j-ingest-sink", sink));
            LOG.infof("Ingestion pipeline '%s': source=%s", name, URISupport.sanitizeUri(uri));
        }
    }

    /**
     * The filter options, forwarded to the sink Kamelet and enforced by the component: id
     * patterns act before the dedup claim, the size floor and the predicate answer
     * {@code FILTERED} and release theirs. Only the predicate bean is validated here — it is
     * the one reference the component would otherwise fail on with a binding error instead of
     * a configuration-level message.
     */
    private void filterParams(String name, IngestRunTimeConfig.PipelineRunTimeConfig.FilterRunTimeConfig filter,
            Map<String, Object> sink) {
        filter.includeId().ifPresent(patterns -> sink.put("includeId", patterns));
        filter.excludeId().ifPresent(patterns -> sink.put("excludeId", patterns));
        if (filter.minDocumentSize() < 0) {
            throw new IllegalStateException("Ingestion pipeline '" + name
                    + "': filter.min-document-size must not be negative (got " + filter.minDocumentSize() + ")");
        }
        if (filter.minDocumentSize() > 0) {
            sink.put("minDocumentSize", String.valueOf(filter.minDocumentSize()));
        }
        filter.documentFilter().ifPresent(beanName -> {
            if (getContext().getRegistry().lookupByNameAndType(beanName, Predicate.class) == null) {
                throw new IllegalStateException("Ingestion pipeline '" + name + "' references document filter '"
                        + beanName + "' but no Predicate bean with that name exists");
            }
            sink.put("documentFilter", "#bean:" + beanName);
        });
    }

    /**
     * Resolves the pipeline's duplicate register to a registry reference: a named bean (existence
     * checked up front, with the configuration-level message), an auto-created in-memory register
     * bound under the configured name, or - for a directory pipeline naming none - a generated
     * built-in one, sized above the file endpoint's default so eviction does not re-ingest large
     * directories.
     */
    private String repositoryRef(String name, IngestRunTimeConfig.PipelineRunTimeConfig runtime, boolean directory) {
        String repositoryName = runtime == null ? null : runtime.source().idempotentRepository().orElse(null);
        boolean autoCreate = runtime != null && runtime.source().idempotentRepositoryAutoCreate();
        if (autoCreate && repositoryName == null) {
            throw new IllegalStateException("Ingestion pipeline '" + name
                    + "' sets source.idempotent-repository-auto-create but no "
                    + "source.idempotent-repository name to create the register under.");
        }
        if (repositoryName != null) {
            IdempotentRepository repository = getContext().getRegistry().lookupByNameAndType(repositoryName,
                    IdempotentRepository.class);
            if (repository == null) {
                if (!autoCreate) {
                    throw new IllegalStateException("Ingestion pipeline '" + name
                            + "' references idempotent repository '" + repositoryName + "' but no such bean exists");
                }
                getContext().getRegistry().bind(repositoryName,
                        MemoryIdempotentRepository.memoryIdempotentRepository(DEFAULT_REGISTER_CAPACITY));
            } else {
                // a CDI-produced repository does not pass through the registry's bind hook, so a
                // CamelContextAware implementation would otherwise run contextless
                CamelContextAware.trySetCamelContext(repository, getContext());
            }
            return repositoryName;
        }
        if (directory) {
            String ref = "langchain4j-ingest-" + name + "-register";
            if (getContext().getRegistry().lookupByNameAndType(ref, IdempotentRepository.class) == null) {
                getContext().getRegistry().bind(ref,
                        MemoryIdempotentRepository.memoryIdempotentRepository(DEFAULT_REGISTER_CAPACITY));
            }
            return ref;
        }
        return null;
    }

    /**
     * When the id lives in the default {@code CamelLangChain4jIngestDocumentId} header, the
     * 3.39 name is still read as a fallback and the deprecation is warned once per pipeline.
     */
    @SuppressWarnings("deprecation")
    private static Processor legacyDocumentIdFallback(String name) {
        AtomicBoolean warned = new AtomicBoolean();
        return exchange -> {
            if (exchange.getMessage().getHeader(IngestHeaders.DOCUMENT_ID) != null) {
                return;
            }
            Object legacy = exchange.getMessage().getHeader(IngestHeaders.LEGACY_DOCUMENT_ID);
            if (legacy != null) {
                exchange.getMessage().setHeader(IngestHeaders.DOCUMENT_ID, legacy);
                if (warned.compareAndSet(false, true)) {
                    LOG.warnf("Ingestion pipeline '%s': document id read via the deprecated %s name"
                            + " - switch the producer to %s",
                            name, IngestHeaders.LEGACY_DOCUMENT_ID, IngestHeaders.DOCUMENT_ID);
                }
            }
        };
    }

    /** Binds a CDI-resolved instance to the registry, so the Kamelet can reference it. */
    private String bindInstance(String name, String what, Object instance) {
        String ref = "langchain4j-ingest-" + name + "-" + what;
        getContext().getRegistry().bind(ref, instance);
        return ref;
    }

    /**
     * A bare header name is read as a header directly rather than parsed: a dotted header name
     * would send the simple parser into OGNL. The expression is initialised here, at route build
     * time — left to reify lazily it would race on the first concurrent exchanges.
     */
    private Expression documentIdExpression(String configured) {
        Expression expression = isSimpleExpression(configured)
                ? ExpressionBuilder.simpleExpression(configured)
                : ExpressionBuilder.headerExpression(configured);
        expression.init(getContext());
        return expression;
    }

    private EmbeddingStore<TextSegment> resolveStore(String name, String configured) {
        return resolve(name, storeCandidates, configured, "embedding store", "embedding-store");
    }

    private EmbeddingModel resolveModel(String name, String configured) {
        return resolve(name, modelCandidates, configured, "embedding model", "embedding-model");
    }

    /**
     * CDI is the one mechanism for both lookups: the named path selects on the qualifier, the
     * unnamed path counts the candidates — through handles, so beans are not instantiated merely
     * to be counted. Picking one silently would bind a pipeline to whichever bean happened to be
     * discovered first. A raw-typed registry search cannot serve here: it never matches a bean
     * typed {@code EmbeddingStore<TextSegment>}.
     */
    private <T> T resolve(String name, Instance<T> candidates, String configured, String what, String property) {
        if (configured != null) {
            Instance<T> named = candidates.select(NamedLiteral.of(configured));
            if (named.isUnsatisfied()) {
                throw new IllegalStateException("Ingestion pipeline '" + name + "' references " + what + " '"
                        + configured + "' but no such bean exists");
            }
            return named.get();
        }
        List<Instance.Handle<T>> handles = StreamSupport.stream(candidates.handles().spliterator(), false)
                .collect(Collectors.toList());
        if (handles.isEmpty()) {
            throw new IllegalStateException("Ingestion pipeline '" + name + "' needs an " + what
                    + ", but no bean of that type exists. Define one, for example with a @Produces method.");
        }
        if (handles.size() > 1) {
            throw new IllegalStateException("Ingestion pipeline '" + name + "' found " + handles.size() + " "
                    + what + " beans. Name the one to use with quarkus.camel.langchain4j.ingest." + name + "."
                    + property);
        }
        return handles.get(0).get();
    }
}
