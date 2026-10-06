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

import java.util.List;
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
import org.apache.camel.component.langchain4j.ingest.LangChain4jIngestHeaders;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.builder.ExpressionBuilder;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import org.apache.camel.util.URISupport;
import org.jboss.logging.Logger;

import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.FILE_SOURCE_KAMELET;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.SINK_KAMELET;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.documentIdHeader;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.emptyOutcomeTail;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.fileSourceParameters;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.generatedName;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.isSimpleExpression;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.kameletUri;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.parseSteps;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.registerRef;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.routeId;
import static org.apache.camel.quarkus.component.langchain4j.ingest.IngestCompositionSupport.sinkParameters;

/**
 * Translates the extension's configuration model — build-time and runtime properties and
 * {@code @Ingest} builder methods — into one {@link PipelineSpec} per pipeline, and each spec into
 * one composition route over the langchain4j-ingest Kamelets: the
 * {@code langchain4j-ingest-file-source} Kamelet (or any consumer URI), through the
 * {@code tika-extract-text-action} or {@code docling-convert-action} Kamelet when a parser is
 * configured, into the {@code langchain4j-ingest-sink} Kamelet, whose engine is the
 * {@code camel-langchain4j-ingest} component. The topology lives in the Kamelet catalog; what
 * stays here is the Quarkus DX: CDI bean resolution with its actionable messages. The route steps
 * between the Kamelets and their parameters live in {@link IngestCompositionSupport}.
 */
@ApplicationScoped
public class IngestRoutes extends RouteBuilder {

    /**
     * The built-in register capacity, sized above Camel's 1000-entry default so eviction does
     * not re-ingest large directories during normal operation; in-memory, so lost on restart.
     */
    static final int DEFAULT_REGISTER_CAPACITY = 100_000;

    private static final Logger LOG = Logger.getLogger(IngestRoutes.class);

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
            IngestRunTimeConfig.PipelineRunTimeConfig runtime = runTimeConfig.pipelines().get(name);
            if (runtime != null && !runtime.enabled()) {
                LOG.infof("Ingestion pipeline '%s' is disabled", name);
                continue;
            }
            compositionRoute(PipelineSpec.fromConfig(name, buildTimeConfig.pipelines().get(name), runtime));
        }

        for (IngestBuilderPipelines.Entry entry : builderPipelines.entries()) {
            builderPipeline(entry);
        }
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
        compositionRoute(PipelineSpec.fromBuilder(name, builderPipelines.definition(entry), external));
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
    private void compositionRoute(PipelineSpec spec) {
        String name = spec.name();
        // bound under the names the sink Kamelet references
        getContext().getRegistry().bind(generatedName(name, "store"), resolveStore(spec));
        getContext().getRegistry().bind(generatedName(name, "model"), resolveModel(spec));
        IdempotentRepository register = register(spec);
        String documentFilter = spec.filters().documentFilter();
        if (documentFilter != null
                && getContext().getRegistry().lookupByNameAndType(documentFilter, Predicate.class) == null) {
            // the one filter reference the component would otherwise fail on with a binding
            // error instead of a configuration-level message
            throw new IllegalStateException("Ingestion pipeline '" + name + "' references document filter '"
                    + documentFilter + "' but no Predicate bean with that name exists");
        }
        String sink = kameletUri(SINK_KAMELET, sinkParameters(spec));

        if (spec.directory() != null) {
            ProcessorDefinition<?> route = from(kameletUri(FILE_SOURCE_KAMELET, fileSourceParameters(spec)))
                    .routeId(routeId(name));
            if (spec.documentId() != null) {
                // override the source's file-name default; captured before any further step
                route = route.setHeader(LangChain4jIngestHeaders.DOCUMENT_ID, documentIdExpression(spec.documentId()));
            }
            // no register for the parse stage: the source's own register already filtered duplicates out
            route = parseSteps(route, spec, null);
            emptyOutcomeTail(route.to(sink), spec);
            LOG.infof("Ingestion pipeline '%s': source=file:%s", name, spec.directory());
        } else {
            ProcessorDefinition<?> route = from(spec.uri()).routeId(routeId(name));
            if (spec.documentId() != null && isSimpleExpression(spec.documentId())) {
                // evaluated against the exchange the consumer delivered, before any parse
                route = route.setHeader(LangChain4jIngestHeaders.DOCUMENT_ID, documentIdExpression(spec.documentId()));
            }
            if (IngestHeaders.DOCUMENT_ID.equals(documentIdHeader(spec))) {
                // the 3.39 name is still read as a fallback: normalised into the canonical
                // header before the actions and the sink, warned once per pipeline
                route = route.process(legacyDocumentIdFallback(name));
            }
            route = parseSteps(route, spec, register);
            route.to(sink);
            LOG.infof("Ingestion pipeline '%s': source=%s", name, URISupport.sanitizeUri(spec.uri()));
        }
    }

    /**
     * Binds or checks the pipeline's duplicate register: a named bean (existence checked up front,
     * with the configuration-level message), an auto-created in-memory register bound under the
     * configured name, or - for a directory pipeline naming none - a generated built-in one, sized
     * above the file endpoint's default so eviction does not re-ingest large directories.
     */
    private IdempotentRepository register(PipelineSpec spec) {
        String ref = registerRef(spec);
        if (ref == null) {
            return null;
        }
        IdempotentRepository repository = getContext().getRegistry().lookupByNameAndType(ref,
                IdempotentRepository.class);
        if (repository == null) {
            if (spec.idempotentRepository() != null && !spec.idempotentRepositoryAutoCreate()) {
                throw new IllegalStateException("Ingestion pipeline '" + spec.name()
                        + "' references idempotent repository '" + ref + "' but no such bean exists");
            }
            repository = MemoryIdempotentRepository.memoryIdempotentRepository(DEFAULT_REGISTER_CAPACITY);
            getContext().getRegistry().bind(ref, repository);
        } else if (spec.idempotentRepository() != null) {
            // a CDI-produced repository does not pass through the registry's bind hook, so a
            // CamelContextAware implementation would otherwise run contextless
            CamelContextAware.trySetCamelContext(repository, getContext());
        }
        return repository;
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

    private EmbeddingStore<TextSegment> resolveStore(PipelineSpec spec) {
        return resolve(spec, storeCandidates, spec.embeddingStore(), "embedding store", "embedding-store",
                "embeddingStore");
    }

    private EmbeddingModel resolveModel(PipelineSpec spec) {
        return resolve(spec, modelCandidates, spec.embeddingModel(), "embedding model", "embedding-model",
                "embeddingModel");
    }

    /**
     * CDI is the one mechanism for both lookups: the named path selects on the qualifier, the
     * unnamed path counts the candidates — through handles, so beans are not instantiated merely
     * to be counted. Picking one silently would bind a pipeline to whichever bean happened to be
     * discovered first. A raw-typed registry search cannot serve here: it never matches a bean
     * typed {@code EmbeddingStore<TextSegment>}.
     */
    private <T> T resolve(PipelineSpec spec, Instance<T> candidates, String configured, String what,
            String property, String setter) {
        String name = spec.name();
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
            // a Java-declared pipeline names its beans in the @Ingest method: the build rejects
            // build-time configuration keys for its name
            throw new IllegalStateException("Ingestion pipeline '" + name + "' found " + handles.size() + " "
                    + what + " beans. Name the one to use with " + (spec.declaredInJava()
                            ? "IngestPipeline." + setter + "(\"<bean name>\") in its @Ingest method"
                            : "quarkus.camel.langchain4j.ingest." + name + "." + property));
        }
        return handles.get(0).get();
    }
}
