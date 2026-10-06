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
package org.apache.camel.quarkus.component.langchain4j.ingest.deployment;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.ArcConfig;
import io.quarkus.arc.deployment.SyntheticBeansRuntimeInitBuildItem;
import io.quarkus.arc.deployment.ValidationPhaseBuildItem.ValidationErrorBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.bootstrap.classloading.QuarkusClassLoader;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationArchivesBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ExcludeConfigBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceDirectoryBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedPackageBuildItem;
import io.quarkus.runtime.configuration.ConfigurationException;
import org.apache.camel.quarkus.component.langchain4j.ingest.Ingest;
import org.apache.camel.quarkus.component.langchain4j.ingest.IngestBuildTimeConfig;
import org.apache.camel.quarkus.component.langchain4j.ingest.IngestPipeline;
import org.apache.camel.quarkus.component.langchain4j.ingest.IngestRoutes;
import org.apache.camel.quarkus.component.langchain4j.ingest.Langchain4jIngestRecorder;
import org.apache.camel.quarkus.core.deployment.spi.CamelContextBuildItem;
import org.apache.camel.quarkus.core.deployment.spi.CamelRuntimeTaskBuildItem;
import org.apache.camel.quarkus.core.deployment.spi.CamelServiceBuildItem;
import org.apache.camel.quarkus.core.deployment.util.CamelSupport;
import org.apache.camel.quarkus.core.deployment.util.PathFilter;
import org.apache.camel.util.URISupport;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;

class Langchain4jIngestProcessor {

    private static final String FEATURE = "camel-langchain4j-ingest";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * {@link Ingest} is listed so that it is indexed: the runtime jar carries no Jandex index, and
     * only an indexed qualifier lets ArC's auto producer methods turn {@code @Ingest} methods into
     * producers.
     */
    @BuildStep
    AdditionalBeanBuildItem beans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(IngestRoutes.class, Ingest.class)
                .setUnremovable()
                .build();
    }

    /**
     * The documented PDF recipe adds {@code tika-parser-pdf-module}, which drags
     * {@code jaxb-runtime} → {@code angus-activation} into the image. Angus' own GraalVM
     * feature then reflects over every registered data-content handler and crashes on
     * BouncyCastle's S/MIME handlers ({@code bcjmail}'s mailcap references
     * {@code jakarta.mail.Part}, and jakarta.mail is not on the classpath). The feature only
     * registers mailcap handlers, which nothing on the ingest path uses, so its registration
     * is excluded — but only in the constellation that crashes: PDFBox present (the recipe) and
     * jakarta.mail absent. An application that uses mail and angus for real keeps its feature.
     *
     * TODO: move to camel-quarkus-tika, see #9087.
     */
    @BuildStep
    void excludeAngusActivationFeature(BuildProducer<ExcludeConfigBuildItem> excludeConfig) {
        if (!QuarkusClassLoader.isClassPresentAtRuntime("org.apache.pdfbox.pdmodel.PDDocument")
                || QuarkusClassLoader.isClassPresentAtRuntime("jakarta.mail.Part")) {
            return;
        }
        excludeConfig.produce(new ExcludeConfigBuildItem("org\\.eclipse\\.angus\\.angus-activation-.*\\.jar",
                "/META-INF/native-image/org.eclipse.angus/angus-activation/native-image.properties"));
    }

    /**
     * The same recipe brings PDFBox itself. Native image needs its AWT-touching classes
     * initialized at run time and its resource tree — font metrics, glyph lists, ICC profile —
     * embedded. The camel-quarkus-pdf extension embeds the same files as a hand-listed constant;
     * this registers the {@code org/apache/pdfbox/resources} directory wholesale instead, so the
     * set cannot drift when a PDFBox upgrade adds or renames a resource. The runtime-init side is
     * widened past camel-pdf to the rendering and graphics packages Tika's text extraction
     * reaches. Produced only when PDFBox is on the classpath.
     *
     * TODO: move to camel-quarkus-tika (#9087) and align camel-quarkus-pdf with it (#9091).
     */
    @BuildStep
    void pdfboxInNative(
            BuildProducer<RuntimeInitializedClassBuildItem> runtimeInitialized,
            BuildProducer<RuntimeInitializedPackageBuildItem> runtimeInitializedPackages,
            BuildProducer<NativeImageResourceDirectoryBuildItem> nativeResourceDirectories,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        if (!QuarkusClassLoader.isClassPresentAtRuntime("org.apache.pdfbox.pdmodel.PDDocument")) {
            return;
        }
        for (String className : new String[] {
                "org.apache.pdfbox.pdmodel.font.PDType1Font",
                "org.apache.pdfbox.pdmodel.PDDocument",
                "org.apache.pdfbox.pdmodel.encryption.StandardSecurityHandler" }) {
            runtimeInitialized.produce(new RuntimeInitializedClassBuildItem(className));
        }
        // text extraction reaches AWT-holding statics across the rendering and graphics trees
        // (SoftMask's DirectColorModel, the CIE color spaces' ICC_ColorSpace), and Tika's own
        // PDF classes embed them in enum constants (PDFParserConfig$TikaImageType wraps
        // rendering.ImageType), so the packages are deferred wholesale rather than chasing one
        // class at a time
        for (String packageName : new String[] {
                "org.apache.pdfbox.rendering",
                "org.apache.pdfbox.pdmodel.graphics",
                "org.apache.tika.parser.pdf" }) {
            runtimeInitializedPackages.produce(new RuntimeInitializedPackageBuildItem(packageName));
        }
        reflectiveClasses.produce(ReflectiveClassBuildItem
                .builder("org.apache.pdfbox.pdmodel.encryption.StandardSecurityHandler",
                        "org.apache.pdfbox.pdmodel.documentinterchange.logicalstructure.PDParentTreeValue")
                .constructors().methods().build());
        nativeResourceDirectories.produce(new NativeImageResourceDirectoryBuildItem("org/apache/pdfbox/resources"));
    }

    /**
     * {@code #class:} beans are instantiated reflectively, which native mode allows only for
     * registered classes: the two camel-core repositories the documentation recommends, plus
     * every {@code IdempotentRepository} implementation the Jandex index knows — application
     * classes always, third-party ones when their jar carries an index.
     */
    @BuildStep
    void repositoryReflection(CombinedIndexBuildItem combinedIndex,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        Set<String> repositories = new HashSet<>(Set.of(
                "org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository",
                "org.apache.camel.support.processor.idempotent.FileIdempotentRepository"));
        combinedIndex.getIndex()
                .getAllKnownImplementations(DotName.createSimple("org.apache.camel.spi.IdempotentRepository"))
                .stream()
                .filter(repository -> !Modifier.isAbstract(repository.flags()))
                .map(repository -> repository.name().toString())
                .forEach(repositories::add);
        reflectiveClasses.produce(ReflectiveClassBuildItem.builder(repositories.toArray(new String[0]))
                .methods()
                .build());
    }

    /**
     * {@code @Ingest} methods are CDI producer methods, so ArC validates their shape and IngestRoutes
     * resolves them; what stays here is what CDI cannot know: a pipeline name is non-blank and unique,
     * also against configuration-declared pipelines. Violations are reported as
     * {@link ValidationErrorBuildItem}s — the channel every build-time check of this extension uses,
     * so dev and test mode see them too, and all of them at once.
     */
    @BuildStep
    void validateIngestMethods(CombinedIndexBuildItem combinedIndex, IngestBuildTimeConfig config, ArcConfig arcConfig,
            BuildProducer<ValidationErrorBuildItem> validationErrors) {
        Set<String> names = new HashSet<>();
        for (AnnotationInstance annotation : combinedIndex.getIndex()
                .getAnnotations(DotName.createSimple(Ingest.class.getName()))) {
            MethodInfo method = annotation.target().asMethod();
            String name = annotation.value().asString();
            String location = method.declaringClass().name() + "#" + method.name();

            if (name.isBlank()) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "@Ingest on " + location + " has a blank pipeline name")));
            } else if (!names.add(name) || config.pipelines().containsKey(name)) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + name + "' is declared more than once "
                                + "(builder and/or configuration). Pipeline names must be unique.")));
            }
            // without auto producer methods, an @Ingest method lacking @Produces would declare nothing
            if (!arcConfig.autoProducerMethods() && !method.hasDeclaredAnnotation(DotNames.PRODUCES)) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "@Ingest method " + location + " is not a producer method: annotate it with @Produces, or"
                                + " leave quarkus.arc.auto-producer-methods enabled")));
            }
        }
    }

    /**
     * The pre-start half of the component-presence check: recorded as a Camel runtime task, it
     * runs after ArC is fully initialised but before the Camel runtime is assembled — and thus
     * before Camel Main binds {@code camel.component.*} properties, whose failure for a missing
     * component would otherwise preempt the friendlier add-extension hint.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    @Consume(SyntheticBeansRuntimeInitBuildItem.class)
    CamelRuntimeTaskBuildItem checkComponentsPresent(Langchain4jIngestRecorder recorder,
            CamelContextBuildItem camelContext) {
        recorder.checkComponentsPresent(camelContext.getCamelContext());
        return new CamelRuntimeTaskBuildItem("langchain4j-ingest-components");
    }

    /**
     * A pipeline whose consumer URI names a component that is not on the classpath stops the
     * build, naming the artifact that fixes it rather than failing at startup. The same goes for
     * the {@code parser} value and its component. Only configured pipelines can be checked: a
     * builder-declared pipeline composes its URI and parser at startup, where the pre-start
     * task above applies the same hint. Component services are REGISTRY-destination, so they are
     * read from the application archives directly — they never appear among the DISCOVERY
     * {@code CamelServiceBuildItem}s.
     */
    @BuildStep
    void validateConnectorsPresent(IngestBuildTimeConfig config, ApplicationArchivesBuildItem applicationArchives,
            BuildProducer<ValidationErrorBuildItem> validationErrors) {
        PathFilter pathFilter = new PathFilter.Builder()
                .include("META-INF/services/org/apache/camel/component/*")
                .build();
        Set<String> components = CamelSupport.services(applicationArchives, pathFilter)
                .map(CamelServiceBuildItem::getName)
                .collect(Collectors.toSet());

        for (Map.Entry<String, IngestBuildTimeConfig.PipelineBuildTimeConfig> entry : config.pipelines().entrySet()) {
            String parser = entry.getValue().parser().orElse(null);
            if (parser != null) {
                if (!IngestPipeline.SUPPORTED_PARSERS.contains(parser)) {
                    validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                            "Ingestion pipeline '" + entry.getKey() + "' sets parser '" + parser
                                    + "'. Supported parsers: "
                                    + IngestPipeline.SUPPORTED_PARSERS.stream().sorted()
                                            .collect(Collectors.joining(", ")))));
                } else if (!components.contains(parser)) {
                    validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                            "Ingestion pipeline '" + entry.getKey() + "' parses with '" + parser
                                    + "', but the Camel component '" + parser + "' is not on the classpath. "
                                    + "Add the extension that provides it, e.g. org.apache.camel.quarkus:camel-quarkus-"
                                    + parser)));
                }
            }
            String uri = entry.getValue().source().uri().orElse(null);
            if (uri == null) {
                continue;
            }
            int colon = uri.indexOf(':');
            String scheme = colon < 1 ? uri : uri.substring(0, colon);
            // a placeholder resolves at startup, so its scheme cannot be known here
            if (scheme.contains("{{") || scheme.contains("$")) {
                continue;
            }
            if (!components.contains(scheme)) {
                // the URI is sanitized: a consumer URI may legitimately carry credentials, and a
                // build log is no place for them. The artifact hint is a heuristic - multi-scheme
                // components (smtp -> camel-quarkus-mail) name their extension differently
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + entry.getKey() + "' consumes from '"
                                + URISupport.sanitizeUri(uri) + "', but the Camel component '" + scheme
                                + "' is not on the classpath. Add the extension that provides it, e.g. "
                                + "org.apache.camel.quarkus:camel-quarkus-" + scheme)));
            }
        }
    }

    /**
     * What can be decided from build-time configuration fails here, at build time, with the fix in
     * the message — never silently at runtime.
     */
    @BuildStep
    void validatePipelines(IngestBuildTimeConfig config, BuildProducer<ValidationErrorBuildItem> validationErrors) {
        for (Map.Entry<String, IngestBuildTimeConfig.PipelineBuildTimeConfig> entry : config.pipelines().entrySet()) {
            IngestBuildTimeConfig.PipelineBuildTimeConfig pipeline = entry.getValue();

            if (pipeline.maxSegmentSize() <= 0 || pipeline.maxOverlapSize() < 0
                    || pipeline.maxOverlapSize() >= pipeline.maxSegmentSize()) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + entry.getKey() + "': max-segment-size must be positive and "
                                + "max-overlap-size must be smaller than it (got " + pipeline.maxSegmentSize()
                                + " / " + pipeline.maxOverlapSize() + ")")));
            }

            if (pipeline.embeddingBatchSize() < 1) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + entry.getKey() + "': embedding-batch-size must be positive (got "
                                + pipeline.embeddingBatchSize() + ")")));
            }

            if (pipeline.maxDocumentSize() < 0) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + entry.getKey() + "': max-document-size must not be negative, 0 "
                                + "meaning no limit (got " + pipeline.maxDocumentSize() + ")")));
            }

            String modality = pipeline.modality();
            if (!IngestPipeline.SUPPORTED_MODALITIES.contains(modality)) {
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + entry.getKey() + "' sets modality '" + modality
                                + "'. Supported modalities: "
                                + IngestPipeline.SUPPORTED_MODALITIES.stream().sorted()
                                        .collect(Collectors.joining(", ")))));
            } else if ("media".equals(modality)) {
                // a media document is embedded whole: nothing to parse, nothing to split
                if (pipeline.parser().isPresent()) {
                    validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                            "Ingestion pipeline '" + entry.getKey() + "' sets modality 'media' together with parser '"
                                    + pipeline.parser().get() + "'. A media document is embedded whole and never"
                                    + " parsed; remove one of them.")));
                }
                if (pipeline.documentSplitter().isPresent()) {
                    validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                            "Ingestion pipeline '" + entry.getKey()
                                    + "' sets modality 'media' together with document-splitter '"
                                    + pipeline.documentSplitter().get() + "'. A media document is embedded whole"
                                    + " and never split; remove one of them.")));
                }
            } else if (pipeline.contentType().isPresent()) {
                // content-type types a media payload; with text it is a sign that modality=media was forgotten
                validationErrors.produce(new ValidationErrorBuildItem(new ConfigurationException(
                        "Ingestion pipeline '" + entry.getKey() + "' sets content-type '" + pipeline.contentType().get()
                                + "' without modality 'media'. content-type types a media payload; set modality=media"
                                + " or remove it.")));
            }
        }
    }
}
