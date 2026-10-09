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
package org.apache.camel.quarkus.support.pdfbox.deployment;

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceDirectoryBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedPackageBuildItem;

/**
 * Native image configuration for Apache PDFBox, shared by the extensions whose applications use it.
 */
class PdfBoxSupportProcessor {

    private static final String[] RUNTIME_INITIALIZED_CLASSES = new String[] {
            "org.apache.pdfbox.pdmodel.font.PDType1Font",
            "org.apache.pdfbox.pdmodel.PDDocument",
            "org.apache.pdfbox.pdmodel.encryption.StandardSecurityHandler"
    };

    // AWT-holding statics, e.g. SoftMask's DirectColorModel or the ICC_ColorSpace of the CIE color spaces, which text
    // extraction (e.g. by Tika) reaches
    private static final String[] RUNTIME_INITIALIZED_PACKAGES = new String[] {
            "org.apache.pdfbox.rendering",
            "org.apache.pdfbox.pdmodel.graphics"
    };

    /**
     * Font metrics, glyph lists, the ICC profile etc. The whole directory is registered, so that resources added or
     * renamed by a PDFBox upgrade are embedded too.
     */
    @BuildStep
    NativeImageResourceDirectoryBuildItem initResources() {
        return new NativeImageResourceDirectoryBuildItem("org/apache/pdfbox/resources");
    }

    @BuildStep
    void configureRuntimeInitializedClasses(BuildProducer<RuntimeInitializedClassBuildItem> runtimeInitializedClass,
            BuildProducer<RuntimeInitializedPackageBuildItem> runtimeInitializedPackage) {
        for (String className : RUNTIME_INITIALIZED_CLASSES) {
            runtimeInitializedClass.produce(new RuntimeInitializedClassBuildItem(className));
        }
        for (String packageName : RUNTIME_INITIALIZED_PACKAGES) {
            runtimeInitializedPackage.produce(new RuntimeInitializedPackageBuildItem(packageName));
        }
    }

    @BuildStep
    void registerForReflection(BuildProducer<ReflectiveClassBuildItem> reflectionClass) {
        reflectionClass.produce(ReflectiveClassBuildItem.builder(
                "org.apache.pdfbox.pdmodel.encryption.StandardSecurityHandler",
                "org.apache.pdfbox.pdmodel.documentinterchange.logicalstructure.PDParentTreeValue")
                .constructors().methods().build());
    }
}
