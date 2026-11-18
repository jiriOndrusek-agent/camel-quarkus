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
package org.apache.camel.quarkus.component.hashicorp.vault.deployment;

import java.util.function.BooleanSupplier;

import io.quarkus.bootstrap.classloading.QuarkusClassLoader;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.BytecodeTransformerBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.Gizmo;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.springframework.vault.support.VaultResponseSupport;

class HashicorpVaultProcessor {
    private static final String FEATURE = "camel-hashicorp-vault";
    private static final String AWS_IAM_CLASS = "org.apache.camel.component.hashicorp.vault.HashicorpVaultAwsIam";
    private static final String AWS_SDK_CLASS = "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    IndexDependencyBuildItem indexDependencies() {
        return new IndexDependencyBuildItem("org.springframework.vault", "spring-vault-core");
    }

    @BuildStep
    void registerForReflection(CombinedIndexBuildItem combinedIndex, BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {
        String[] vaultResponseClasses = combinedIndex.getIndex()
                .getAllKnownSubclasses(VaultResponseSupport.class)
                .stream()
                .map(ClassInfo::name)
                .map(DotName::toString)
                .toArray(String[]::new);

        reflectiveClass.produce(ReflectiveClassBuildItem.builder(VaultResponseSupport.class).methods(true).build());
        reflectiveClass.produce(ReflectiveClassBuildItem.builder(vaultResponseClasses).methods(true).build());
    }

    /**
     * The aws-iam login (HashicorpVaultAwsIam) needs the optional AWS SDK. Without it, native-image cannot resolve
     * the class, so its login methods throw instead, and the methods with AWS SDK types in their signature are removed.
     */
    @BuildStep(onlyIf = AwsSdkAbsent.class)
    BytecodeTransformerBuildItem removeAwsIamLogin() {
        return new BytecodeTransformerBuildItem.Builder()
                .setClassToTransform(AWS_IAM_CLASS)
                .setCacheable(true)
                .setVisitorFunction((className, classVisitor) -> new ClassVisitor(Gizmo.ASM_API_VERSION, classVisitor) {
                    @Override
                    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
                            String[] exceptions) {
                        if (name.equals("validate") || name.equals("authentication")) {
                            MethodVisitor mv = super.visitMethod(access, name, descriptor, null, exceptions);
                            mv.visitCode();
                            mv.visitTypeInsn(Opcodes.NEW, "java/lang/IllegalArgumentException");
                            mv.visitInsn(Opcodes.DUP);
                            mv.visitLdcInsn("The aws-iam login needs the AWS SDK: add the software.amazon.awssdk:auth"
                                    + " dependency");
                            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/IllegalArgumentException", "<init>",
                                    "(Ljava/lang/String;)V", false);
                            mv.visitInsn(Opcodes.ATHROW);
                            mv.visitMaxs(3, 3);
                            mv.visitEnd();
                            return null;
                        }
                        if (descriptor.contains("software/amazon/awssdk")) {
                            return null;
                        }
                        return super.visitMethod(access, name, descriptor, signature, exceptions);
                    }
                })
                .build();
    }

    static final class AwsSdkAbsent implements BooleanSupplier {
        @Override
        public boolean getAsBoolean() {
            return !QuarkusClassLoader.isClassPresentAtRuntime(AWS_SDK_CLASS);
        }
    }
}
