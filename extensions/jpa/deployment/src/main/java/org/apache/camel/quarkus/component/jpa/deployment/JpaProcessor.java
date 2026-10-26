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
package org.apache.camel.quarkus.component.jpa.deployment;

import java.util.List;
import java.util.Set;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.BytecodeTransformerBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.RemovedResourceBuildItem;
import io.quarkus.gizmo.Gizmo;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.maven.dependency.ArtifactKey;
import jakarta.persistence.EntityManagerFactory;
import org.apache.camel.component.jpa.JpaComponent;
import org.apache.camel.component.jpa.JpaEndpoint;
import org.apache.camel.component.jpa.JpaHelper;
import org.apache.camel.quarkus.component.jpa.CamelJpaProducer;
import org.apache.camel.quarkus.component.jpa.CamelJpaRecorder;
import org.apache.camel.quarkus.component.jpa.QuarkusTransactionStrategy;
import org.apache.camel.quarkus.core.deployment.spi.CamelBeanQualifierResolverBuildItem;
import org.apache.camel.quarkus.core.deployment.spi.CamelRuntimeBeanBuildItem;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

class JpaProcessor {

    private static final String FEATURE = "camel-jpa";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @Record(ExecutionTime.RUNTIME_INIT)
    @BuildStep
    void configureJpaComponentBean(
            BuildProducer<AdditionalBeanBuildItem> additionalBeans,
            BuildProducer<CamelRuntimeBeanBuildItem> camelRuntimeBean,
            CamelJpaRecorder recorder) {
        additionalBeans.produce(new AdditionalBeanBuildItem(CamelJpaProducer.class));

        camelRuntimeBean.produce(
                new CamelRuntimeBeanBuildItem(
                        "jpa",
                        JpaComponent.class.getName(),
                        recorder.createJpaComponent()));
    }

    @Record(ExecutionTime.STATIC_INIT)
    @BuildStep
    void registerPersistenceUnitCamelBeanQualifierResolver(
            List<PersistenceUnitDescriptorBuildItem> persistenceUnitDescriptors,
            BuildProducer<CamelBeanQualifierResolverBuildItem> camelBeanQualifierResolver,
            CamelJpaRecorder recorder) {
        // If there are multiple persistence unit configs, then users need to explicitly state which one to use
        // via their component / endpoint configuration. Otherwise if there is just 1, and it is not the default,
        // we can create a resolver for PersistenceUnitLiteral and make named PersistenceUnit autowiring work as expected
        if (persistenceUnitDescriptors.size() == 1) {
            PersistenceUnitDescriptorBuildItem persistenceUnitDescriptor = persistenceUnitDescriptors.get(0);
            if (!persistenceUnitDescriptor.getPersistenceUnitName().equals("<default>")) {
                CamelBeanQualifierResolverBuildItem beanQualifierResolver = new CamelBeanQualifierResolverBuildItem(
                        EntityManagerFactory.class,
                        recorder.createPersistenceUnitQualifierResolver(persistenceUnitDescriptor.getPersistenceUnitName()));
                camelBeanQualifierResolver.produce(beanQualifierResolver);
            }
        }
    }

    @BuildStep
    void removedResources(BuildProducer<RemovedResourceBuildItem> removedResources) {

        removedResources.produce(new RemovedResourceBuildItem(
                ArtifactKey.fromString("org.apache.camel:camel-jpa"),
                Set.of(
                        "org/apache/camel/component/jpa/DefaultTransactionStrategy.class")));
    }

    // TODO: Remove this and make it possible to override methods in JpaEndpoint
    // https://github.com/apache/camel-quarkus/issues/7369
    @BuildStep
    BytecodeTransformerBuildItem transformJpaEndpoint() {
        // Since spring-orm is not on the classpath, transform JpaEndpoint.createEntityManagerFactory
        // to avoid trying to create a local LocalEntityManagerFactoryBean and
        // suppress ClassNotFoundException when the component could not find a EntityManagerFactory to use.
        //
        // For example:
        //
        // protected EntityManagerFactory createEntityManagerFactory() {
        //     throw new IllegalStateException();
        // }
        return new BytecodeTransformerBuildItem.Builder()
                .setClassToTransform(JpaEndpoint.class.getName())
                .setCacheable(true)
                .setVisitorFunction((className, classVisitor) -> {
                    return new ClassVisitor(Gizmo.ASM_API_VERSION, classVisitor) {
                        @Override
                        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
                                String[] exceptions) {
                            MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);

                            if (name.equals("createEntityManagerFactory")
                                    && descriptor.equals("()Ljakarta/persistence/EntityManagerFactory;")) {
                                return new MethodVisitor(Gizmo.ASM_API_VERSION, visitor) {
                                    @Override
                                    public void visitCode() {
                                        super.visitCode();
                                        visitor.visitTypeInsn(Opcodes.NEW, "java/lang/IllegalStateException");
                                        visitor.visitInsn(Opcodes.DUP);
                                        visitor.visitLdcInsn(
                                                "Cannot create EntityManagerFactory. Check quarkus.hibernate-orm configuration.");
                                        visitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/IllegalStateException",
                                                "<init>", "(Ljava/lang/String;)V", false);
                                        visitor.visitInsn(Opcodes.ATHROW);
                                    }
                                };
                            }
                            return visitor;
                        }
                    };
                })
                .build();
    }

    @BuildStep
    BytecodeTransformerBuildItem transformJpaHelper() {
        // DefaultTransactionStrategy is removed. JpaHelper still creates it for the repositories, which have
        // no CamelContext to find the TransactionStrategy bean, so create a QuarkusTransactionStrategy instead:
        //
        // private static TransactionStrategy createDefaultTransactionStrategy(CamelContext camelContext,
        //         EntityManagerFactory entityManagerFactory) {
        //     return new QuarkusTransactionStrategy();
        // }
        String quarkusTransactionStrategy = Type.getInternalName(QuarkusTransactionStrategy.class);
        return new BytecodeTransformerBuildItem.Builder()
                .setClassToTransform(JpaHelper.class.getName())
                .setCacheable(true)
                .setVisitorFunction((className, classVisitor) -> {
                    return new ClassVisitor(Gizmo.ASM_API_VERSION, classVisitor) {
                        @Override
                        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
                                String[] exceptions) {
                            MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
                            if (name.equals("createDefaultTransactionStrategy")) {
                                return new MethodVisitor(Gizmo.ASM_API_VERSION, visitor) {
                                    @Override
                                    public void visitCode() {
                                        super.visitCode();
                                        visitor.visitTypeInsn(Opcodes.NEW, quarkusTransactionStrategy);
                                        visitor.visitInsn(Opcodes.DUP);
                                        visitor.visitMethodInsn(Opcodes.INVOKESPECIAL, quarkusTransactionStrategy, "<init>",
                                                "()V", false);
                                        visitor.visitInsn(Opcodes.ARETURN);
                                    }
                                };
                            }
                            return visitor;
                        }
                    };
                })
                .build();
    }
}
