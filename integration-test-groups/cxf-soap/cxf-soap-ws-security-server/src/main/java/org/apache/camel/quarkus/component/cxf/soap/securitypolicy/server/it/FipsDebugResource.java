package org.apache.camel.quarkus.component.cxf.soap.securitypolicy.server.it;

import java.security.SecureRandom;
import java.security.Security;
import java.util.Arrays;
import java.util.stream.Collectors;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

// TEMPORARY local debug endpoint, not to be committed
@Path("/fips-debug")
public class FipsDebugResource {
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String get() {
        StringBuilder sb = new StringBuilder();
        sb.append("providers=").append(Arrays.stream(Security.getProviders()).map(p -> p.getName()).collect(Collectors.joining(","))).append('\n');
        sb.append("wss4j.FIPSUtils=").append(org.apache.wss4j.common.util.FIPSUtils.isFIPSEnabled()).append('\n');
        sb.append("cxf.JavaUtils=").append(org.apache.cxf.helpers.JavaUtils.isFIPSEnabled()).append('\n');
        sb.append("sysprop.fips.enabled=").append(System.getProperty("fips.enabled")).append('\n');
        try {
            SecureRandom r = SecureRandom.getInstance("PKCS11");
            sb.append("PKCS11 SecureRandom from ").append(r.getProvider().getName()).append(" ").append(r.nextInt()).append('\n');
        } catch (Exception e) {
            sb.append("PKCS11 SecureRandom FAILED ").append(e).append('\n');
        }
        return sb.toString();
    }
}
