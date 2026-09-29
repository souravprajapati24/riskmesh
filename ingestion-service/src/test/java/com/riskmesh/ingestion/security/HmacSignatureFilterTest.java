package com.riskmesh.ingestion.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class HmacSignatureFilterTest {

    private static final String SECRET = "test-shared-secret";

    private final MerchantSecretResolver secretResolver = merchantId -> SECRET.getBytes(StandardCharsets.UTF_8);
    private final HmacSignatureFilter filter = new HmacSignatureFilter(secretResolver, new ObjectMapper());

    @Test
    void allowsRequestWithValidSignature() throws Exception {
        String body = "{\"externalTxnId\":\"txn-1\"}";
        MockHttpServletRequest request = postRequest(body);
        request.addHeader("X-Gateway-Signature", "sha256=" + sign(body));
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = Mockito.mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        Mockito.verify(chain).doFilter(Mockito.any(CachedBodyHttpServletRequest.class), Mockito.eq(response));
    }

    @Test
    void rejectsRequestWithInvalidSignature() throws Exception {
        String body = "{\"externalTxnId\":\"txn-1\"}";
        MockHttpServletRequest request = postRequest(body);
        request.addHeader("X-Gateway-Signature", "sha256=0000000000000000000000000000000000000000000000000000000000000000");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = Mockito.mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        Mockito.verifyNoInteractions(chain);
        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void rejectsRequestWithMissingSignatureHeader() throws Exception {
        String body = "{\"externalTxnId\":\"txn-1\"}";
        MockHttpServletRequest request = postRequest(body);
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = Mockito.mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        Mockito.verifyNoInteractions(chain);
        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void skipsFilteringForUnrelatedPaths() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/internal/merchants/abc");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = Mockito.mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        Mockito.verify(chain).doFilter(request, response);
    }

    private MockHttpServletRequest postRequest(String body) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/transactions");
        request.setContent(body.getBytes(StandardCharsets.UTF_8));
        return request;
    }

    private String sign(String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }
}
