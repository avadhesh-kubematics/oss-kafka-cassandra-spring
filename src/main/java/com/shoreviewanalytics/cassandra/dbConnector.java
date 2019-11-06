package com.shoreviewanalytics.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.shoreviewanalytics.config.AppConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;


import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

@Configuration
public class dbConnector {

    @Autowired
    AppConfig config;


    private static SSLContext loadCaCert() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        InputStream fis = null;
        X509Certificate caCert;
        try {
            fis = dbConnector.class.getResourceAsStream("/myca.pem");
            caCert = (X509Certificate) cf.generateCertificate(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null);
        ks.setCertificateEntry("caCert", caCert);
        tmf.init(ks);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);
        return sslContext;
    }

    private CqlSession session;

    void connect(String node, Integer port, String datacenter, String username, String password) throws Exception {

        CqlSessionBuilder builder = CqlSession.builder();
        builder.withAuthCredentials(username,password);
        builder.withSslContext(loadCaCert());
        builder.addContactPoint(new InetSocketAddress(node, port));
        builder.withLocalDatacenter(datacenter);
        builder.withKeyspace("KAFKA_EXAMPLES");
        session = builder.build();
    }

    CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }

}
