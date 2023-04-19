package ai.platon.pulsar.standalone.starter;

import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import javax.net.ssl.*

object HttpsUrlValidator {
    var hv = HostnameVerifier { urlHostName, session ->
        println(
            "Warning: URL Host: " + urlHostName + " vs. "
                    + session.peerHost
        )
        true
    }

    @Throws(Exception::class)
    fun disableSSLVerified() {
        trustAllHttpsCertificates()
        HttpsURLConnection.setDefaultHostnameVerifier(hv)
    }

    fun retrieveResponseFromServer(url: String?): String? {
        val connection: HttpURLConnection? = null
        return try {
            val validationUrl = URL(url)
            trustAllHttpsCertificates()
            HttpsURLConnection.setDefaultHostnameVerifier(hv)

            //            connection = (HttpURLConnection) validationUrl.openConnection();
            //            final BufferedReader in = new BufferedReader(new InputStreamReader(
            //                    connection.getInputStream()));
            //
            //            String line;
            //            final StringBuffer stringBuffer = new StringBuffer(255);
            //
            //            synchronized (stringBuffer) {
            //                while ((line = in.readLine()) != null) {
            //                    stringBuffer.append(line);
            //                    stringBuffer.append("\n");
            //                }
            //                return stringBuffer.toString();
            //            }
            null
        } catch (e: IOException) {
            println(e.message)
            null
        } catch (e1: Exception) {
            println(e1.message)
            null
        } finally {
            //            if (connection != null) {
            //                connection.disconnect();
            //            }
        }
    }

    @Throws(Exception::class)
    fun trustAllHttpsCertificates() {
        val trustAllCerts = arrayOfNulls<TrustManager>(1)
        val tm: TrustManager = miTM()
        trustAllCerts[0] = tm
        val sc = SSLContext
            .getInstance("SSL")
        sc.init(null, trustAllCerts, null)
        HttpsURLConnection.setDefaultSSLSocketFactory(
            sc.socketFactory
        )
    }

    internal class miTM : TrustManager, X509TrustManager {
        override fun getAcceptedIssuers(): Array<X509Certificate> {
            return emptyArray()
        }

        fun isServerTrusted(
            certs: Array<X509Certificate?>?
        ): Boolean {
            return true
        }

        fun isClientTrusted(
            certs: Array<X509Certificate?>?
        ): Boolean {
            return true
        }

        @Throws(CertificateException::class)
        override fun checkServerTrusted(
            certs: Array<X509Certificate>, authType: String
        ) {
            return
        }

        @Throws(CertificateException::class)
        override fun checkClientTrusted(
            certs: Array<X509Certificate>, authType: String
        ) {
            return
        }
    }
}

