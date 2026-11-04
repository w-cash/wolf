PUBLIC, DISPOSABLE TLS TEST MATERIAL ONLY.

The private server key is intentionally checked in and must never be used outside
these offline loopback tests. The CA is trusted only by each test client; it is
never installed in an OS trust store or a production client. The CA signing key
is not retained. The CA has critical CA:TRUE/keyCertSign/cRLSign constraints.
The leaf has critical CA:FALSE/digitalSignature/keyEncipherment constraints,
serverAuth extended key usage and only IP SAN 127.0.0.1.

Created with OpenSSL/LibreSSL RSA-2048 and SHA-256. The CA has 7300 days of
validity, and the signed leaf has 7000 days, serial 2. A private temporary
directory held the disposable PEM keys and CSR during generation. OpenSSL
verified the leaf for sslserver against only the public fixture CA.

notBefore=Nov  4 20:55:22 2026 GMT
notAfter=Jan  3 20:55:22 2046 GMT
SHA256 Fingerprint=99:F3:EC:0C:11:50:A5:9D:13:CB:48:28:D1:B5:67:92:42:18:C1:0F:26:58:D3:3C:26:8B:69:49:E1:47:23:6F
