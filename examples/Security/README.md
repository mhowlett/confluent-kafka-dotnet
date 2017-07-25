# Security Configuration Examples

A walkthough of various security configuration scenarios with example code in C#.

This is a work in progress. I will hopefully get a chance to add a Kerberos example soon.

For further information, some good resources are:

- [https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka](https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka)
- [https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka](https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka)
- [https://kafka.apache.org/documentation/#security](https://kafka.apache.org/documentation/#security)
- [https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

## Example 1 - Secure Connection + Server Identity Verification.

- The most simple setup.
- Client verifies identity of server.
- Server does not verify identity of (authenticate) the client.
- Communication is encrypted - safe from man-in-the-middle attacks.

**Procedure:**

1. Create a [private key] / [public key certificate] pair for the broker:

    ```
    keytool -keystore server.keystore.jks -alias localhost -validity 365 -genkey -keyalg RSA
    ```

    This will:

    1. Automatically create a new java keystore `server.keystore.jks` in the current directory if it doesn't exist already.
        1. You will be prompted for a password to protect this keystore if it does not exist already.
        1. You will be prompted to enter the keystore's password if it does exist.
    
    1. Create a new RSA private/public key pair.

    1. Prompt you for additional information that is required to make a *public key certificate*.
        - You should set the CN (the answer to the question 'What is your first and last name?') to be `localhost` (or the FQDN of the computer the broker will be run on).

    1. Make the public key certificate.

    1. Store the private key and self-signed public key certificate under the alias `localhost` in the keystore file.
        - you will be prompted for a password to protect this specific alias.

1. Create a Certificate Authority

    **Important Note:** You should not be tempted to use a simpler configuration procedure that uses self-signed certificates - this is not secure (susceptible to man-in-the-middle attacks).

    ```
    openssl req -nodes -new -x509 -keyout ca-key -out ca-cert -days 365
    ```

    This creates a private key / public key cetificate pair where the private key isn't password protected. You will be prompted for information to put in the certificate - none of this is important (but you might as well enter data relevant to you).


1. Sign the broker public key certificate

    1. Generate a Certificate Signing Request (CSR) from the self signed certificate you created in step 1.

    ```
    keytool -keystore server.keystore.jks -alias localhost -certreq -file localhost.csr
    ```

    2. Use the CA key pair you generated in step 2 to create a CA signed certificate from this CSR.

    ```
    openssl x509 -req -CA ca-cert -CAkey ca-key -in localhost.csr -out localhost.crt -days 365 -CAcreateserial 
    ```

    3. Import the signed certificate into your server keystore (over-writing the self-signed one). Before you can do this, you'll need to add the CA public key certificate as well.

    ```
    keytool -keystore server.keystore.jks -alias CARoot -import -file ca-cert
    keytool -keystore server.keystore.jks -alias localhost -import -file localhost.crt
    ```

1. Configure the broker and client

    You now have everything you need to configure the broker and client.

    Broker config (assuming the files you created above are in `/tmp`):

    ```
    listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093
    ssl.keystore.location=/tmp/server.keystore.jks
    ssl.keystore.type=JKS
    ssl.keystore.password=test1234
    ssl.key.password=test1234
    ```

    librdkafka based client config 

    ```
        { "bootstrap.servers", "localhost:9093" },
        { "security.protocol", "SSL" },
        { "ssl.ca.location", "/tmp/ca-cert" }
        { "debug", "security" }
    ```

## Example 2 - Example 1 plus SSL Client Authentication

- Example 1 functionality plus SSL client authentication (SSL mutual auth)

TODO: this example works, but do some more explanation + variations.

**Procedure:**

1. Create a truststore containing the ca-cert.

    ```
    keytool -keystore server.truststore.jks -alias CARoot -import -file ca-cert
    ```

2. Create a new CSR for the client certificate.

    ```
    openssl req -newkey rsa:2048 -nodes -keyout localhost_client.key -out localhost_client.csr
    ```

    answer the question 'Common Name (e.g. server FQDN or YOUR name) []:' with `localhost`.

    enter a blank password.

    create a CA cert from the CSR:

    ```
    openssl x509 -req -CA ca-cert -CAkey ca-key -in localhost_client.csr -out localhost_client.crt -days 365 -CAcreateserial 
    ```


1. Configure the broker and client.

    ```
    listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093
    ssl.keystore.location=/tmp/server.keystore.jks
    ssl.keystore.type=JKS
    ssl.keystore.password=test1234
    ssl.key.password=test1234
    ssl.truststore.location=/tmp/server.truststore.jks
    ssl.truststore.type=JKS
    ssl.truststore.password=test1234
    ssl.client.auth=required
    ```


## Example 3 - Example 2 plus Kerberos Authentication

- Note the prinicple from the SSL client certificate is overriden when you configure additional authentication.

**Procedure:**

TODO
