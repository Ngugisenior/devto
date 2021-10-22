'use strict';
const { PerformanceObserver, performance } = require('perf_hooks');
var util = require('util');
var hana = require('@sap/hana-client');

var connOptions = {
    //Option 1, retrieve the connection parameters from the hdbuserstore
    serverNode: '@pyraxuserkey',  //host,port, uid, and pwd retrieved from hdbuserstore

    //Option 2, specify the connection parameters
    //serverNode: 'your host:your port',
    //UID: 'USER1',
    //PWD: 'Password1',

    //Additional parameters
    //As of 2.7 trace info can be directed to stdout or stderr
    //traceFile: 'stdout',
    //traceOptions: 'sql=warning',

    //As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud).
    //encrypt: 'true',  //Must be set to true when connecting to HANA as a Service
    sslValidateCertificate: 'false',  //Must be set to false when connecting to an SAP HANA, express edition instance that uses a self-signed certificate.
    //Used to specify where the trust store is located.

    //For encrypted connections, the default crypto provider is mscrypto on Windows or openSSL on Linux or macos
    //To use the SAP crypto provider, uncomment the below line.
    //sslCryptoProvider: 'commoncrypto',

    //As of SAP HANA Client 2.6 for OpenSSL connections, the following settings can be ignored as root certificates are read from the default OS location.
    //ssltruststore: '/home/dan/.ssl/trust.pem',
    //Alternatively provide the contents of the certificate directly (DigiCertGlobalRootCA.pem)
    //DigiCert Global Root CA: https://cacerts.digicert.com/DigiCertGlobalRootCA.crt.pem used for SAP HANA cloud
    //on-premise cert can be retrieved using openssl s_client -connect localhost:39015
    //This option is not supported with the mscrypto provider (the default provider on Windows)
    //ssltruststore: '-----BEGIN CERTIFICATE-----MIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBhMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBDQTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVTMRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5jb20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsBCSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97nh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7PT19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4gdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAOBgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbRTLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUwDQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/EsrhMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJFPnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0lsYSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQkCAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=-----END CERTIFICATE-----'
};

//Synchronous  example querying a table
var connection = hana.createConnection();

//As of 2.9, tracing can be directed to a callback
/*
var traceCB = function (buf) {
    console.log(buf);
};
connection.onTrace("sql=error,debug=fatal,OutBufferSize=64k", traceCB);  
*/

connection.connect(connOptions);

//connection.onTrace("", null);  //disables callback tracing for the rest of the program

var sql = 'select TITLE, FIRSTNAME, NAME from HOTEL.CUSTOMER;';
var t0 = performance.now()
var result = connection.exec(sql);
console.log(util.inspect(result, { colors: false }));
var t1 = performance.now();
console.log("time in ms " +  (t1 - t0));
connection.disconnect();

//Asynchronous example calling a stored procedure
connection = hana.createConnection();
connection.connect(connOptions, function(err) {
    if (err) {
        return console.error(err);
    }
    //Prepared statement example
    const statement = connection.prepare('CALL HOTEL.SHOW_RESERVATIONS(?,?)');
    const parameters = [11, '2020-12-24'];
    var results = statement.execQuery(parameters, function(err, results) {
        if (err) {
            return console.error(err);
        }
        while (results.next()) {
            console.log(util.inspect(results.getValues(), { colors: false }));
        }
        results.close(function(err) {
            if (err) {
                return console.error(err);
            }
            console.log("results.close");
            statement.drop(function(err) {
                if (err) {
                    return console.error(err);
                }
                console.log("statement.drop");
                return connection.disconnect(function(err) {
                    if (err) {
                        return console.error(err);
                    }
                    console.log("disconnect");
                });
            });
        });
    });
});
