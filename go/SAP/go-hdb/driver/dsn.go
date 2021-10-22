package driver

import (
	"net/url"
	"strings"
)

//hdb://myuser:mypassword@localhost:30015?DISTRIBUTION=all&RECONNECT=false

type DsnInfo struct {
	Host, Username, Password string
	ConnectProps             url.Values
}

// Golang specific connect properties
var (
	convertTimestampToUTC bool
)

// ^^

func parseDSN(dsn string) (*DsnInfo, error) {
	convertTimestampToUTC = true

	url, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	userName := ""
	password := ""
	if url.User != nil {
		userName = url.User.Username()
		password, _ = url.User.Password()
	}

	processGolangConnectProperties(url)

	return &DsnInfo{Host: url.Host, Username: userName, Password: password,
		ConnectProps: url.Query()}, nil
}

func processGolangConnectProperties(theURL *url.URL) {
	setDefaultGolangPropertyValues()
	query := theURL.Query()
	golangConnectProperties := make([]string, 0)

	for queryKey, queryValue := range query {
		golangConnectProperty := queryKey
		var found bool
		if stringEqualsIgnoreCase(queryKey, "utcTimeConversion") {
			found = true
			processUTCTimestampConversion(queryValue[0])
		}
		if found {
			golangConnectProperties = append(golangConnectProperties, golangConnectProperty)
		}
	}
	for _, golangConnectProperty := range golangConnectProperties {
		query.Del(golangConnectProperty)
	}
	theURL.RawQuery = query.Encode()
}

func setDefaultGolangPropertyValues() {
	convertTimestampToUTC = true
}

func processUTCTimestampConversion(queryValue string) {
	if stringEqualsIgnoreCase(queryValue, "no") ||
		stringEqualsIgnoreCase(queryValue, "false") || stringEqualsIgnoreCase(queryValue, "0") {
		convertTimestampToUTC = false
	}
}

func stringEqualsIgnoreCase(string1, string2 string) bool {
	return strings.EqualFold(string1, string2)
}
