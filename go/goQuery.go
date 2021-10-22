package main

import (
  "fmt"
	"database/sql"
	"log"

  _ "SAP/go-hdb/driver"
  )

func main() {
  //Option 1, retrieve the connection parameters from the hdbuserstore
  //host, port, user name and password come from the hdbuserstore key USER1UserKey
  connectString := "hdb://?key=pyraxuserkey&encrypt=true&sslValidateCertificate=false"

  //Option 2, specify the connection parameters
  //connectString := "hdb://User1:Password1@999deec0-ccb7-4a5e-b317-d419e19be648.hana.prod-us10.hanacloud.ondemand.com:443?encrypt=true&sslValidateCertificate=false"

  //encrypt and sslValidateCertificate should be true for HANA Cloud connections
  //As of SAP HANA Client 2.6, connections on port 443 enable encryption by default

  fmt.Println("Connect String is " + connectString)

  db, err := sql.Open("hdb", connectString)
  if err != nil {
    log.Fatal(err)
    return
  }
  defer db.Close()

  rows, err := db.Query("SELECT NAME, ADDRESS from HOTEL.CUSTOMER")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()

  var lastName string
  var address string
  for rows.Next() {
    err = rows.Scan(&lastName, &address)
    if err != nil {
      log.Fatal(err)
    }
    fmt.Println(lastName + ": " + address)
  }

  err = rows.Err()
  if err != nil {
  	log.Fatal(err)
  }
}
