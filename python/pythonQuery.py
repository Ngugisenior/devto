#Import your dependencies
import platform
from hdbcli import dbapi

#verify the architecture of Python
print ("Platform architecture: " + platform.architecture()[0])

#Initialize your connection
conn = dbapi.connect(
    #Option 1, retrieve the connection parameters from the hdbuserstore
    key='pyraxuserkey', # address, port, user and password are retrieved from the hdbuserstore

    #Option2, specify the connection parameters
    #address='10.7.168.11',
    #port='39015',
    #user='User1',
    #password='Password1',

    #Additional parameters
    #encrypt=True, # must be set to True when connecting to HANA as a Service
    #As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
    #sslValidateCertificate=False #Must be set to false when connecting
    #to an SAP HANA, express edition instance that uses a self-signed certificate.
)
#If no errors, print connected
print('connected')

cursor = conn.cursor()
sql_command = "select TITLE, FIRSTNAME, NAME from HOTEL.CUSTOMER;"
cursor.execute(sql_command)
rows = cursor.fetchall()
for row in rows:
    for col in row:
        print ("%s" % col, end=" ")
    print ("  ")
cursor.close()
print("\n")

#Prepared statement example
sql_command2 = "call HOTEL.SHOW_RESERVATIONS(?,?);"
parameters = [11, "2020-12-24"]
cursor.execute(sql_command2, parameters)
rows = cursor.fetchall()
for row in rows:
    for col in row:
        print ("%s" % col, end=" ")
    print (" ")
cursor.close()
conn.close()
