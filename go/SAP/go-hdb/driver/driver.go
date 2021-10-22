package driver

/*
#cgo CFLAGS: -I${SRCDIR}/includes
#cgo LDFLAGS: -Wl,-rpath,\$ORIGIN
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include "DBCAPI_DLL.h"
#include "DBCAPI.h"
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

//DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

//For conversion between string and timestamp
const TS_LAYOUT_STRING = "2006-01-02 15:04:05.000000000"

//For conversion between string and timestamp without without seconds
const SECONDDATE_LAYOUT_STRING = "2006-01-02 15:04:05"

//For conversion between string and date
const DATE_LAYOUT_STRING = "2006-01-02"

//For conversion between string and time
const TIME_LAYOUT_STRING = "15:04:05"

//Max length of varbinary column
const MAX_BINARY_SIZE = 5000

const DEFAULT_ISOLATION_LEVEL = 1

const EXPECTED_DBCAPI_VERSION = "libdbcapiHDB 2.10.013.1631558844"

const (
	FunctionCode_DDL    = 1
	FunctionCode_INSERT = 2
	FunctionCode_UPDATE = 3
	FunctionCode_DELETE = 4
)

var init_ok bool

type drv struct{}

//database connection
type connection struct {
	conn *C.dbcapi_connection
}

type transaction struct {
	conn *C.dbcapi_connection
}

//statement
type statement struct {
	query string
	stmt  *C.dbcapi_stmt
	conn  *C.dbcapi_connection
	Ok    bool
}

const (
	// strconv.IntSize gives the size (in bits) of an int/uint - this is also changes with the size
	// of pointers, depending on the build type (32 or 64 bit OS). See the following links for more
	// information:
	// 1) https://golang.org/pkg/strconv/ (strconv package (IntSize constant))
	// 2) https://stackoverflow.com/questions/25741841/how-can-i-check-if-my-program-is-compiled-for-32-or-64-bit-processor
	pointerSize = C.size_t(strconv.IntSize / 8)
	nullSize    = C.size_t(C.sizeof_dbcapi_bool)
	boolSize    = C.size_t(C.sizeof_dbcapi_bool)
)

//result set
type rows struct {
	stmt            *C.dbcapi_stmt
	conn            *C.dbcapi_connection
	isFromStatement bool
	isClosed        bool
	err             error
}

//keep track of rows affected after inserts and updates
type result struct {
	rowsAffected int64
	err          error
}

//needed to handle nil time values
type NullTime struct {
	Time  time.Time
	Valid bool
}

//needed to handle nil binary values
type NullBytes struct {
	Bytes []byte
	Valid bool
}

// Needed to handle bound parameters
type boundParameterValue struct {
	boundStruct *C.struct_dbcapi_bind_data
	batchSize   uint64
}

func (bpv *boundParameterValue) cleanup() {
	if bpv.boundStruct != nil {
		defer C.free(unsafe.Pointer(bpv.boundStruct))

		if (bpv.batchSize > 1) && (bpv.boundStruct.value.is_address == C.dbcapi_bool(1)) {
			bpv.cleanupBatch()
		} else {
			bpv.cleanupNonBatch()
		}
	}
}

func (bpv *boundParameterValue) cleanupNonBatch() {
	if bpv.boundStruct.value.is_null != nil {
		C.free(unsafe.Pointer(bpv.boundStruct.value.is_null))
	}

	if bpv.boundStruct.value.buffer != nil {
		C.free(unsafe.Pointer(bpv.boundStruct.value.buffer))
	}

	if bpv.boundStruct.value.length != nil {
		C.free(unsafe.Pointer(bpv.boundStruct.value.length))
	}
}

func (bpv *boundParameterValue) cleanupBatch() {
	if bpv.boundStruct.value.is_null != nil {
		C.free(unsafe.Pointer(bpv.boundStruct.value.is_null))
	}

	if bpv.boundStruct.value.length != nil {
		C.free(unsafe.Pointer(bpv.boundStruct.value.length))
	}

	hasValues := (bpv.boundStruct.value.buffer != nil)

	if hasValues {
		defer C.free(unsafe.Pointer(bpv.boundStruct.value.buffer))

		for i := uint64(0); i < bpv.batchSize; i++ {
			if hasValues {
				valuePtr := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(bpv.boundStruct.value.buffer)) +
					uintptr(pointerSize*C.size_t(i))))
				C.free(unsafe.Pointer(*valuePtr))
			}
		}
	}
}

// Needed to handle dbcapi_bind_param_info struct deallocations (only done for columns of one row)
type paramInfoValues struct {
	paramInfos []*C.dbcapi_bind_param_info
}

func (bpi *paramInfoValues) cleanup() {
	if bpi.paramInfos != nil {

		// Assumes that all locations have a value assocated
		for i := 0; i < len(bpi.paramInfos); i++ {
			if bpi.paramInfos[i] != nil {
				C.free(unsafe.Pointer(bpi.paramInfos[i]))
			}
		}
	}
}

func init() {
	sql.Register(DriverName, &drv{})
	cName := C.CString("hdbGolangDriver")
	defer C.free(unsafe.Pointer(cName))
	api := C.dbcapi_init(cName, C.DBCAPI_API_VERSION_1, nil)
	if api != 1 {
		init_ok = false
	} else {
		init_ok = true
	}
}

func getError(conn *C.dbcapi_connection) error {
	if conn == nil {
		return driver.ErrBadConn
	}

	errorLen := C.dbcapi_error_length(conn)
	if errorLen == 0 {
		errorLen = 1
	}
	err := make([]byte, errorLen)
	errCode := C.dbcapi_error(conn, (*C.char)(unsafe.Pointer(&err[0])), errorLen)
	//-10806: connection lost and transaction rolled back and -10807: connection lost and -10108: reconnect error and -10821 session not connected
	if errCode == -10806 || errCode == -10807 || errCode == -10108 || errCode == -10821 {
		return driver.ErrBadConn
	}
	return fmt.Errorf("%d: %s", errCode, string(err[:errorLen-1]))
}

func isBadConnection(conn *C.dbcapi_connection) bool {
	if conn == nil {
		return true
	}

	len := C.size_t(C.DBCAPI_ERROR_SIZE)
	err := make([]byte, len)
	errCode := C.dbcapi_error(conn, (*C.char)(unsafe.Pointer(&err[0])), len)
	//-10806: connection lost and transaction rolled back and -10807: connection lost and -10108: reconnect error and -10821 session not connected
	return errCode == -10806 || errCode == -10807 || errCode == -10108 || errCode == -10821
}

func setPropertiesAndConnect(conn *C.dbcapi_connection, dsnInfo *DsnInfo) error {
	err := setDefaultConnectProperties(conn, dsnInfo)
	if err != nil {
		return err
	}

	err = setAdditionalConnectProperties(conn, dsnInfo)
	if err != nil {
		return err
	}

	connRes := C.dbcapi_connect2(conn)

	if connRes != 1 {
		return getError(conn)
	}
	return nil
}

func setDefaultConnectProperties(conn *C.dbcapi_connection, dsnInfo *DsnInfo) error {
	{
		cServernodeKey := C.CString("SERVERNODE")
		defer C.free(unsafe.Pointer(cServernodeKey))
		cServernode := C.CString(dsnInfo.Host)
		defer C.free(unsafe.Pointer(cServernode))
		if C.dbcapi_set_connect_property(conn, cServernodeKey, cServernode) != 1 {
			return getError(conn)
		}
	}
	{
		cUsernameKey := C.CString("UID")
		defer C.free(unsafe.Pointer(cUsernameKey))
		cUsername := C.CString(dsnInfo.Username)
		defer C.free(unsafe.Pointer(cUsername))
		if C.dbcapi_set_connect_property(conn, cUsernameKey, cUsername) != 1 {
			return getError(conn)
		}
	}
	{
		cPasswordKey := C.CString("PWD")
		defer C.free(unsafe.Pointer(cPasswordKey))
		cPassword := C.CString(dsnInfo.Password)
		defer C.free(unsafe.Pointer(cPassword))
		if C.dbcapi_set_connect_property(conn, cPasswordKey, cPassword) != 1 {
			return getError(conn)
		}
	}
	{
		cCharsetKey := C.CString("CHARSET")
		defer C.free(unsafe.Pointer(cCharsetKey))
		cCharset := C.CString("UTF-8")
		defer C.free(unsafe.Pointer(cCharset))
		if C.dbcapi_set_connect_property(conn, cCharsetKey, cCharset) != 1 {
			return getError(conn)
		}
	}
	return nil
}

func setAdditionalConnectProperties(conn *C.dbcapi_connection, dsnInfo *DsnInfo) error {
	connProps := dsnInfo.ConnectProps
	for key := range connProps {
		values := connProps[key]
		cKey := C.CString(key)
		defer C.free(unsafe.Pointer(cKey))
		cValue := C.CString(values[0])
		defer C.free(unsafe.Pointer(cValue))
		if C.dbcapi_set_connect_property(conn, cKey, cValue) != 1 {
			return getError(conn)
		}
	}
	return nil
}

func (d *drv) Open(dsn string) (driver.Conn, error) {
	if !init_ok {
		return nil, fmt.Errorf("Loading the dbcapi library has failed, please ensure that it is on your library path")
	}

	buffer := (*C.char)(C.malloc(256))
	defer C.free(unsafe.Pointer(buffer))
	client := C.dbcapi_client_version(buffer, 256)
	version := C.GoString(buffer)

	if client != 1 {
		return nil, fmt.Errorf("Unable to retrieve the version string from libdbcapiHDB.")
	} else if version != EXPECTED_DBCAPI_VERSION {
		return nil, fmt.Errorf("Invalid libdbcapiHDB version.  Expected '%s', but found '%s'.", EXPECTED_DBCAPI_VERSION, version)
	}

	dsnInfo, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn := C.dbcapi_new_connection()
	if conn == nil {
		return nil, errors.New("dbcapi failed to allocate a connection object")
	}

	err = setPropertiesAndConnect(conn, dsnInfo)

	if err != nil {
		C.dbcapi_free_connection(conn)
		return nil, err
	}

	autoCommRes := C.dbcapi_set_autocommit(conn, 1)

	if autoCommRes != 1 {
		defer C.dbcapi_free_connection(conn)
		return nil, getError(conn)
	}
	return &connection{conn: conn}, nil
}

// CheckNamedValue (from the NamedValueChecker interface) is implemented here only to allow our
// custom bulk insertimplementation for Exec(), as the default sql driver checks all default values
// supported, and this will value for the [][]interface{}type.
// No implementation is needed here, as reflection is used in our parameter processing logic to
// determine what parameter type we are dealing with
func (c *connection) CheckNamedValue(nv *driver.NamedValue) error {
	return nil
}

func (connection *connection) Prepare(query string) (driver.Stmt, error) {
	psql := C.CString(query)
	defer C.free(unsafe.Pointer(psql))
	ps := C.dbcapi_prepare(connection.conn, psql)

	if ps == nil {
		return nil, getError(connection.conn)
	}

	return &statement{query: query, stmt: ps, conn: connection.conn}, nil
}

func (connection *connection) Close() error {
	defer C.dbcapi_free_connection(connection.conn)
	disconn := C.dbcapi_disconnect(connection.conn)
	var err error
	if disconn != 1 {
		err = getError(connection.conn)
	}
	return err
}

func (connection *connection) Begin() (driver.Tx, error) {
	autoCommRes := C.dbcapi_set_autocommit(connection.conn, 0)
	if autoCommRes != 1 {
		return nil, getError(connection.conn)
	}

	return &transaction{conn: connection.conn}, nil
}

func (connection *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, errors.New("Setting of read only option is currently unsupported")
	}

	isolationLevel := int(opts.Isolation)
	//0 means that use the driver's default isolation level
	if isolationLevel == 0 {
		return connection.Begin()
	}
	if isolationLevel < 0 || isolationLevel > 3 {
		return nil, errors.New(fmt.Sprintf("Unsupported isolation level requested: %d ", isolationLevel))
	}
	err := setIsolationLevel(connection.conn, isolationLevel)
	if err != nil {
		return nil, err
	}

	return connection.Begin()
}

func setIsolationLevel(conn *C.dbcapi_connection, isolationLevel int) error {
	isolationLevelRes := C.dbcapi_set_transaction_isolation(conn, C.dbcapi_u32(isolationLevel))
	if isolationLevelRes != 1 {
		return getError(conn)
	}

	return nil
}

func (connection *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	if args != nil && len(args) != 0 {
		return nil, driver.ErrSkip
	}

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))
	stmt := C.dbcapi_execute_direct(connection.conn, sql)

	if stmt == nil {
		return nil, getError(connection.conn)
	}
	defer C.dbcapi_free_stmt(stmt)

	funcCode := C.dbcapi_get_function_code(stmt)
	if funcCode == FunctionCode_DDL {
		return driver.ResultNoRows, nil
	}

	rowsAffected := C.dbcapi_affected_rows(stmt)
	if rowsAffected == -1 {
		return &result{rowsAffected: -1, err: getError(connection.conn)}, nil
	} else {
		return &result{rowsAffected: int64(rowsAffected), err: nil}, nil
	}
}

func (connection *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	if args != nil && len(args) != 0 {
		return nil, driver.ErrSkip
	}

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))
	stmt := C.dbcapi_execute_direct(connection.conn, sql)

	if stmt == nil {
		return nil, getError(connection.conn)
	}

	return &rows{stmt: stmt, conn: connection.conn, isFromStatement: false, isClosed: false, err: nil}, nil
}

func (rows *rows) Close() error {
	if !rows.isClosed {
		if !rows.isFromStatement {
			defer C.dbcapi_free_stmt(rows.stmt)
		}
		rows.isClosed = true
		closeRowsRes := C.dbcapi_reset(rows.stmt)

		if closeRowsRes != 1 {
			rows.err = getError(rows.conn)
		}
	}
	return rows.err
}

func (rows *rows) Columns() []string {
	colCount := int(C.dbcapi_num_cols(rows.stmt))
	if colCount == -1 {
		return nil
	}
	columnNames := make([]string, colCount)
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
	defer C.free(unsafe.Pointer(colInfo))

	for i := 0; i < colCount; i++ {
		colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(i), colInfo)
		columnNames[i] = ""
		if colinfoRes == 1 {
			columnNames[i] = C.GoString(colInfo.name)
		}
	}

	return columnNames
}

func (rows *rows) ColumnTypeDatabaseTypeName(index int) string {
	dbTypeName := ""
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)
	if colinfoRes == 1 {
		dbTypeName = getNativeType(int(colInfo.native_type))
	}

	return dbTypeName
}

func (rows *rows) ColumnTypeNullable(index int) (bool, bool) {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)

	nullable := false
	ok := false
	if colinfoRes == 1 {
		ok = true
		if colInfo.nullable == 1 {
			nullable = true
		}
	}

	return nullable, ok
}

func (rows *rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)

	precision := int64(0)
	scale := int64(0)
	ok := false
	if colinfoRes == 1 && colInfo.native_type == C.DT_DECIMAL {
		precision = int64(colInfo.precision)
		scale = int64(colInfo.scale)
		ok = true
	}

	return precision, scale, ok
}

func (rows *rows) ColumnTypeLength(index int) (int64, bool) {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
	defer C.free(unsafe.Pointer(colInfo))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)

	length := int64(0)
	ok := false
	if colinfoRes == 1 {
		dbcapiType := colInfo._type
		if dbcapiType == C.A_BINARY {
			length = int64(colInfo.max_size)
			ok = true
		} else if dbcapiType == C.A_STRING {
			//date, time, timestamp and decimal types are represented using string since:
			//1. dbcapi does not support date, time and timestamp types.
			//2. Go 1.8 does not have any decimal types
			hanaType := colInfo.native_type
			if hanaType != C.DT_DATE && hanaType != C.DT_TIME && hanaType != C.DT_TIMESTAMP && hanaType != C.DT_DECIMAL &&
				hanaType != C.DT_DAYDATE && hanaType != C.DT_SECONDTIME &&
				hanaType != C.DT_LONGDATE && hanaType != C.DT_SECONDDATE {
				length = int64(colInfo.max_size)
				ok = true
			}
		}
	}

	return length, ok
}

func (rows *rows) ColumnTypeScanType(index int) reflect.Type {
	colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
	colinfoRes := C.dbcapi_get_column_info(rows.stmt, C.dbcapi_u32(index), colInfo)
	defer C.free(unsafe.Pointer(colInfo))

	if colinfoRes != 1 {
		//This function does not allow you to return an error. Return string since
		//most types can be converted to/from string.
		return reflect.TypeOf(string(""))
	}

	dbcapiType := colInfo._type
	switch dbcapiType {
	case C.A_BINARY:
		return reflect.TypeOf([]byte{0})
	case C.A_STRING:
		hanaType := colInfo.native_type
		//date, time, timestamp and decimal types are represented using string since dbcapi
		//does not support date, time and timestamp types.
		if hanaType == C.DT_DATE || hanaType == C.DT_TIME || hanaType == C.DT_TIMESTAMP ||
			hanaType == C.DT_DAYDATE || hanaType == C.DT_SECONDTIME ||
			hanaType == C.DT_LONGDATE || hanaType == C.DT_SECONDDATE {
			return reflect.TypeOf(time.Time(time.Time{}))
		}
		return reflect.TypeOf(string(""))
	case C.A_DOUBLE:
		return reflect.TypeOf(float64(0.0))
	case C.A_VAL64:
		return reflect.TypeOf(int64(0))
	case C.A_UVAL64:
		return reflect.TypeOf(uint64(0))
	case C.A_VAL32:
		return reflect.TypeOf(int32(0))
	case C.A_UVAL32:
		return reflect.TypeOf(uint32(0))
	case C.A_VAL16:
		return reflect.TypeOf(int16(0))
	case C.A_UVAL16:
		return reflect.TypeOf(uint16(0))
	case C.A_VAL8:
		return reflect.TypeOf(int8(0))
	case C.A_UVAL8:
		return reflect.TypeOf(uint8(0))
	case C.A_FLOAT:
		return reflect.TypeOf(float32(0.0))
	default:
		return reflect.TypeOf(string(""))
	}
}

func getNativeType(nativeTypeCode int) string {
	switch nativeTypeCode {
	case C.DT_DATE:
		return "DATE"
	case C.DT_DAYDATE:
		return "DATE"
	case C.DT_TIME:
		return "TIME"
	case C.DT_SECONDTIME:
		return "TIME"
	case C.DT_TIMESTAMP:
		return "TIMESTAMP"
	case C.DT_LONGDATE:
		return "TIMESTAMP"
	case C.DT_SECONDDATE:
		return "TIMESTAMP"
	case C.DT_VARCHAR1:
		return "VARCHAR"
	case C.DT_VARCHAR2:
		return "VARCHAR"
	case C.DT_ALPHANUM:
		return "ALPHANUM"
	case C.DT_CHAR:
		return "CHAR"
	case C.DT_CLOB:
		return "CLOB"
	case C.DT_STRING:
		return "STRING"
	case C.DT_DOUBLE:
		return "DOUBLE"
	case C.DT_REAL:
		return "REAL"
	case C.DT_DECIMAL:
		return "DECIMAL"
	case C.DT_INT:
		return "INT"
	case C.DT_SMALLINT:
		return "SMALLINT"
	case C.DT_BINARY:
		return "BINARY"
	case C.DT_VARBINARY:
		return "VARBINARY"
	case C.DT_BSTRING:
		return "BSTRING"
	case C.DT_BLOB:
		return "BLOB"
	case C.DT_ST_GEOMETRY:
		return "ST_GEOMETRY"
	case C.DT_ST_POINT:
		return "ST_POINT"
	case C.DT_TINYINT:
		return "TINYINT"
	case C.DT_BIGINT:
		return "BIGINT"
	case C.DT_BOOLEAN:
		return "BOOLEAN"
	case C.DT_NSTRING:
		return "NSTRING"
	case C.DT_SHORTTEXT:
		return "SHORTTEXT"
	case C.DT_NCHAR:
		return "NCHAR"
	case C.DT_NVARCHAR:
		return "NVARCHAR"
	case C.DT_NCLOB:
		return "NCLOB"
	case C.DT_TEXT:
		return "TEXT"
	case C.DT_BINTEXT:
		return "BINTEXT"
	default:
		return "UNKNOWN"
	}
}

func (rows *rows) Next(dest []driver.Value) error {
	fetchNextRes := C.dbcapi_fetch_next(rows.stmt)
	if fetchNextRes != 1 {
		if isBadConnection(rows.conn) {
			return driver.ErrBadConn
		}
		return io.EOF
	}

	return rows.getNextRowData(dest)
}

func (rows *rows) HasNextResultSet() bool {
	return true
}

func (rows *rows) NextResultSet() error {
	nextRs := C.dbcapi_get_next_result(rows.stmt)
	if nextRs != 1 {
		return io.EOF
	}

	return nil
}

func (rows *rows) getNextRowData(dest []driver.Value) error {
	col := (*C.struct_dbcapi_data_value)(C.malloc(C.sizeof_struct_dbcapi_data_value))
	defer C.free(unsafe.Pointer(col))

	for i := 0; i < len(dest); i++ {
		getCol := C.dbcapi_get_column(rows.stmt, C.dbcapi_u32(i), col)
		if getCol != 1 {
			return getError(rows.conn)
		}

		err := getData(col, &dest[i], i, rows.stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func getData(col *C.struct_dbcapi_data_value, val *driver.Value, index int, stmt *C.dbcapi_stmt) error {
	if col.is_null != nil && *col.is_null == 1 {
		*val = nil
		return nil
	}

	colType := col._type
	switch colType {
	case C.A_BINARY:
		//Go 1.8 does not have any lob types so we need to fetch the entire lob in memory
		bArr := make([]byte, int(*col.length))
		//Varbinary data in HANA can have a max size of 5000. If the size is greater than
		//5000, then we must be dealing with lobs.
		if *col.length <= MAX_BINARY_SIZE {
			copy(bArr, (*[MAX_BINARY_SIZE]byte)(unsafe.Pointer(col.buffer))[:])
			*val = bArr
		} else {
			*val = convertToLargeByteArray(col.buffer, uint(*col.length))
		}

	case C.A_STRING:
		strPtr := (*C.char)(unsafe.Pointer(col.buffer))
		strVal := C.GoString(strPtr)
		colInfo := (*C.struct_dbcapi_column_info)(C.malloc(C.sizeof_struct_dbcapi_column_info))
		defer C.free(unsafe.Pointer(colInfo))
		colinfoRes := C.dbcapi_get_column_info(stmt, C.dbcapi_u32(index), colInfo)
		if colinfoRes != 1 {
			return errors.New(fmt.Sprintf("Could not determine the column type for the: %d column", index+1))
		}

		hanaType := colInfo.native_type
		if hanaType == C.DT_TIMESTAMP || hanaType == C.DT_LONGDATE {
			tsVal, err := time.Parse(TS_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = tsVal
		} else if hanaType == C.DT_SECONDDATE {
			sdVal, err := time.Parse(SECONDDATE_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = sdVal
		} else if hanaType == C.DT_TIME || hanaType == C.DT_SECONDTIME {
			tVal, err := time.Parse(TIME_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = tVal
		} else if hanaType == C.DT_DATE || hanaType == C.DT_DAYDATE {
			dVal, err := time.Parse(DATE_LAYOUT_STRING, strVal)
			if err != nil {
				return err
			}
			*val = dVal
		} else {
			*val = strVal
		}
	case C.A_DOUBLE:
		doublePtr := (*float64)(unsafe.Pointer(col.buffer))
		*val = *doublePtr
	case C.A_VAL64:
		longPtr := (*int64)(unsafe.Pointer(col.buffer))
		*val = *longPtr
	case C.A_UVAL64:
		longPtr := (*uint64)(unsafe.Pointer(col.buffer))
		*val = *longPtr
	case C.A_VAL32:
		intPtr := (*int32)(unsafe.Pointer(col.buffer))
		*val = *intPtr
	case C.A_UVAL32:
		intPtr := (*uint32)(unsafe.Pointer(col.buffer))
		*val = *intPtr
	case C.A_VAL16:
		shortPtr := (*int16)(unsafe.Pointer(col.buffer))
		*val = *shortPtr
	case C.A_UVAL16:
		shortPtr := (*uint16)(unsafe.Pointer(col.buffer))
		*val = *shortPtr
	case C.A_VAL8:
		bytePtr := (*int8)(unsafe.Pointer(col.buffer))
		*val = *bytePtr
	case C.A_UVAL8:
		bytePtr := (*uint8)(unsafe.Pointer(col.buffer))
		*val = *bytePtr
	case C.A_FLOAT:
		floatPtr := (*float32)(unsafe.Pointer(col.buffer))
		*val = *floatPtr
	default:
		return errors.New("Unknown type")
	}

	return nil
}

func convertToLargeByteArray(buffer *C.char, size uint) []byte {
	res := make([]byte, size)
	s := res[:]
	ptr := uintptr(unsafe.Pointer(buffer))
	for size > MAX_BINARY_SIZE {
		copy(s, (*[MAX_BINARY_SIZE]byte)(unsafe.Pointer(ptr))[:])
		s = s[MAX_BINARY_SIZE:]
		size -= MAX_BINARY_SIZE
		ptr += MAX_BINARY_SIZE
	}
	if size > 0 {
		copy(s, (*[MAX_BINARY_SIZE]byte)(unsafe.Pointer(ptr))[:size])
	}
	return res
}

//Ignores the context param since dbcapi does not support setting timeout on non-prepared statement
func (connection *connection) Ping(ctx context.Context) error {
	query := "SELECT 'PING' FROM SYS.DUMMY"
	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))
	stmt := C.dbcapi_execute_direct(connection.conn, sql)

	if stmt == nil {
		return driver.ErrBadConn
	}

	C.dbcapi_free_stmt(stmt)
	return nil
}

func (statement *statement) Close() error {
	C.dbcapi_free_stmt(statement.stmt)
	return nil
}

func (statement *statement) NumInput() int {
	// We don't let the sql package handle the validation, since we now support bulk parameters
	// passed in as an array of an array [][] interface{} so we return -1 here
	return -1
}

func (statement *statement) Exec(args []driver.Value) (driver.Result, error) {
	// This needs to find, from this level, whether or not we are dealing with [][]interface{}
	// or regular values.
	// 1) If we are dealing with regular parameters, proceed as normal
	// 2) If we are dealing with an [][]interface{} this will be the
	// only parameter [len(args) == 1].
	//    a) Bindings will be consumed up to how many rows contain the same types of columns
	//       consecutively. Once a row is found that is not similar, what has been batched so far is
	//       sent off
	//    b) Variable length arguments may use the metadata length reported to know the buffer size
	//       to use
	// 3) [][]interface{} and separate parameters cannot be mixed
	functionCode := C.dbcapi_get_function_code(statement.stmt)
	argsLength := len(args)
	var err error
	if argsLength == 1 {
		v := reflect.ValueOf(args[0])
		if bulkValues, ok := v.Interface().([][]interface{}); ok {
			if (functionCode != FunctionCode_INSERT) &&
				(functionCode != FunctionCode_UPDATE) &&
				(functionCode != FunctionCode_DELETE) {
				return nil, errors.New("Batching is only supported for Inserts/Updates/Deletes")
			}

			err = statement.executeBatchInternal(bulkValues)
		} else {
			err = statement.executeInternal(args)
		}
	} else {
		err = statement.executeInternal(args)
	}

	if err != nil {
		return nil, err
	}

	if functionCode == FunctionCode_DDL {
		return driver.ResultNoRows, nil
	}
	rowsAffected := C.dbcapi_affected_rows(statement.stmt)
	if rowsAffected == -1 {
		return &result{rowsAffected: -1, err: getError(statement.conn)}, nil
	} else {
		return &result{rowsAffected: int64(rowsAffected), err: nil}, nil
	}
}

func (statement *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	value := make([]driver.Value, len(args))
	for i := 0; i < len(args); i++ {
		value[i] = args[i].Value
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return statement.Exec(value)
	} else {
		err := setTimeout(statement, deadline.Sub(time.Now()).Seconds())
		if err != nil {
			return nil, err
		}
		return statement.Exec(value)
	}
}

func (statement *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	value := make([]driver.Value, len(args))
	for i := 0; i < len(args); i++ {
		value[i] = args[i].Value
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return statement.Query(value)
	} else {
		err := setTimeout(statement, deadline.Sub(time.Now()).Seconds())
		if err != nil {
			return nil, err
		}
		return statement.Query(value)
	}
}

func setTimeout(statement *statement, timeout float64) error {
	//Add +1 since float64 to int32 will cause some loss in precision
	timeoutRes := C.dbcapi_set_query_timeout(statement.stmt, C.dbcapi_i32(timeout+1))
	if timeoutRes != 1 {
		return getError(statement.conn)
	}

	return nil
}

func (statement *statement) Query(args []driver.Value) (driver.Rows, error) {
	for i := 0; i < len(args); i++ {
		bpv, err := statement.bindParameter(i, args[i])
		if bpv != nil {
			defer bpv.cleanup()
		}

		if err != nil {
			return nil, err
		}
	}

	execRes := C.dbcapi_execute(statement.stmt)
	if execRes != 1 {
		return nil, getError(statement.conn)
	}

	return &rows{stmt: statement.stmt, conn: statement.conn, isFromStatement: true, isClosed: false, err: nil}, nil
}

func (result *result) LastInsertId() (int64, error) {
	return -1, errors.New("Feature not supported")
}

func (result *result) RowsAffected() (int64, error) {
	if result.err != nil {
		return 0, result.err
	}
	return result.rowsAffected, nil
}

func (statement *statement) executeInternal(args []driver.Value) error {
	for i := 0; i < len(args); i++ {
		bpv, err := statement.bindParameter(i, args[i])
		defer bpv.cleanup()

		if err != nil {
			return err
		}
	}

	execRes := C.dbcapi_execute(statement.stmt)

	if execRes != 1 {
		return getError(statement.conn)
	}

	return nil
}

//This function can be optimized since dbcapi is fixed now. The caller can allocate
//a buffer once and pass it to the function, rather then this function allocating
//buffer for a single use.
func (statement *statement) bindParameter(index int, paramVal driver.Value) (*boundParameterValue, error) {
	// All pointer memory is to be deallocated by the caller
	bpv := new(boundParameterValue)
	bpv.boundStruct = (*C.struct_dbcapi_bind_data)(C.malloc(C.sizeof_struct_dbcapi_bind_data))
	descRes := C.dbcapi_describe_bind_param(statement.stmt, C.dbcapi_u32(index), bpv.boundStruct)

	if descRes != 1 {
		return bpv, getError(statement.conn)
	}

	bpv.boundStruct.value.is_address = C.dbcapi_bool(0)
	if paramVal == nil {
		nullIndicator := (*C.dbcapi_bool)(C.malloc(C.sizeof_dbcapi_bool))
		*nullIndicator = C.dbcapi_bool(1)
		bpv.boundStruct.value.is_null = nullIndicator
	} else {
		v := reflect.ValueOf(paramVal)
		length := (*C.size_t)(C.malloc(C.sizeof_size_t))
		bpv.boundStruct.value.length = length

		switch v.Kind() {
		case reflect.Uint8:
			fallthrough
		case reflect.Uint16:
			fallthrough
		case reflect.Uint32:
			fallthrough
		case reflect.Uint64:
			fallthrough
		case reflect.Uint:
			*length = C.size_t(8)
			iVal := (*C.uint64_t)(C.malloc(C.sizeof_uint64_t))
			*iVal = C.uint64_t(v.Uint())
			bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(iVal))
			bpv.boundStruct.value._type = C.A_UVAL64
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			fallthrough
		case reflect.Int:
			*length = C.size_t(8)
			iVal := (*C.int64_t)(C.malloc(C.sizeof_int64_t))
			*iVal = C.int64_t(v.Int())
			bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(iVal))
			bpv.boundStruct.value._type = C.A_VAL64
		case reflect.Bool:
			*length = C.size_t(1)
			bVal := (*C.bool)(C.malloc(C.sizeof_bool))
			*bVal = C.bool(v.Bool())
			bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(bVal))
			bpv.boundStruct.value._type = C.A_VAL8
		case reflect.Float32:
			fallthrough
		case reflect.Float64:
			*length = C.size_t(8)
			dVal := (*C.double)(C.malloc(C.sizeof_double))
			*dVal = C.double(v.Float())
			bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(dVal))
			bpv.boundStruct.value._type = C.A_DOUBLE
		case reflect.String:
			strVal := v.String()
			stringLen := len(strVal)
			a := make([]byte, stringLen+1)
			copy(a[:], strVal)
			bpv.boundStruct.value.buffer = (*C.char)(C.malloc(C.size_t(stringLen + 1)))
			C.memset(unsafe.Pointer(bpv.boundStruct.value.buffer), C.int(0), C.size_t(stringLen+1))
			C.strncpy(bpv.boundStruct.value.buffer, (*C.char)(unsafe.Pointer(&a[0])), C.size_t(stringLen))
			*length = C.size_t(stringLen)
			bpv.boundStruct.value._type = C.A_STRING
		case reflect.Slice:
			if bArr, ok := v.Interface().([]byte); ok {
				bArrLen := len(bArr)
				*length = C.size_t(bArrLen)
				if len(bArr) > 0 {
					bpv.boundStruct.value.buffer = (*C.char)(C.malloc(C.size_t(bArrLen)))
					C.memcpy(unsafe.Pointer(bpv.boundStruct.value.buffer), unsafe.Pointer(&bArr[0]), C.size_t(bArrLen))
				} else {
					a := make([]byte, 1)
					bpv.boundStruct.value.buffer = (*C.char)(C.malloc(C.size_t(1)))
					C.memcpy(unsafe.Pointer(bpv.boundStruct.value.buffer), unsafe.Pointer(&a[0]), C.size_t(1))
				}
				bpv.boundStruct.value._type = C.A_BINARY
			} else {
				return nil, errors.New("Unknown type")
			}
		default:
			if tVal, ok := v.Interface().(time.Time); ok {
				paramInfo := (*C.dbcapi_bind_param_info)(C.malloc(C.sizeof_struct_dbcapi_bind_param_info))
				defer C.free(unsafe.Pointer(paramInfo))
				paramInfoResult := C.dbcapi_get_bind_param_info(statement.stmt, C.dbcapi_u32(index), paramInfo)

				if paramInfoResult == C.dbcapi_bool(0) {
					return bpv, errors.New("Could not get Parameter Metadata for the column")
				}

				var insertTimeVal time.Time

				if convertTimestampToUTC {
					insertTimeVal = tVal.UTC()
				} else {
					insertTimeVal = tVal
				}

				strVal := ""

				if (insertTimeVal.Year() == 0) && //Server does not support the year = 0 for DATE and TIMESTAMP type columns
					((paramInfo.native_type == C.dbcapi_native_type(C.DT_DAYDATE)) ||
						(paramInfo.native_type == C.dbcapi_native_type(C.DT_DATE)) ||
						(paramInfo.native_type == C.dbcapi_native_type(C.DT_LONGDATE)) ||
						(paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDDATE))) {
					if convertTimestampToUTC {
						return bpv, fmt.Errorf(
							"Specified date '%v' would result in a UTC date '%v' with year = 0, which is not supported by the server",
							tVal, insertTimeVal)
					}
					return bpv, fmt.Errorf("Specified date '%v' with year = 0 is not supported by the server", insertTimeVal)
				}
				if paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDTIME) || // TIME
					paramInfo.native_type == C.dbcapi_native_type(C.DT_TIME) {
					strVal = insertTimeVal.Format(TIME_LAYOUT_STRING)
				} else if paramInfo.native_type == C.dbcapi_native_type(C.DT_DAYDATE) || // DATE
					paramInfo.native_type == C.dbcapi_native_type(C.DT_DATE) {
					strVal = insertTimeVal.Format(DATE_LAYOUT_STRING)
				} else if paramInfo.native_type == C.dbcapi_native_type(C.DT_LONGDATE) || // TIMESTAMP
					paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDDATE) {
					if insertTimeVal.Nanosecond() == 0 {
						strVal = insertTimeVal.Format(SECONDDATE_LAYOUT_STRING)
					} else {
						strVal = insertTimeVal.Format(TS_LAYOUT_STRING)
					}
				} else { // Non-DATE|TIME|TIMESTAMP type - let the conversion success/failure be determined by lower levels
					strVal = insertTimeVal.Format(TS_LAYOUT_STRING)
				}

				stringLen := len(strVal)
				a := make([]byte, stringLen+1)
				copy(a[:], strVal)
				bpv.boundStruct.value.buffer = (*C.char)(C.malloc(C.size_t(stringLen + 1)))
				C.memset(unsafe.Pointer(bpv.boundStruct.value.buffer), C.int(0), C.size_t(stringLen+1))
				C.strncpy(bpv.boundStruct.value.buffer, (*C.char)(unsafe.Pointer(&a[0])), C.size_t(stringLen))
				*length = C.size_t(stringLen)
				bpv.boundStruct.value._type = C.A_STRING
			} else {
				return bpv, fmt.Errorf("Unknown type: %s ", v.Kind())
			}
		}
	}

	bindRes := C.dbcapi_bind_param(statement.stmt, C.dbcapi_u32(index), bpv.boundStruct)
	if bindRes != 1 {
		return bpv, getError(statement.conn)
	}
	return bpv, nil
}

func (statement *statement) executeBatchInternal(bulkValues [][]interface{}) error {
	// Bindings will be consumed up to how many rows contain the same types of columns
	// consecutively. Once a row is found that is not similar, what has been batched so far is
	// sent off and the logic continues with the next set of rows until the batch is consumed.
	// It is recommended to use the same types in all columns for all rows, for maximum efficiency.
	rows := len(bulkValues)

	if rows == 0 {
		return errors.New("No values provided")
	}

	var (
		err                 error
		boundParameterInfos *paramInfoValues
	)

	for rowIndex := 0; rowIndex < rows; {
		startRow := rowIndex
		var startingRowValues []reflect.Value
		doBoundParameterInfosCleanup := (boundParameterInfos == nil)

		startingRowValues, boundParameterInfos, err =
			statement.getColumnTypesForRow(bulkValues[startRow], rowIndex, boundParameterInfos)
		doBoundParameterInfosCleanup = doBoundParameterInfosCleanup && (boundParameterInfos != nil)
		if doBoundParameterInfosCleanup {
			defer boundParameterInfos.cleanup()
		}
		if err != nil {
			return err
		}
		rowIndex++

		for rowIndex < rows {
			include, err := statement.rowCanBeIncludedInBatch(bulkValues[rowIndex],
				startingRowValues, boundParameterInfos.paramInfos, rowIndex)

			if err != nil {
				return err
			} else if !include {
				break
			}

			rowIndex++
		}

		if err != nil {
			return err
		}

		res := C.dbcapi_set_batch_size(statement.stmt, C.dbcapi_u32(rowIndex-startRow))

		if res != 1 {
			return getError(statement.conn)
		}

		err = statement.bindBulkParametersAndExecute(startRow, rowIndex-1, startingRowValues,
			bulkValues, boundParameterInfos.paramInfos)

		if err != nil {
			return err
		}
	}

	return nil
}

func (statement *statement) getColumnTypesForRow(row []interface{}, rowIndex int,
	boundParameterInfos *paramInfoValues) ([]reflect.Value, *paramInfoValues, error) {
	rowLength := len(row)
	columnValues := make([]reflect.Value, rowLength)
	retrieveParamInfos := boundParameterInfos == nil

	if retrieveParamInfos {
		boundParameterInfos = new(paramInfoValues)
		boundParameterInfos.paramInfos = make([]*C.dbcapi_bind_param_info, rowLength)
	}

	var err error
	for columnIndex, column := range row {
		var paramInfo *C.dbcapi_bind_param_info
		if retrieveParamInfos {
			paramInfo, err = statement.retrieveParamInfo(columnIndex)
			boundParameterInfos.paramInfos[columnIndex] = paramInfo

			if err != nil {
				break
			}
		} else {
			paramInfo = boundParameterInfos.paramInfos[columnIndex]
		}
		columnValue := reflect.ValueOf(column)
		if columnValue.IsValid() {
			err = statement.isAcceptableInputParameterType(columnValue, paramInfo, rowIndex,
				columnIndex)

			if err != nil {
				break
			}
		}
		columnValues[columnIndex] = columnValue
	}

	return columnValues, boundParameterInfos, err
}

func (statement *statement) retrieveParamInfo(columnIndex int) (*C.dbcapi_bind_param_info, error) {
	// Will be deallocated by caller
	paramInfo := (*C.dbcapi_bind_param_info)(C.malloc(C.sizeof_struct_dbcapi_bind_param_info))
	paramInfoResult := C.dbcapi_get_bind_param_info(statement.stmt,
		C.dbcapi_u32(columnIndex), paramInfo)

	if paramInfoResult == C.dbcapi_bool(0) {
		return paramInfo, fmt.Errorf("Could not get Parameter Metadata for the column %d",
			columnIndex+1)
	}

	return paramInfo, nil
}

func (statement *statement) rowCanBeIncludedInBatch(row []interface{},
	startingRowValues []reflect.Value, paramInfos []*C.dbcapi_bind_param_info,
	rowIndex int) (bool, error) {
	if len(row) != len(startingRowValues) {
		return false, errors.New("All of the batch rows do not have the same length")
	}

	canBeBatched := true

	for columnIndex, columnInterface := range row {
		columnValue := reflect.ValueOf(columnInterface)
		startingRowValue := startingRowValues[columnIndex]

		// nil (NULL) matches any previous column value type
		if !startingRowValue.IsValid() {
			// If any of the startingRowValues are nil and the current row's column value is not
			// nil, then set the startingRowValue to the value of the current row's column - it will
			// be used in future comparisons
			if columnValue.IsValid() {
				startingRowValues[columnIndex] = columnValue
			}
		} else if columnValue.IsValid() {
			err := statement.isAcceptableInputParameterType(columnValue, paramInfos[columnIndex],
				rowIndex, columnIndex)
			if err != nil {
				return false, fmt.Errorf("Unsupported type (row %d, column %d)", rowIndex+1,
					columnIndex+1)
			}

			canBeBatched = columnValue.Kind() == startingRowValue.Kind()
		} // !columnValue.isValid() => nil [NULL] (which matches any type)

		if !canBeBatched {
			break
		}
	}
	return canBeBatched, nil
}

func (statement *statement) isAcceptableInputParameterType(value reflect.Value,
	paramInfo *C.dbcapi_bind_param_info, rowIndex int, columnIndex int) error {
	switch value.Kind() {
	case reflect.Slice:
		if _, ok := value.Interface().([]byte); !ok {
			return fmt.Errorf("Unsupported type (row %d, column %d)", rowIndex+1, columnIndex+1)
		}
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		fallthrough
	case reflect.Bool:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.String:
		return nil
	default:
		if tVal, ok := value.Interface().(time.Time); ok {
			var insertTimeVal time.Time

			if convertTimestampToUTC {
				insertTimeVal = tVal.UTC()
			} else {
				insertTimeVal = tVal
			}

			// Server does not support the year = 0 for DATE and TIMESTAMP type columns
			if (insertTimeVal.Year() == 0) &&
				((paramInfo.native_type == C.dbcapi_native_type(C.DT_DAYDATE)) ||
					(paramInfo.native_type == C.dbcapi_native_type(C.DT_DATE)) ||
					(paramInfo.native_type == C.dbcapi_native_type(C.DT_LONGDATE)) ||
					(paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDDATE))) {
				if convertTimestampToUTC {
					return fmt.Errorf(
						"Specified date '%v' would result in a UTC date '%v' with year = 0, "+
							"which is not supported by the server", tVal, insertTimeVal)
				}
				return fmt.Errorf("Specified date '%v' with year = 0 is not supported "+
					"by the server", insertTimeVal)
			}
			return nil
		}
		return fmt.Errorf("Unsupported type (row %d, column %d)", rowIndex+1, columnIndex+1)
	}
}

func (statement *statement) bindBulkParametersAndExecute(startRow int, endRow int,
	columnValues []reflect.Value, bulkValues [][]interface{},
	paramInfos []*C.dbcapi_bind_param_info) error {

	boundColumns, err := statement.bindBulkParameters(startRow, endRow, columnValues, bulkValues,
		paramInfos)

	for _, boundColumn := range boundColumns {
		if boundColumn != nil {
			defer boundColumn.cleanup()
		}
	}

	if err != nil {
		return err
	}

	execRes := C.dbcapi_execute(statement.stmt)

	if execRes != 1 {
		return getError(statement.conn)
	}

	return nil
}

func (statement *statement) bindBulkParameters(startRow int, endRow int,
	columnValues []reflect.Value, bulkValues [][]interface{},
	paramInfos []*C.dbcapi_bind_param_info) ([]*boundParameterValue, error) {
	numberOfColumns := len(columnValues)
	// boundColumns always needs to be returned as the memory is deallocated by the caller using
	// using defer boundColumns[index].cleanup()
	boundColumns := make([]*boundParameterValue, numberOfColumns)
	var err error

	// Loop through the columns kinds, and make a buffer array for it
	for i := 0; i < numberOfColumns; i++ {
		columnValue := columnValues[i]

		var bpv *boundParameterValue
		var err error
		if !columnValue.IsValid() {
			// this is only the case if all of the rows for a particular column had nil values
			bpv, err = statement.bindBulkNullValuesForColumn(i, startRow, endRow, paramInfos[i])
		} else {
			bpv, err = statement.bindBulkRegularParameters(i, columnValue, bulkValues, startRow,
				endRow, paramInfos[i])
		}

		boundColumns[i] = bpv

		if err != nil {
			break
		}
	}
	return boundColumns, err
}

func (statement *statement) bindBulkNullValuesForColumn(columnIndex int, startRow int,
	endRow int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	bpv.boundStruct.value.buffer = (*C.char)(C.malloc(pointerSize * C.size_t(batchSize)))
	lengths := bpv.boundStruct.value.length

	for i := startRow; i <= endRow; i++ {
		outputValueIndex := i - startRow
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		*length = 0
		nullIndicator :=
			(*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
				uintptr(nullSize*C.size_t(outputValueIndex))))
		*nullIndicator = C.dbcapi_bool(1)
	}

	bindRes := C.dbcapi_bind_param(statement.stmt, C.dbcapi_u32(columnIndex), bpv.boundStruct)

	if bindRes != 1 {
		return bpv, getError(statement.conn)
	}

	return bpv, nil
}

func (statement *statement) bindBulkRegularParameters(columnIndex int,
	columnValue reflect.Value, bulkValues [][]interface{}, startRow int, endRow int,
	paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	var bpv *boundParameterValue
	var err error

	switch columnValue.Kind() {
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Uint:
		bpv, err = statement.bindIntegerBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo, false)
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		bpv, err = statement.bindIntegerBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo, true)
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		bpv, err = statement.bindFloatBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo)
	case reflect.Bool:
		bpv, err = statement.bindBooleanBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo)
	case reflect.String:
		bpv, err = statement.bindStringBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo)
	case reflect.Slice:
		bpv, err = statement.bindByteArrayBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo)
	default: // Time batch (validated in rowCanBeIncludedInBatch())
		bpv, err = statement.bindTimeBatch(columnIndex, bulkValues, startRow, endRow,
			paramInfo)
	}

	if err != nil {
		return bpv, err
	}

	bindRes := C.dbcapi_bind_param(statement.stmt, C.dbcapi_u32(columnIndex), bpv.boundStruct)

	if bindRes != 1 {
		return bpv, getError(statement.conn)
	}
	return bpv, nil
}

func (statement *statement) initializeBatchBoundParametrValuesForColumn(columnIndex int,
	batchSize int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv := new(boundParameterValue)
	bpv.boundStruct = (*C.struct_dbcapi_bind_data)(C.malloc(C.sizeof_struct_dbcapi_bind_data))
	C.memset(unsafe.Pointer(bpv.boundStruct), C.int(0), C.sizeof_struct_dbcapi_bind_data)
	descRes := C.dbcapi_describe_bind_param(statement.stmt, C.dbcapi_u32(columnIndex),
		bpv.boundStruct)

	if descRes != 1 {
		return bpv, getError(statement.conn)
	}

	bpv.boundStruct.value.is_address = C.dbcapi_bool(1)
	nullIndicators := (*C.dbcapi_bool)(C.malloc(nullSize * C.size_t(batchSize)))
	C.memset(unsafe.Pointer(nullIndicators), C.int(0), nullSize*C.size_t(batchSize))
	bpv.boundStruct.value.is_null = nullIndicators
	lengths := (*C.size_t)(C.malloc(C.size_t(batchSize) * C.sizeof_size_t))
	C.memset(unsafe.Pointer(lengths), C.int(0), C.sizeof_size_t*C.size_t(batchSize))
	bpv.boundStruct.value.length = lengths
	return bpv, nil
}

func (statement *statement) bindIntegerBatch(columnIndex int, bulkValues [][]interface{},
	startRow int, endRow int, paramInfo *C.dbcapi_bind_param_info,
	signed bool) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	lengths := bpv.boundStruct.value.length

	valueLen := C.size_t(C.sizeof_int64_t)
	iVals := (**C.char)(C.malloc(pointerSize * C.size_t(batchSize)))

	for i := startRow; i <= endRow; i++ {
		outputValueIndex := i - startRow
		theValue := reflect.ValueOf(bulkValues[i][columnIndex])
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		*length = valueLen
		nullIndicator :=
			(*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
				uintptr(nullSize*C.size_t(outputValueIndex))))
		if theValue.IsValid() {
			*nullIndicator = C.dbcapi_bool(0)
			if signed {
				iVal := (*C.int64_t)(C.malloc(C.size_t(C.sizeof_int64_t)))
				*iVal = C.int64_t(theValue.Int())
				iValPos := (**C.int64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(iVals)) +
					uintptr(pointerSize*C.size_t(outputValueIndex))))
				*iValPos = iVal
			} else {
				iVal := (*C.uint64_t)(C.malloc(C.size_t(C.sizeof_uint64_t)))
				*iVal = C.uint64_t(theValue.Uint())
				iValPos := (**C.uint64_t)(unsafe.Pointer(uintptr(unsafe.Pointer(iVals)) +
					uintptr(pointerSize*C.size_t(outputValueIndex))))
				*iValPos = iVal
			}
		} else {
			*nullIndicator = C.dbcapi_bool(1)
		}
	}

	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(iVals))
	if signed {
		bpv.boundStruct.value._type = C.A_VAL64
	} else {
		bpv.boundStruct.value._type = C.A_UVAL64
	}
	return bpv, nil
}

func (statement *statement) bindFloatBatch(columnIndex int, bulkValues [][]interface{},
	startRow int, endRow int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	lengths := bpv.boundStruct.value.length

	valueLen := C.size_t(C.sizeof_int64_t)
	dVals := (**C.double)(C.malloc(pointerSize * C.size_t(batchSize)))

	for i := startRow; i <= endRow; i++ {
		outputValueIndex := i - startRow
		theValue := reflect.ValueOf(bulkValues[i][columnIndex])
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		*length = valueLen
		nullIndicator :=
			(*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
				uintptr(nullSize*C.size_t(outputValueIndex))))
		if theValue.IsValid() {
			*nullIndicator = C.dbcapi_bool(0)
			dVal := (*C.double)(C.malloc(C.size_t(C.sizeof_double)))
			*dVal = C.double(theValue.Float())
			dValPos := (**C.double)(unsafe.Pointer(uintptr(unsafe.Pointer(dVals)) +
				uintptr(pointerSize*C.size_t(outputValueIndex))))
			*dValPos = dVal
		} else {
			*nullIndicator = C.dbcapi_bool(1)
		}
	}

	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(dVals))
	bpv.boundStruct.value._type = C.A_DOUBLE
	return bpv, nil
}

func (statement *statement) bindBooleanBatch(columnIndex int, bulkValues [][]interface{},
	startRow int, endRow int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	lengths := bpv.boundStruct.value.length

	valueLen := C.size_t(C.sizeof_uint8_t)
	bVals := (**C.uint8_t)(C.malloc(pointerSize * C.size_t(batchSize)))

	for i := startRow; i <= endRow; i++ {
		outputValueIndex := i - startRow
		theValue := reflect.ValueOf(bulkValues[i][columnIndex])
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		*length = valueLen
		nullIndicator :=
			(*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
				uintptr(nullSize*C.size_t(outputValueIndex))))
		if theValue.IsValid() {
			*nullIndicator = C.dbcapi_bool(0)
			bVal := (*C.uint8_t)(C.malloc(C.size_t(C.sizeof_uint8_t)))
			if theValue.Bool() {
				*bVal = C.uint8_t(1)
			} else {
				*bVal = C.uint8_t(0)
			}
			bValPos := (**C.uint8_t)(unsafe.Pointer(uintptr(unsafe.Pointer(bVals)) +
				uintptr(pointerSize*C.size_t(outputValueIndex))))
			*bValPos = bVal
		} else {
			*nullIndicator = C.dbcapi_bool(1)
		}
	}

	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(bVals))
	bpv.boundStruct.value._type = C.A_VAL8
	return bpv, nil
}

func (statement *statement) bindStringBatch(columnIndex int, bulkValues [][]interface{},
	startRow int, endRow int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	lengths := bpv.boundStruct.value.length

	strVals := (**C.char)(C.malloc(pointerSize * C.size_t(batchSize)))

	for i := startRow; i <= endRow; i++ {
		outputValueIndex := i - startRow
		theValue := reflect.ValueOf(bulkValues[i][columnIndex])
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		nullIndicator :=
			(*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
				uintptr(nullSize*C.size_t(outputValueIndex))))
		if theValue.IsValid() {
			*length = C.size_t(len(theValue.String()))
			*nullIndicator = C.dbcapi_bool(0)
			strValPos := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(strVals)) +
				uintptr(pointerSize*C.size_t(outputValueIndex))))
			*strValPos = C.CString(theValue.String())
		} else {
			*length = 0
			*nullIndicator = C.dbcapi_bool(1)
		}
	}

	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(strVals))
	bpv.boundStruct.value._type = C.A_STRING
	return bpv, nil
}

func (statement *statement) bindByteArrayBatch(columnIndex int, bulkValues [][]interface{},
	startRow int, endRow int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	lengths := bpv.boundStruct.value.length
	bArrVals := (**C.char)(C.malloc(pointerSize * C.size_t(batchSize)))
	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(bArrVals))
	bpv.boundStruct.value._type = C.A_BINARY

	for i := startRow; i <= endRow; i++ {
		outputValueIndex := i - startRow
		// The check for this is a []byte done earlier [in isAcceptableInputParameterType()],
		// here we just use the value here
		theValue := reflect.ValueOf(bulkValues[i][columnIndex])
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		nullIndicator := (*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
			uintptr(nullSize*C.size_t(outputValueIndex))))
		if theValue.IsValid() {
			bArrLen := len(theValue.Bytes())
			*length = C.size_t(bArrLen)
			bArrValPos := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(bArrVals)) +
				uintptr(pointerSize*C.size_t(outputValueIndex))))
			var bArrVal *C.char

			if bArrLen > 0 {
				bArrVal = (*C.char)(C.malloc(C.size_t(bArrLen)))
				C.memcpy(unsafe.Pointer(bArrVal), unsafe.Pointer(&theValue.Bytes()[0]),
					C.size_t(bArrLen))
			} else {
				a := make([]byte, 1)
				bArrVal = (*C.char)(C.malloc(C.size_t(1)))
				C.memcpy(unsafe.Pointer(bArrVal), unsafe.Pointer(&a[0]), C.size_t(1))
			}
			*bArrValPos = bArrVal
		} else {
			*length = 0
			*nullIndicator = C.dbcapi_bool(1)
		}
	}

	return bpv, nil
}

func (statement *statement) bindTimeBatch(columnIndex int, bulkValues [][]interface{},
	startRow int, endRow int, paramInfo *C.dbcapi_bind_param_info) (*boundParameterValue, error) {
	batchSize := endRow - startRow + 1
	// bpv always needs to be returned as the memory is deallocated by the caller using
	// using defer bpv.cleanup()
	bpv, err := statement.initializeBatchBoundParametrValuesForColumn(columnIndex, batchSize,
		paramInfo)

	if err != nil {
		return bpv, err
	}

	nullIndicators := bpv.boundStruct.value.is_null
	lengths := bpv.boundStruct.value.length
	strVals := (**C.char)(C.malloc(pointerSize * C.size_t(batchSize)))
	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(strVals))
	var firstError error

	for i := startRow; i <= endRow; i++ {
		theValue := reflect.ValueOf(bulkValues[i][columnIndex])
		outputValueIndex := i - startRow
		length :=
			(*C.size_t)(unsafe.Pointer(uintptr(unsafe.Pointer(lengths)) +
				uintptr(C.sizeof_size_t*C.size_t(outputValueIndex))))
		nullIndicator := (*C.dbcapi_bool)(unsafe.Pointer(uintptr(unsafe.Pointer(nullIndicators)) +
			uintptr(nullSize*C.size_t(outputValueIndex))))
		if theValue.IsValid() {
			// This validation is always done in rowCanBeIncludedInBatch(), so we can just get the
			// value here
			tVal, _ := theValue.Interface().(time.Time)
			strVal, err := statement.getAdjustedStringValueFromTime(i, columnIndex, tVal, paramInfo)

			strValPos := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(strVals)) +
				uintptr(pointerSize*C.size_t(outputValueIndex))))

			if firstError == nil && err != nil {
				// Allow for all values to be evaluated to simplify batch cleanup
				*strValPos = C.CString("")
				*length = 0
				firstError = err
			} else {
				*strValPos = C.CString(strVal)
				*length = C.size_t(len(strVal))
			}
		} else {
			*length = 0
			*nullIndicator = C.dbcapi_bool(1)
		}
	}

	bpv.boundStruct.value.buffer = (*C.char)(unsafe.Pointer(strVals))
	bpv.boundStruct.value._type = C.A_STRING
	return bpv, firstError
}

func (statement *statement) getAdjustedStringValueFromTime(rowIndex int, columnIndex int,
	tVal time.Time, paramInfo *C.dbcapi_bind_param_info) (string, error) {
	var insertTimeVal time.Time
	var strVal string

	if convertTimestampToUTC {
		insertTimeVal = tVal.UTC()
	} else {
		insertTimeVal = tVal
	}

	// Server does not support the year = 0 for DATE and TIMESTAMP type columns
	if (insertTimeVal.Year() == 0) &&
		((paramInfo.native_type == C.dbcapi_native_type(C.DT_DAYDATE)) ||
			(paramInfo.native_type == C.dbcapi_native_type(C.DT_DATE)) ||
			(paramInfo.native_type == C.dbcapi_native_type(C.DT_LONGDATE)) ||
			(paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDDATE))) {
		if convertTimestampToUTC {
			return strVal, fmt.Errorf(
				"Error for Time value in row %d, column %d: Specified date '%v' would result "+
					"in a UTC date '%v' with year = 0, which is not supported by the server",
				rowIndex, columnIndex, tVal, insertTimeVal)
		}
		return strVal, fmt.Errorf("Error for Time value in row %d, column %d: "+
			"Specified date '%v' with year = 0 is not supported by the server",
			rowIndex, columnIndex, insertTimeVal)
	}
	if paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDTIME) || // TIME
		paramInfo.native_type == C.dbcapi_native_type(C.DT_TIME) {
		strVal = insertTimeVal.Format(TIME_LAYOUT_STRING)
	} else if paramInfo.native_type == C.dbcapi_native_type(C.DT_DAYDATE) || // DATE
		paramInfo.native_type == C.dbcapi_native_type(C.DT_DATE) {
		strVal = insertTimeVal.Format(DATE_LAYOUT_STRING)
	} else if paramInfo.native_type == C.dbcapi_native_type(C.DT_LONGDATE) || // TIMESTAMP
		paramInfo.native_type == C.dbcapi_native_type(C.DT_SECONDDATE) {
		if insertTimeVal.Nanosecond() == 0 {
			strVal = insertTimeVal.Format(SECONDDATE_LAYOUT_STRING)
		} else {
			strVal = insertTimeVal.Format(TS_LAYOUT_STRING)
		}
	} else {
		// Non-DATE|TIME|TIMESTAMP type - let the conversion success/failure be determined by
		// The C layer implementation
		strVal = insertTimeVal.Format(TS_LAYOUT_STRING)
	}
	return strVal, nil
}

func (transaction *transaction) Commit() error {
	commitRes := C.dbcapi_commit(transaction.conn)
	if commitRes != 1 {
		return getError(transaction.conn)
	}

	autoCommRes := C.dbcapi_set_autocommit(transaction.conn, 1)
	if autoCommRes != 1 {
		return getError(transaction.conn)
	}

	err := setIsolationLevel(transaction.conn, DEFAULT_ISOLATION_LEVEL)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *transaction) Rollback() error {
	rollbackRes := C.dbcapi_rollback(transaction.conn)
	if rollbackRes != 1 {
		return getError(transaction.conn)
	}

	autoCommRes := C.dbcapi_set_autocommit(transaction.conn, 1)
	if autoCommRes != 1 {
		return getError(transaction.conn)
	}

	err := setIsolationLevel(transaction.conn, DEFAULT_ISOLATION_LEVEL)
	if err != nil {
		return err
	}

	return nil
}

func (nullTime *NullTime) Scan(value interface{}) error {
	nullTime.Time, nullTime.Valid = value.(time.Time)
	return nil
}

func (nullTime NullTime) Value() (driver.Value, error) {
	if !nullTime.Valid {
		return nil, nil
	}
	return nullTime.Time, nil
}

func (nullBytes *NullBytes) Scan(value interface{}) error {
	nullBytes.Bytes, nullBytes.Valid = value.([]byte)
	return nil
}

func (nullBytes NullBytes) Value() (driver.Value, error) {
	if !nullBytes.Valid {
		return nil, nil
	}
	return nullBytes.Bytes, nil
}
