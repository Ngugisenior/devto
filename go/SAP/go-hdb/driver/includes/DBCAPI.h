// ***************************************************************************
// Copyright (c) 2016 SAP SE or an SAP affiliate company. All rights reserved.
// ***************************************************************************
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// While not a requirement of the license, if you do modify this file, we
// would appreciate hearing about it. Please email
// dbcapi_interfaces@sap.com
//
// ***************************************************************************

#ifndef __DBCAPI_H__
#define __DBCAPI_H__

/** \mainpage the SAP HANA C API
 *
 * \section intro_sec Introduction
 * The HANA C application programming interface (API) is a data
 * access API for the C / C++ languages. The C API specification defines
 * a set of functions, variables and conventions that provide a consistent
 * database interface independent of the actual database being used. Using
 * the HANA C API, your C / C++ applications have direct access to
 * HANA database servers.
 *
 * The HANA C API simplifies the creation of C and C++ wrapper
 * drivers for several interpreted programming languages including Node.js,
 * PHP, Perl, Python, and Ruby. The HANA C API is layered on top of the
 * DBLIB package and it was implemented with Embedded SQL.
 *
 * Although it is not a replacement for SQLDBC, the HANA C API
 * simplifies the creation of applications using C and C++. You do not need
 * an advanced knowledge of SQLDBC to use the HANA C API.
 *
 * \section distribution Distribution of the API
 * The API is built as a dynamic link library (DLL) (\b libdbcapiHDB.dll) on
 * Microsoft Windows systems and as a shared object (\b libdbcapiHDB.so) on
 * Unix systems. The DLL is statically linked to the SQLDBC package of the
 * HANA version on which it is built. Applications using libdbcapiHDB.dll
 * can either link directly to it or load it dynamically. For more
 * information about dynamic loading, see the section "Dynamically
 * Loading the DLL".
 *
 * Descriptions of the HANA C API data types and entry points are
 * provided in the main header file (\b DBCAPI.h).
 *
 * \section dynamic_loading Dynamically Loading the DLL
 * The code to dynamically load the DLL is contained in the DBCAPI_DLL.cpp
 * source file. Applications must use the DBCAPI_DLL.h header file and
 * include the source code in DBCAPI_DLL.cpp. You can use the
 * dbcapi_initialize_interface method to dynamically load the DLL and
 * look up the entry points.
 *
 * \section threading_support Threading Support
 * The C API library is thread-unaware, meaning that the library does not
 * perform any tasks that require mutual exclusion. In order to allow the
 * library to work in threaded applications, there is only one rule to
 * follow: <b>no more than one request is allowed on a single connection </b>.
 * With this rule, the application is responsible for doing mutual exclusion
 * when accessing any connection-specific resource. This includes
 * connection handles, prepared statements, and result set objects.
 *
 * \version 1.0
 */

/** \file DBCAPI.h
 * Main API header file.
 * This file describes all the data types and entry points of the API.
 */

/** Version 1 was the initial version of the C/C++ API.
 *
 * You must define _DBCAPI_VERSION as 1 or higher for this functionality.
 */
#define DBCAPI_API_VERSION_1		1

/** If the command line does not specify which version to build,
 * then build the latest version.
 */
#ifndef _DBCAPI_VERSION
#define _DBCAPI_VERSION			DBCAPI_API_VERSION_1
#endif

/** Returns the minimal error buffer size.
 */
#define DBCAPI_ERROR_SIZE		256

#if defined(__cplusplus)
extern "C" {
#endif

/** A handle to an interface context
 */
typedef struct dbcapi_interface_context	dbcapi_interface_context;

/** A handle to a connection object
 */
typedef struct dbcapi_connection	dbcapi_connection;

/** A handle to a statement object
 */
typedef struct dbcapi_stmt	   	dbcapi_stmt;

/** A portable 32-bit signed value */
typedef signed int   			dbcapi_i32;
/** A portable 32-bit unsigned value */
typedef unsigned int 			dbcapi_u32;
/** A portable boolean value */
typedef dbcapi_i32 			dbcapi_bool;

/** 64-bit signed integer */
typedef signed long long                dbcapi_i64;

/** The run-time calling convention in use (Windows only).
 */
#ifdef _WIN32
    #define _dbcapi_entry_		__stdcall
#endif
#ifndef _dbcapi_entry_
    #define _dbcapi_entry_
#endif

#if defined(_WIN32)
    #define DBCAPI_API __declspec(dllexport)
#elif defined(__linux__) || defined(__APPLE__)
    #define DBCAPI_API __attribute__ ((visibility("default")))
#elif defined(_AIX)
    #define DBCAPI_API
#else
    #error Unknown platform
#endif

/** Callback function type
 */
#define DBCAPI_CALLBACK	    _dbcapi_entry_

/** Parameter type for dbcapi_register_callback function used to specify the address of the callback routine.
 */
typedef void (DBCAPI_CALLBACK *DBCAPI_CALLBACK_PARM)( dbcapi_stmt *	stmt,
						      const char *	warning_message,
						      dbcapi_i32	warning_code,
						      const char *	sql_state,
						      void *		user_data );

/**
* Return code of functions. This is not an error code,
* it only indicates the status of the function call.
*/
typedef enum dbcapi_retcode {       // maps SQLDBC_Retcode
    DBCAPI_INVALID_OBJECT = -10909, // Application tries to use an invalid object reference.
    DBCAPI_OK = 0,                  // Function call successful.
    DBCAPI_NOT_OK = 1,              // Function call not successful. Further information can be found in the corresponding error object.
    DBCAPI_DATA_TRUNC = 2,          // Data was truncated during the call.
    DBCAPI_OVERFLOW = 3,            // Signalizes a numeric overflow.
    DBCAPI_SUCCESS_WITH_INFO = 4,   // The method succeeded with warnings.
    DBCAPI_BUFFER_FULL = 5,         // Signalizes that the communication buffer was exceeds.
    DBCAPI_NO_DATA_FOUND = 100,     // Data was not found.
    DBCAPI_NEED_DATA = 99           // Late binding, data is needed for execution.
} dbcapi_retcode;

/** Specifies the data type being passed in or retrieved.
 */
typedef enum dbcapi_data_type
{
    /// Invalid data type.
    A_INVALID_TYPE,
    /// Binary data.  Binary data is treated as-is and no character set conversion is performed.
    A_BINARY,
    /// String data.  The data where character set conversion is performed.
    A_STRING,
    /// Double data.  Includes float values.
    A_DOUBLE,
    /// 64-bit integer.
    A_VAL64,
    /// 64-bit unsigned integer.
    A_UVAL64,
    /// 32-bit integer.
    A_VAL32,
    /// 32-bit unsigned integer.
    A_UVAL32,
    /// 16-bit integer.
    A_VAL16,
    /// 16-bit unsigned integer.
    A_UVAL16,
    /// 8-bit integer.
    A_VAL8,
    /// 8-bit unsigned integer.
    A_UVAL8,
    //// Float precision data.
    A_FLOAT
} dbcapi_data_type;

/** Returns a description of the attributes of a data value.
 */
typedef struct dbcapi_data_value
{
    /// A pointer to user supplied buffer of data.
    char * 		buffer;
    /// The size of the buffer.
    size_t		buffer_size;
    /// A pointer to the number of valid bytes in the buffer.  This value must be less than buffer_size.
    size_t *		length;
    /// The type of the data
    dbcapi_data_type	type;
    /// A pointer to indicate whether the last fetched data is NULL.
    dbcapi_bool *	is_null;
    /// Indicates whether the buffer value is an pointer to the actual value.
    dbcapi_bool		is_address;
} dbcapi_data_value;

/** A data direction enumeration.
 */
typedef enum dbcapi_data_direction
{
    /// Invalid data direction.
    DD_INVALID		= 0x0,
    /// Input-only host variables.
    DD_INPUT		= 0x1,
    /// Output-only host variables.
    DD_OUTPUT		= 0x2,
    /// Input and output host variables.
    DD_INPUT_OUTPUT	= 0x3
} dbcapi_data_direction;

/** A bind parameter structure used to bind parameter and prepared statements.
 */
typedef struct dbcapi_bind_data
{
    /// The direction of the data. (input, output, input_output)
    dbcapi_data_direction	direction;
    /// The actual value of the data.
    dbcapi_data_value		value;
    /// Name of the bind parameter. This is only used by dbcapi_describe_bind_param().
    char *			name;
} dbcapi_bind_data;

/** An enumeration of the native types of values as described by the server.
 */
#if !defined( SQLA )
typedef enum dbcapi_native_type
{
    DT_NULL = 0x00,
    DT_TINYINT = 0x01,
    DT_SMALLINT = 0x02,
    DT_INT = 0x03,
    DT_BIGINT = 0x04,
    DT_DECIMAL = 0x05,
    DT_REAL = 0x06,
    DT_DOUBLE = 0x07,
    DT_CHAR = 0x08,
    DT_VARCHAR1 = 0x09,
    DT_NCHAR = 0x0A,
    DT_NVARCHAR = 0x0B,
    DT_BINARY = 0x0C,
    DT_VARBINARY = 0x0D,
    DT_DATE = 0x0E,
    DT_TIME = 0x0F,
    DT_TIMESTAMP = 0x10,
    DT_TIME_TZ = 0x11,
    DT_TIME_LTZ = 0x12,
    DT_TIMESTAMP_TZ = 0x13,
    DT_TIMESTAMP_LTZ = 0x14,
    DT_INTERVAL_YM = 0x15,
    DT_INTERVAL_DS = 0x16,
    DT_ROWID = 0x17,
    DT_UROWID = 0x18,
    DT_CLOB = 0x19,
    DT_NCLOB = 0x1A,
    DT_BLOB = 0x1B,
    DT_BOOLEAN = 0x1C,
    DT_STRING = 0x1D,
    DT_NSTRING = 0x1E,
    DT_LOCATOR = 0x1F,
    DT_NLOCATOR = 0x20,
    DT_BSTRING = 0x21,
    DT_DECIMAL_DIGIT_ARRAY = 0x22,
    DT_VARCHAR2 = 0x23,
    DT_TABLE = 0x2D,
    DT_ABAPSTREAM = 0x30,
    DT_ABAPSTRUCT = 0x31,
    DT_ARRAY = 0x32,
    DT_TEXT = 0x33,
    DT_SHORTTEXT = 0x34,
    DT_BINTEXT = 0x35,
    DT_ALPHANUM = 0x37,
    DT_LONGDATE = 0x3D,
    DT_SECONDDATE = 0x3E,
    DT_DAYDATE = 0x3F,
    DT_SECONDTIME = 0x40,
    DT_CLOCATOR = 0x46,
    DT_BLOB_DISK_RESERVED = 0x47,
    DT_CLOB_DISK_RESERVED = 0x48,
    DT_NCLOB_DISK_RESERVE = 0x49,
    DT_ST_GEOMETRY = 0x4A,
    DT_ST_POINT = 0x4B,
    DT_FIXED16 = 0x4C,
    DT_ABAP_ITAB = 0x4D,
    DT_RECORD_ROW_STORE = 0x4E,
    DT_RECORD_COLUMN_STORE = 0x4F,
    DT_NOTYPE = 0xFF
} dbcapi_native_type;
#else
typedef enum dbcapi_native_type
{
    DT_NULL        =   0,
    DT_DATE        = 384,
    DT_TIME        = 388,
    DT_TIMESTAMP   = 392,
    DT_VARCHAR1    = 448,
    DT_CHAR        = 452,
    DT_CLOB        = 456,
    DT_VARCHAR2    = 460,
    DT_DOUBLE      = 480,
    DT_REAL        = 482,
    DT_DECIMAL     = 484,
    DT_INT         = 496,
    DT_SMALLINT    = 500,
    DT_VARBINARY   = 524,
    DT_BLOB        = 528,
    DT_TINYINT     = 604,
    DT_BIGINT      = 608,
    DT_UNSINT      = 612,
    DT_UNSSMALLINT = 616,
    DT_UNSBIGINT   = 620,
    DT_BOOLEAN     = 624,
    DT_NSTRING     = 628,
    DT_NCHAR       = 632,
    DT_NVARCHAR    = 636,
    DT_NCLOB       = 640
} dbcapi_native_type;
#endif

/** Returns column metadata information.
 */
typedef struct dbcapi_column_info
{
    /// The alias or the name of the column (null-terminated).
    /// The string can be referenced as long as the result set object is not freed.
    char *			name;
#if !defined( SQLA )
    /// The name of the column (null-terminated).
    char *			column_name;
#endif
    /// The column data type.
    dbcapi_data_type		type;
    /// The native type of the column in the database.
    dbcapi_native_type		native_type;
    /// The precision.
    unsigned short		precision;
    /// The scale.
    unsigned short		scale;
    /// The maximum size a data value in this column can take.
    size_t 			max_size;
    /// Indicates whether a value in the column can be null.
    dbcapi_bool			nullable;
    /// The name of the table (null-terminated).
    /// The string can be referenced as long as the result set object is not freed.
    char *			table_name;
    /// The name of the owner (null-terminated).
    /// The string can be referenced as long as the result set object is not freed.
    char *			owner_name;
    /// Indicates whether the column is bound to a user buffer.
    dbcapi_bool			is_bound;
    /// Information about the bound column.
    dbcapi_data_value		binding;
#if defined( SQLA )
    /// The name of the column (null-terminated).
    char *			column_name;
#endif
} dbcapi_column_info;

/** Gets information about the currently bound parameters.
 */
typedef struct dbcapi_bind_param_info
{
    /// A pointer to the name of the parameter.
    char *		    	name;
    /// The direction of the parameter.
    dbcapi_data_direction 	direction;
    /// Information about the bound input value.
    dbcapi_data_value		input_value;
    /// Information about the bound output value.
    dbcapi_data_value		output_value;
    /// The native type of the column in the database.
    dbcapi_native_type		native_type;
    /// The precision.
    unsigned short		precision;
    /// The scale.
    unsigned short		scale;
    /// The maximum size a data value in this column can take.
    size_t 			max_size;
} dbcapi_bind_param_info;

/** Returns metadata information about a column value in a result set.
 */
typedef struct dbcapi_data_info
{
    /// The type of the data in the column.
    dbcapi_data_type		type;
    /// Indicates whether the last fetched data is NULL.
    /// This field is only valid after a successful fetch operation.
    dbcapi_bool			is_null;
    /// The total number of bytes available to be fetched.
    /// This field is only valid after a successful fetch operation.
    size_t	 		data_size;
} dbcapi_data_info;

/** Initializes the interface.
 */
DBCAPI_API dbcapi_bool dbcapi_init( const char * app_name, dbcapi_u32 api_version, dbcapi_u32 * version_available );

/** Initializes the interface using a context.
 *
 * \param app_name A string that names the API used, for example "PHP", "PERL", or "RUBY".
 * \param api_version The current API version that the application is using.
 * This should be _DBCAPI_VERSION defined above
 * \param version_available An optional argument to return the maximum API version that is supported.
 * \return a context object on success and NULL on failure.
 * \sa dbcapi_fini_ex()
 */
DBCAPI_API dbcapi_interface_context * dbcapi_init_ex( const char * app_name, dbcapi_u32 api_version, dbcapi_u32 * version_available );

/** Finalizes the interface.
 *
 * Frees any resources allocated by the API.
 *
 * \sa dbcapi_init()
 */
DBCAPI_API void dbcapi_fini();

/** Finalize the interface that was created using the specified context.
 * Frees any resources allocated by the API.
 * \param context A context object that was returned from dbcapi_init_ex()
 * \sa dbcapi_init_ex()
 */
DBCAPI_API void dbcapi_fini_ex( dbcapi_interface_context *context );

/** Creates a connection object.
 *
 * You must create an API connection object before establishing a database connection. Errors can be retrieved
 * from the connection object. Only one request can be processed on a connection at a time. In addition,
 * not more than one thread is allowed to access a connection object at a time. Undefined behavior or a failure
 * occurs when multiple threads attempt to access a connection object simultaneously.
 *
 * \return A connection object
 * \sa dbcapi_connect(), dbcapi_disconnect()
 */
DBCAPI_API dbcapi_connection * dbcapi_new_connection( void );

/** Creates a connection object using a context.
 * An API connection object needs to be created before a database connection is established. Errors can be retrieved
 * from the connection object. Only one request can be processed on a connection at a time. In addition,
 * not more than one thread is allowed to access a connection object at a time. If multiple threads attempt
 * to access a connection object simultaneously, then undefined behavior/crashes will occur.
 * \param context A context object that was returned from dbcapi_init_ex()
 * \return A connection object
 * \sa dbcapi_connect(), dbcapi_disconnect(), dbcapi_init_ex()
 */
DBCAPI_API dbcapi_connection * dbcapi_new_connection_ex( dbcapi_interface_context *context );

/** Frees the resources associated with a connection object.
 *
 * \param dbcapi_conn A connection object created with dbcapi_new_connection().
 * \sa dbcapi_new_connection()
 */
DBCAPI_API void dbcapi_free_connection( dbcapi_connection *dbcapi_conn );

/** Creates a connection object with an extra parameter, arg.
 *
 * \param arg A void * pointer that should be set to NULL for now.
 * \return A connection object.
 * \sa dbcapi_new_connection(), dbcapi_execute(), dbcapi_execute_direct(), dbcapi_execute_immediate(), dbcapi_prepare()
 */
DBCAPI_API dbcapi_connection * dbcapi_make_connection( void * arg );

/** Creates a connection object based on a supplied context.
 * \param context A valid context object that was created by dbcapi_init_ex()
 * \param arg A void * pointer that should be set to NULL for now.
 * \return A connection object.
 * \sa dbcapi_init_ex(), dbcapi_execute(), dbcapi_execute_direct(), dbcapi_execute_immediate(), dbcapi_prepare()
 */
DBCAPI_API dbcapi_connection * dbcapi_make_connection_ex( dbcapi_interface_context *context, void * arg );

/** Creates a connection to a HANA database server using the supplied connection object and connection string.
 *
 * The supplied connection object must first be allocated using dbcapi_new_connection().
 *
 * The following example demonstrates how to retrieve the error code of a failed connection attempt:
 *
 * <pre>
 * dbcapi_connection * dbcapi_conn;
 * dbcapi_conn = dbcapi_new_connection();
 * if( !dbcapi_connect( dbcapi_conn, "server=host1:30015;uid=system;pwd=passwd" ) ) {
 *     char reason[DBCAPI_ERROR_SIZE];
 *     dbcapi_i32 code;
 *     code = dbcapi_error( dbcapi_conn, reason, sizeof(reason) );
 *     printf( "Connection failed. Code: %d Reason: %s\n", code, reason );
 * } else {
 *     printf( "Connected successfully!\n" );
 *     dbcapi_disconnect( dbcapi_conn );
 * }
 * dbcapi_free_connection( dbcapi_conn );
 * </pre>
 *
 * \param dbcapi_conn A connection object created by dbcapi_new_connection().
 * \param str A HANA connection string.
 * \return 1 if the connection is established successfully or 0 when the connection fails. Use dbcapi_error() to
 * retrieve the error code and message.
 * \sa dbcapi_new_connection(), dbcapi_error()
 */
DBCAPI_API dbcapi_bool dbcapi_connect( dbcapi_connection * dbcapi_conn, const char * str );

/** Creates a connection to a HANA database server using the saved connect properties.
 *
 * \param dbcapi_conn A connection object created by dbcapi_new_connection().
 * \return 1 when successful or 0 when unsuccessful.
 * \sa dbcapi_new_connection(), dbcapi_error()
 */
DBCAPI_API dbcapi_bool dbcapi_connect2( dbcapi_connection * dbcapi_conn );

/** Disconnects an already established HANA connection.
 *
 * All uncommitted transactions are rolled back.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \return 1 when successful or 0 when unsuccessful.
 * \sa dbcapi_connect(), dbcapi_new_connection()
 */
DBCAPI_API dbcapi_bool dbcapi_disconnect( dbcapi_connection * dbcapi_conn );

/** Enable trace buffering
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param options Trace configuration options.
 * \return 1 when successful or 0 when unsuccessful.
 */
DBCAPI_API dbcapi_bool dbcapi_enable_trace_buffering( dbcapi_connection * dbcapi_conn, const char * options );

/** Read trace buffer
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param dst Destination buffer to receive trace messages.
 * \param len Size of the destrination buffer.
 * \param completeLines Whether to read only complete trace lines.
 * \return Number of bytes written to the buffer including the null-terminator.
 */
DBCAPI_API size_t dbcapi_read_buffered_trace( dbcapi_connection * dbcapi_conn, char * dst, size_t len, bool completeLines );

/** Set connect property in memory.
 *
 * This function can be called to save the connect property, name and value pair.
 * Such saved information will be used to open the connection.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param property The property to be saved.
 * \param value The value of the property.
 * \return 1 when successful or 0 when unsuccessful.
 */
DBCAPI_API dbcapi_bool dbcapi_set_connect_property( dbcapi_connection * dbcapi_conn, const char * property, const char * value );

/** Set client property in memory.
 *
 * This function can be used to save the client information, name and value pair.  Such saved
 * information can be retrieved any time as long as dbcapi_conn is still valid.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param property The property to be saved.
 * \param value The value of the property.
 * \return 1 when successful or 0 when unsuccessful.
 */
DBCAPI_API dbcapi_bool dbcapi_set_clientinfo( dbcapi_connection * dbcapi_conn, const char * property, const char * value );

/** Get a 7-bit ASCII client property from memory.
 *
 * This function can be used to retrieve the client information, name and value pair.
 * The key name is in ASCII (7-bit) encoding, the value returned is encoded in ASCII (7-bit)
 * The information must be saved through dbcapi_set_clientinfo before calling this function.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param property The property to be retrieved.
 * \return the property setting when successful or NULL when unsuccessful.
 */
DBCAPI_API const char * dbcapi_get_clientinfo( dbcapi_connection * dbcapi_conn, const char * property );
DBCAPI_API dbcapi_bool  dbcapi_get_clientinfo_utf8( dbcapi_connection * conn, const char * property, char * buf, size_t buflen );

/** Cancel an outstanding request on a connection.
 * This function can be used to cancel an outstanding request on a specific connection.
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 */
DBCAPI_API void dbcapi_cancel( dbcapi_connection * dbcapi_conn );

/** Executes the supplied SQL statement immediately without returning a result set.
 *
 * This function is useful for SQL statements that do not return a result set.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param sql A string representing the SQL statement to be executed.
 * \return 1 on success or 0 on failure.
 */
DBCAPI_API dbcapi_bool dbcapi_execute_immediate( dbcapi_connection * dbcapi_conn, const char * sql );

/** Prepares a supplied SQL string.
 *
 * Execution does not happen until dbcapi_execute() is
 * called. The returned statement object should be freed using dbcapi_free_stmt().
 *
 * The following statement demonstrates how to prepare a SELECT SQL string:
 *
 * <pre>
 * char * str;
 * dbcapi_stmt * stmt;
 *
 * str = "select * from employees where salary >= ?";
 * stmt = dbcapi_prepare( dbcapi_conn, str );
 * if( stmt == NULL ) {
 *     // Failed to prepare statement, call dbcapi_error() for more info
 * }
 * </pre>
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param sql_str The SQL statement to be prepared.
 * \return A handle to a HANA statement object. The statement object can be used by dbcapi_execute()
 * to execute the statement.
 * \sa dbcapi_free_stmt(), dbcapi_connect(), dbcapi_execute(), dbcapi_num_params(), dbcapi_describe_bind_param(), dbcapi_bind_param()
 */
DBCAPI_API dbcapi_stmt * dbcapi_prepare( dbcapi_connection * dbcapi_conn, const char * sql_str );

/** Aborts the running database request that being executed on the connection.
 * There are no guarantees about the fate of the connection or the outstanding
 * request after calling this function.
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 */
DBCAPI_API void dbcapi_abort( dbcapi_connection *dbcapi_conn );

/** Returns the function code of a prepared statement.
*
* \param dbcapi_stmt A statement object returned by the successful execution of dbcapi_prepare().
* \return The function code of the statement, or -1 if the statement object is not valid.
* \sa dbcapi_get_function_code()
*/
DBCAPI_API dbcapi_i32 dbcapi_get_function_code( dbcapi_stmt * dbcapi_stmt );

/** Frees resources associated with a prepared statement object.
 *
 * \param dbcapi_stmt A statement object returned by the successful execution of dbcapi_prepare() or dbcapi_execute_direct().
 * \sa dbcapi_prepare(), dbcapi_execute_direct()
 */
DBCAPI_API void dbcapi_free_stmt( dbcapi_stmt * dbcapi_stmt );

/** Returns the number of parameters expected for a prepared statement.
 *
 * \param dbcapi_stmt A statement object returned by the successful execution of dbcapi_prepare().
 * \return The expected number of parameters, or -1 if the statement object is not valid.
 * \sa dbcapi_prepare()
 */
DBCAPI_API dbcapi_i32 dbcapi_num_params( dbcapi_stmt * dbcapi_stmt );

/** Describes the bind parameters of a prepared statement.
 *
 * This function allows the caller to determine information about prepared statement parameters.  The type of prepared
 * statement, stored procedured or a DML, determines the amount of information provided.  The direction of the parameters
 * (input, output, or input-output) are always provided.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the parameter. This number must be between 0 and dbcapi_num_params() - 1.
 * \param param An dbcapi_bind_data structure that is populated with information.
 * \return 1 when successful or 0 when unsuccessful.
 * \sa dbcapi_bind_param(), dbcapi_prepare()
 */
DBCAPI_API dbcapi_bool dbcapi_describe_bind_param( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_data * param );

/** Bind a user-supplied buffer as a parameter to the prepared statement.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the parameter. This number must be between 0 and dbcapi_num_params() - 1.
 * \param param An dbcapi_bind_data structure description of the parameter to be bound.
 * \return 1 on success or 0 on unsuccessful.
 * \sa dbcapi_describe_bind_param()
 */
DBCAPI_API dbcapi_bool dbcapi_bind_param( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_data * param );

/** Sends data as part of a bound parameter.
 *
 * This method can be used to send a large amount of data for a bound parameter in chunks.
 * This method can be used only when the batch size is 1.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the parameter. This should be a number between 0 and dbcapi_num_params() - 1.
 * \param buffer The data to be sent.
 * \param size The number of bytes to send.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_prepare()
 */
DBCAPI_API dbcapi_bool dbcapi_send_param_data( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, char * buffer, size_t size );

/** Retrieves LOBs from stored procedure calls
 *
 * When executing a stored procedure with LOB output parameters, the app would not know the length of the LOBs
 * before execution.  Therefore, it does not know how much memory it need to allocate for the LOB output parameter.
 * The function will provide a way for the app to retrieve a LOB output parameter chunk by chunk.
 *
 * \param dbcapi_stmt A statement object executed by dbcapi_execute().
 * \param param_index The number of the parameter to be retrieved.  The parameter must be a LOB output parameter.
 *      The parameter number is between 0 and dbcapi_num_params() - 1.
 * \param offset The starting offset of the data to get.
 * \param buffer A buffer to be filled with the contents of the output parameter. The buffer pointer must be aligned correctly
 * for the data type copied into it.
 * \param size The size of the buffer in bytes. The function fails
 * if you specify a size greater than 2^31 - 1.
 * \return The number of bytes successfully copied into the supplied buffer.
 * This number must not exceed 2^31 - 1.
 * 0 indicates that no data remains to be copied.  -1 indicates a failure.
 * \sa dbcapi_execute()
 */
DBCAPI_API dbcapi_i32 dbcapi_get_param_data( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 param_index, size_t offset, void * buffer, size_t size );

/** This function is currently unused.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the parameter. This should be a number between 0 and dbcapi_num_params() - 1.
 * \return 1 on success or 0 on failure
 * \sa dbcapi_prepare(), dbcapi_send_param_data()
 */
DBCAPI_API dbcapi_bool dbcapi_reset_param_data( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index );

/** Finish the parameter data.
 *
 * This method can be used to complete sending a large amount of data for a bound LOB parameter.
 * For LOB parameters, the data length may be unknown when the statement is executed, then this
 * function will be useful.  We first bind the parameters with the data length setting to 2147483647,
 * then send the parameter data in chunks using dbcapi_send_param_data.  After we have sent the
 * last chunk of data, then we call this function to finish the parameter.  Please note, we still
 * need to call dbcapi_send_param_data and dbcapi_finish_param_data once, even the length of the
 * LOB parameter is zero, when the LOB length is unknown at execution.
 * This method can be used only when the batch size is 1.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the parameter. This should be a number between 0 and dbcapi_num_params() - 1.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_prepare(), dbcapi_send_param_data()
 */
DBCAPI_API dbcapi_bool dbcapi_finish_param_data( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index );

/** Retrieves the length of the last error message stored in the connection object
 *  including the NULL terminator.  If there is no error, 0 is returned.
 *
 * \param dbcapi_conn A connection object returned from dbcapi_new_connection().
 * \return The length of the last error message including the NULL terminator.
 */
DBCAPI_API size_t dbcapi_error_length( dbcapi_connection * dbcapi_conn );

/** Sets the size of the row array for a batch execute.
 *
 * The batch size is used only for an INSERT statement. The default batch size is 1.
 * A value greater than 1 indicates a wide insert.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param num_rows The number of rows for batch execution. The value must be 1 or greater.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_bind_param(), dbcapi_get_batch_size()
 */
DBCAPI_API dbcapi_bool dbcapi_set_batch_size( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 num_rows );

/** Sets the bind type of parameters.
 *
 * The default value is 0, which indicates column-wise binding. A non-zero value indicates
 * row-wise binding and specifies the byte size of the data structure that stores the row.
 * The parameter is bound to the first element in a contiguous array of values. The address
 * offset to the next element is computed based on the bind type:
 *
 * <ul>
 * <li>Column-wise binding - the byte size of the parameter type</li>
 * <li>Row-wise binding - the row_size</li>
 * </ul>
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param row_size The byte size of the row. A value of 0 indicates column-wise binding and a positive value indicates row-wise binding.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_bind_param()
 */
DBCAPI_API dbcapi_bool dbcapi_set_param_bind_type( dbcapi_stmt * dbcapi_stmt, size_t row_size );

/** Retrieves the size of the row array for a batch execute.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \return The size of the row array.
 * \sa dbcapi_set_batch_size()
 */
DBCAPI_API dbcapi_u32 dbcapi_get_batch_size( dbcapi_stmt * dbcapi_stmt );

/** Sets the size of the row set to be fetched by the dbcapi_fetch_absolute() and dbcapi_fetch_next() functions.
 *
 * The default size of the row set is 1. Specifying num_rows to be a value greater than 1 indicates a wide fetch.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param num_rows The size of the row set. The value must be 1 or greater.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_bind_column(), dbcapi_fetch_absolute(), dbcapi_fetch_next(), dbcapi_get_rowset_size()
 */
DBCAPI_API dbcapi_bool dbcapi_set_rowset_size( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 num_rows );

/** Retrieves the size of the row set to be fetched by the dbcapi_fetch_absolute() and dbcapi_fetch_next() functions.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \return The size of the row set, or 0 if the statement does not return a result set.
 * \sa dbcapi_set_rowset_size(), dbcapi_fetch_absolute(), dbcapi_fetch_next()
 */
DBCAPI_API dbcapi_u32 dbcapi_get_rowset_size( dbcapi_stmt * dbcapi_stmt );

/** Sets the bind type of columns.
 *
 * The default value is 0, which indicates column-wise binding. A non-zero value indicates
 * row-wise binding and specifies the byte size of the data structure that stores the row.
 * The column is bound to the first element in a contiguous array of values. The address
 * offset to the next element is computed based on the bind type:
 *
 * <ul>
 * <li>Column-wise binding - the byte size of the column type</li>
 * <li>Row-wise binding - the row_size</li>
 * </ul>
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param row_size The byte size of the row. A value of 0 indicates column-wise binding and a positive value indicates row-wise binding.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_bind_column()
 */
DBCAPI_API dbcapi_bool dbcapi_set_column_bind_type( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 row_size );

/** Binds a user-supplied buffer as a result set column to the prepared statement.
 *
 *  If the size of the fetched row set is greater than 1, the buffer must be large enough to
 *  hold the data of all of the rows in the row set. This function can also be used to clear the
 *  binding of a column by specifying value to be NULL.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the column. This number must be between 0 and dbcapi_num_cols() - 1.
 * \param value An dbcapi_data_value structure describing the bound buffers, or NULL to clear previous binding information.
 * \return 1 on success or 0 on unsuccessful.
 * \sa dbcapi_clear_column_bindings(), dbcapi_set_rowset_size()
 */
DBCAPI_API dbcapi_bool dbcapi_bind_column( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_data_value * value );

/** Removes all column bindings defined using dbcapi_bind_column().
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_bind_column()
 */
DBCAPI_API dbcapi_bool dbcapi_clear_column_bindings( dbcapi_stmt * dbcapi_stmt );

/** Returns the number of rows fetched.
 *
 * In general, the number of rows fetched is equal to the size specified by the dbcapi_set_rowset_size() function. The
 * exception is when there are fewer rows from the fetch position to the end of the result set than specified, in which
 * case the number of rows fetched is smaller than the specified row set size. The function returns -1 if the last fetch
 * was unsuccessful or if the statement has not been executed. The function returns 0 if the statement has been executed
 * but no fetching has been done.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \return The number of rows fetched or -1 on failure.
 * \sa dbcapi_bind_column(), dbcapi_fetch_next(), dbcapi_fetch_absolute()
 */
DBCAPI_API dbcapi_i32 dbcapi_fetched_rows( dbcapi_stmt * dbcapi_stmt );

/** Sets the current row in the fetched row set.
 *
 * When a dbcapi_fetch_absolute() or dbcapi_fetch_next() function is executed, a row set
 * is created and the current row is set to be the first row in the row set. The functions
 * dbcapi_get_column(), dbcapi_get_data(), dbcapi_get_data_info() are used to retrieve data
 * at the current row.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param row_num The row number within the row set. The valid values are from 0 to dbcapi_fetched_rows() - 1.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_set_rowset_size(), dbcapi_get_column(), dbcapi_get_data(), dbcapi_get_data_info(), dbcapi_fetch_absolute(), dbcapi_fetch_next()
 */
DBCAPI_API dbcapi_bool dbcapi_set_rowset_pos( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 row_num );

/** Resets a statement to its prepared state condition.
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \return 1 on success, 0 on failure.
 * \sa dbcapi_prepare()
 */
DBCAPI_API dbcapi_bool dbcapi_reset( dbcapi_stmt * dbcapi_stmt );

/** Retrieves information about the parameters that were bound using dbcapi_bind_param().
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \param index The index of the parameter. This number should be between 0 and dbcapi_num_params() - 1.
 * \param info A dbcapi_bind_param_info buffer to be populated with the bound parameter's information.
 * \return 1 on success or 0 on failure.
 * \sa dbcapi_bind_param(), dbcapi_describe_bind_param(), dbcapi_prepare()
 */
DBCAPI_API dbcapi_bool dbcapi_get_bind_param_info( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_param_info * info );

/** Executes a prepared statement.
 *
 * You can use dbcapi_num_cols() to verify if the executed statement returned a result set.
 *
 * The following example shows how to execute a statement that does not return a result set:
 *
 * <pre>
 * dbcapi_stmt *	stmt;
 * int			i;
 * dbcapi_bind_data	param;
 *
 * stmt = dbcapi_prepare( dbcapi_conn, "insert into test(id,value) values( ?,? )" );
 * if( stmt != NULL ) {
 *     dbcapi_describe_bind_param( stmt, 0, &param );
 *     param.value.buffer = (char *)&i;
 *     param.value.type   = A_VAL32;
 *     dbcapi_bind_param( stmt, 0, &param );
 *
 *     dbcapi_describe_bind_param( stmt, 1, &param );
 *     param.value.buffer = (char *)&i;
 *     param.value.type   = A_VAL32;
 *     dbcapi_bind_param( stmt, 1, &param );
 *
 *     for( i = 0; i < 10; i++ ) {
 *         if( !dbcapi_execute( stmt ) ) {
 *	       // call dbcapi_error()
 *	   }
 *     }
 *     dbcapi_free_stmt( stmt );
 * }
 * </pre>
 *
 * \param dbcapi_stmt A statement prepared successfully using dbcapi_prepare().
 * \return 1 if the statement is executed successfully or 0 on failure.
 * \sa dbcapi_prepare()
 */
DBCAPI_API dbcapi_bool dbcapi_execute( dbcapi_stmt * dbcapi_stmt );

/** Executes the SQL statement specified by the string argument and possibly returns a result set.
 *
 * Use this method to prepare and execute a statement,
 * or instead of calling dbcapi_prepare() followed by dbcapi_execute().
 *
 * The following example shows how to execute a statement that returns a result set:
 *
 * <pre>
 * dbcapi_stmt *   stmt;
 *
 * stmt = dbcapi_execute_direct( dbcapi_conn, "select * from employees" );
 * if( stmt && dbcapi_num_cols( stmt ) > 0 ) {
 *     while( dbcapi_fetch_next( stmt ) ) {
 *         int i;
 *	   for( i = 0; i < dbcapi_num_cols( stmt ); i++ ) {
 *             // Get column i data
 *         }
 *     }
 *     dbcapi_free_stmt( stmt  );
 * }
 * </pre>
 *
 * <em>Note:</em> This function cannot be used for executing a SQL statement with parameters.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param sql_str A SQL string. The SQL string should not have parameters such as ?.
 * \return A statement handle if the function executes successfully, NULL when the function executes unsuccessfully.
 * \sa dbcapi_fetch_absolute(), dbcapi_fetch_next(), dbcapi_num_cols(), dbcapi_get_column()
 */
DBCAPI_API dbcapi_stmt * dbcapi_execute_direct( dbcapi_connection * dbcapi_conn, const char * sql_str );

/** Moves the current row in the result set to the specified row number and then fetches
 *  rows of data starting from the current row.
 *
 *  The number of rows fetched is set using the dbcapi_set_rowset_size() function. By default, one row is returned.
 *
 * \param dbcapi_stmt A statement object that was executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \param row_num The row number to be fetched. The first row is 1, the last row is -1.
 * \return 1 if the fetch was successfully, 0 when the fetch is unsuccessful.
 * \sa dbcapi_execute_direct(), dbcapi_execute(), dbcapi_error(), dbcapi_fetch_next(), dbcapi_set_rowset_size()
 */
DBCAPI_API dbcapi_bool dbcapi_fetch_absolute( dbcapi_stmt * dbcapi_stmt, dbcapi_i32 row_num );

/** Returns the next set of rows from the result set.
 *
 * When the result object is first created, the current row
 * pointer is set to before the first row, that is, row 0.
 * This function first advances the row pointer to the next
 * unfetched row and then fetches rows of data starting from
 * that row. The number of rows fetched is set by the
 * dbcapi_set_rowset_size() function. By default, one row is returned.
 *
 * \param dbcapi_stmt A statement object that was executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \return 1 if the fetch was successfully, 0 when the fetch is unsuccessful.
 * \sa dbcapi_fetch_absolute(), dbcapi_execute_direct(), dbcapi_execute(), dbcapi_error(), dbcapi_set_rowset_size()
 */
DBCAPI_API dbcapi_bool dbcapi_fetch_next( dbcapi_stmt * dbcapi_stmt );

/** Advances to the next result set in a multiple result set query.
 *
 * If a query (such as a call to a stored procedure) returns multiple result sets, then this function
 * advances from the current result set to the next.
 *
 * The following example demonstrates how to advance to the next result set in a multiple result set query:
 *
 * <pre>
 * stmt = dbcapi_execute_direct( dbcapi_conn, "call my_multiple_results_procedure()" );
 * if( stmt != NULL ) {
 *     do {
 *         while( dbcapi_fetch_next( stmt ) ) {
 *            // get column data
 *         }
 *     } while( dbcapi_get_next_result( stmt ) );
 *     dbcapi_free_stmt( stmt );
 * }
 * </pre>
 *
 * \param dbcapi_stmt A statement object executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \return 1 if the statement successfully advances to the next result set, 0 otherwise.
 * \sa dbcapi_execute_direct(), dbcapi_execute()
 */
DBCAPI_API dbcapi_bool dbcapi_get_next_result( dbcapi_stmt * dbcapi_stmt );

/** Checks if the statement has next result.
 * \param dbcapi_stmt A statement object executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \return 1 if the statement has next result set, 0 otherwise.
 */
DBCAPI_API dbcapi_bool dbcapi_has_next_result( dbcapi_stmt * dbcapi_stmt );

/** Returns the number of rows affected by execution of the prepared statement.
 *
 * \param dbcapi_stmt A statement that was prepared and executed successfully with no result set returned.
 *                    For example, an INSERT, UPDATE or DELETE statement was executed.
 * \return The number of rows affected or -1 on failure.
 * \sa dbcapi_execute(), dbcapi_execute_direct()
 */
DBCAPI_API dbcapi_i32 dbcapi_affected_rows( dbcapi_stmt * dbcapi_stmt );

/** Returns number of columns in the result set.
 *
 * \param dbcapi_stmt A statement object created by dbcapi_prepare() or dbcapi_execute_direct().
 * \return The number of columns in the result set or -1 on a failure.
 * \sa dbcapi_execute(), dbcapi_execute_direct(), dbcapi_prepare()
 */
DBCAPI_API dbcapi_i32 dbcapi_num_cols( dbcapi_stmt * dbcapi_stmt );

/** Returns the approximate number of rows in the result set.
 *
 * \param dbcapi_stmt A statement object that was executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \return The approximate number of rows in the result set.
 * \sa dbcapi_execute_direct(), dbcapi_execute()
 */
DBCAPI_API dbcapi_i32 dbcapi_num_rows( dbcapi_stmt * dbcapi_stmt );

/** Fills the supplied buffer with the value fetched for the specified column at the current row.
 *
 * When a dbcapi_fetch_absolute() or dbcapi_fetch_next() function is executed, a row set
 * is created and the current row is set to be the first row in the row set. The current
 * row is set using the dbcapi_set_rowset_pos() function.
 *
 * For A_BINARY and A_STRING data types,
 * value->buffer points to an internal buffer associated with the result set.
 * Do not rely upon or alter the content of the pointer buffer as it changes when a
 * new row is fetched or when the result set object is freed.  Users should copy the
 * data out of those pointers into their own buffers.
 *
 * The value->length field indicates the number of valid characters that
 * value->buffer points to. The data returned in value->buffer is not
 * null-terminated. This function fetches all the returned values from the
 * HANA database server.  For example, if the column contains
 * a blob, this function attempts to allocate enough memory to hold that value.
 * If you do not want to allocate memory, use dbcapi_get_data() instead.
 *
 * \param dbcapi_stmt A statement object executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \param col_index The number of the column to be retrieved.
 *	The column number is between 0 and dbcapi_num_cols() - 1.
 * \param buffer An dbcapi_data_value object to be filled with the data fetched for column col_index at the current row in the row set.
 * \return 1 on success or 0 for failure. A failure can happen if any of the parameters are invalid or if there is
 * not enough memory to retrieve the full value from the HANA database server.
 * \sa dbcapi_execute_direct(), dbcapi_execute(), dbcapi_fetch_absolute(), dbcapi_fetch_next(), dbcapi_set_rowset_pos()
 */
DBCAPI_API dbcapi_bool dbcapi_get_column( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_data_value * buffer );

/** Retrieves the data fetched for the specified column at the current row into the supplied buffer memory.
 *
 * When a dbcapi_fetch_absolute() or dbcapi_fetch_next() function is executed, a row set
 * is created and the current row is set to be the first row in the row set. The current
 * row is set using the dbcapi_set_rowset_pos() function.
 *
 * \param dbcapi_stmt A statement object executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \param col_index The number of the column to be retrieved.
 *	The column number is between 0 and dbcapi_num_cols() - 1.
 * \param offset The starting offset of the data to get.
 * \param buffer A buffer to be filled with the contents of the column at the current row in the row set. The buffer pointer must be aligned correctly
 * for the data type copied into it.
 * \param size The size of the buffer in bytes. The function fails
 * if you specify a size greater than 2^31 - 1.
 * \return The number of bytes successfully copied into the supplied buffer.
 * This number must not exceed 2^31 - 1.
 * 0 indicates that no data remains to be copied.  -1 indicates a failure.
 * \sa dbcapi_execute(), dbcapi_execute_direct(), dbcapi_fetch_absolute(), dbcapi_fetch_next(), dbcapi_set_rowset_pos()
 */
DBCAPI_API dbcapi_i32 dbcapi_get_data( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, size_t offset, void * buffer, size_t size );

/** Retrieves information about the fetched data at the current row.
 *
 * When a dbcapi_fetch_absolute() or dbcapi_fetch_next() function is executed, a row set
 * is created and the current row is set to be the first row in the row set. The current
 * row is set using the dbcapi_set_rowset_pos() function.
 *
 * \param dbcapi_stmt A statement object executed by
 *     dbcapi_execute() or dbcapi_execute_direct().
 * \param col_index The column number between 0 and dbcapi_num_cols() - 1.
 * \param buffer A data info buffer to be filled with the metadata about the data at the current row in the row set.
 * \return 1 on success, and 0 on failure. Failure is returned when any of the supplied parameters are invalid.
 * \sa dbcapi_execute(), dbcapi_execute_direct(), dbcapi_fetch_absolute(), dbcapi_fetch_next(), dbcapi_set_rowset_pos()
 */
DBCAPI_API dbcapi_bool dbcapi_get_data_info( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_data_info * buffer );

/** Retrieves column metadata information and fills the dbcapi_column_info structure with information about the column.
 *
 * \param dbcapi_stmt A statement object created by dbcapi_prepare() or dbcapi_execute_direct().
 * \param col_index The column number between 0 and dbcapi_num_cols() - 1.
 * \param buffer A column info structure to be filled with column information.
 * \return 1 on success or 0 if the column index is out of range,
 * or if the statement does not return a result set.
 * \sa dbcapi_execute(), dbcapi_execute_direct(), dbcapi_prepare()
 */
DBCAPI_API dbcapi_bool dbcapi_get_column_info( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_column_info * buffer );

/** Commits the current transaction.
 *
 * \param dbcapi_conn The connection object on which the commit operation is performed.
 * \return 1 when successful or 0 when unsuccessful.
 * \sa dbcapi_rollback()
 */
DBCAPI_API dbcapi_bool dbcapi_commit( dbcapi_connection * dbcapi_conn );

/** Rolls back the current transaction.
 *
 * \param dbcapi_conn The connection object on which the rollback operation is to be performed.
 * \return 1 on success, 0 otherwise.
 * \sa dbcapi_commit()
 */
DBCAPI_API dbcapi_bool dbcapi_rollback( dbcapi_connection * dbcapi_conn );

/** Returns the current client version.
 *
 * This method fills the buffer passed with the major, minor, patch, and build number of the client library.
 * The buffer will be null-terminated.
 *
 * \param buffer The buffer to be filled with the client version string.
 * \param len The length of the buffer supplied.
 * \return 1 when successful or 0 when unsuccessful.
 */
DBCAPI_API dbcapi_bool dbcapi_client_version( char * buffer, size_t len );

/** Returns the current client version.
 *
 * This method fills the buffer passed with the major, minor, patch, and build number of the client library.
 * The buffer will be null-terminated.
 *
 * \param context The object that was created with dbcapi_init_ex().
 * \param buffer The buffer to be filled with the client version string.
 * \param len The length of the buffer supplied.
 * \return 1 when successful or 0 when unsuccessful.
 * \sa dbcapi_init_ex()
 */
DBCAPI_API dbcapi_bool dbcapi_client_version_ex( dbcapi_interface_context *context, char *buffer, size_t len );

/** Retrieves the last error code and message stored in the connection object.
 *
 * \param dbcapi_conn A connection object returned from dbcapi_new_connection().
 * \param buffer A buffer to be filled with the error message.
 * \param size The size of the supplied buffer.
 * \return The last error code. Positive values are warnings, negative values are errors, and 0 indicates success.
 * \sa dbcapi_connect()
 */
DBCAPI_API dbcapi_i32 dbcapi_error( dbcapi_connection * dbcapi_conn, char * buffer, size_t size );

/** Retrieves the current SQLSTATE.
 *
 * \param dbcapi_conn A connection object returned from dbcapi_new_connection().
 * \param buffer A buffer to be filled with the current 5-character SQLSTATE.
 * \param size The buffer size.
 * \return The number of bytes copied into the buffer.
 * \sa dbcapi_error()
 */
DBCAPI_API size_t dbcapi_sqlstate( dbcapi_connection * dbcapi_conn, char * buffer, size_t size );

/** Clears the last stored error code
 *
 * \param dbcapi_conn A connection object returned from dbcapi_new_connection().
 * \sa dbcapi_new_connection()
 */
DBCAPI_API void dbcapi_clear_error( dbcapi_connection * dbcapi_conn );

/** Sets the AUTOCOMMIT mode to be on or off.
 *
 * The default AUTOCOMMIT mode is off, so calling the dbcapi_commit or dbcapi_rollback API is required
 * for each transaction.
 * When the AUTOCOMMIT mode is set to on, all statements are committed after they execute.   They cannot
 * be rolled back.
 * \param dbcapi_conn The connection object on which the rollback operation is to be performed.
 * \param mode A value of 1 or 0.  The value 1 will enable AUTOCOMMIT and the value 0 will disable AUTOCOMMIT
 * \return 1 on success, 0 otherwise.
 * \sa dbcapi_commit(), dbcapi_rollback()
 */
DBCAPI_API dbcapi_bool dbcapi_set_autocommit( dbcapi_connection * dbcapi_conn, dbcapi_bool mode );

/** Sets the transaction isolation level.
 *
 * The default isolation level is READ_COMMITTED.  This function can be used to change the default
 * isolation level.
 * \param dbcapi_conn The connection object on which the transaction isolation level is to be set.
 * \param isolation_level A value of 1, 2, or 3 that represents READ_COMMITTED, REPEATABLE_READ, or
 * SERIALIZABLE respectively
 * \return 1 on success, 0 otherwise.
 */
DBCAPI_API dbcapi_bool dbcapi_set_transaction_isolation( dbcapi_connection * dbcapi_conn, dbcapi_u32 isolation_level );

/** Sets the query timeout of a statement.
 *
 * The default statement query timeout is zero.  This function can be used to change the default
 * query timeout value.
 * \param dbcapi_stmt A statement object returned by the successful execution of dbcapi_prepare().
 * \param timeout_value A query timeout value in seconds
 * \return 1 on success, 0 otherwise.
 */
DBCAPI_API dbcapi_bool dbcapi_set_query_timeout( dbcapi_stmt * dbcapi_stmt, dbcapi_i32 timeout_value );

/** Register a warning callback routine.
 *
 *  Call the function to register a routine which will be called by DBCAPI with a message whenever a warning has occurred.
 *
 * \param dbcapi_conn A connection object with a connection established using dbcapi_connect().
 * \param callback Address of the callback routine.
 * \param user_data A pointer to user-defined data structure that may be used in the user-defined callback function.
 * \return 1 when successful or 0 when unsuccessful.
 */

DBCAPI_API dbcapi_bool dbcapi_register_warning_callback( dbcapi_connection * dbcapi_conn, DBCAPI_CALLBACK_PARM callback, void * user_data );

/** Retrieve a print line printed during execution of a store procedure.
*
* The initial call after execution of the stored procedure will retrieve
* the first line printed. If the print line is successfully retrieved, it
* is removed from the queue and subsequent calls will retrieve following
* print lines (if available).
*
* \param dbcapi_stmt A statement object.
* \param host_type Type of the output buffer (typically a character type,
*      e.g. SQLDBC_HOSTTYPE_UTF8).
* \param buffer A pointer to the output buffer.
* \param length_indicator Pointer to a variable that will store the
*        length of the output. On sucess, contains the number of bytes
*        copied to the buffer, except the number of bytes necessary
*        for the the zero-terminator, if the terminate flag
*        was set. If the source string exceeds the buffer_size
*        value DBCAPI_DATA_TRUNC will be returned and
*        length_indicator is set to the number of bytes
*        (except the terminator bytes) needed to copy without
*        truncation.
* \param buffer_size The size of the buffer in bytes.
* \param terminate Specifies that the output buffer must be finished
*        with a C-style zero-terminator. The terminate flag
*        works only for the host var type character (ASCII, UCS2 or UTF8).
* \return #DBCAPI_OK if the print line is retrieved.
*         #DBCAPI_DATA_TRUNC if the provided buffer was not large
*         enough. Subsequent calls will continue to try and retreive
*         the same print line, so a larger buffer must be provided.
*         #DBCAPI_NO_DATA_FOUND no more print lines available
*/
DBCAPI_API dbcapi_retcode dbcapi_get_print_line( dbcapi_stmt * dbcapi_stmt, const dbcapi_i32 host_type, void * buffer,
                                                 size_t * length_indicator, size_t buffer_size, const dbcapi_bool terminate );

/** Retrieve the current setting of the AUTOCOMMIT mode.
 *
 * \param dbcapi_conn The connection object returned from dbcapi_new_connection().
 * \param mode An autocommit mode to be retrieved from the driver.
 * \return true on success, false on failure which can occur if dbcapi_conn is an invalid dbcapi connection object.
 */
DBCAPI_API dbcapi_bool dbcapi_get_autocommit( dbcapi_connection * dbcapi_conn, dbcapi_bool * mode );

#if !defined( SQLA )

/** Returns the row status array for the last batch execution.
*
* The row status array describes the state of each row.
* 1 - succeeded, -2 - failed.
*
* \return A pointer to the first element of the row status array.
*/
DBCAPI_API dbcapi_i32* dbcapi_get_row_status( dbcapi_stmt * dbcapi_stmt );

DBCAPI_API dbcapi_i64 dbcapi_get_stmt_server_cpu_time( dbcapi_stmt * dbcapi_stmt );

DBCAPI_API dbcapi_i64 dbcapi_get_stmt_server_memory_usage( dbcapi_stmt * dbcapi_stmt );

DBCAPI_API dbcapi_i64 dbcapi_get_stmt_server_processing_time( dbcapi_stmt * dbcapi_stmt );

DBCAPI_API dbcapi_i64 dbcapi_get_resultset_server_cpu_time( dbcapi_stmt * dbcapi_stmt );

DBCAPI_API dbcapi_i64 dbcapi_get_resultset_server_memory_usage( dbcapi_stmt * dbcapi_stmt );

DBCAPI_API dbcapi_i64 dbcapi_get_resultset_server_processing_time( dbcapi_stmt * dbcapi_stmt );

/** Retrieve the current MD5 Statement Hash of last prepared or executed statement
 *
 * \param dbcapi_stmt The statement object that prepared or executed SQL
 * \param buffer      The buffer to copy the data into
 * \param buffer_size The size of the buffer
 * \return Value of statement hash is copied into buffer, or empty string is returned if no value.
 */

DBCAPI_API dbcapi_retcode dbcapi_get_stmt_statement_hash( dbcapi_stmt * dbcapi_stmt,
                                                          void * buffer, size_t buffer_size );

#endif

/** Sets statement packet size.
 *
 * \param dbcapi_stmt A statement object.
 * \param packet_size A statement packet size.
 */
DBCAPI_API void dbcapi_set_stmt_packet_size( dbcapi_stmt * dbcapi_stmt, dbcapi_i32 packet_size );

/** Returns the packet size of the statement.
 *
 * \param dbcapi_stmt A statement object.
 */
DBCAPI_API dbcapi_i32 dbcapi_get_stmt_packet_size( dbcapi_stmt * dbcapi_stmt );

/**
* The following API is for internal use-only
*/
DBCAPI_API dbcapi_bool dbcapi_register_message_callback( dbcapi_connection * dbcapi_conn, DBCAPI_CALLBACK_PARM callback, void * user_data );

#if defined(__cplusplus)
}
#endif

#endif
