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

#ifndef __DBCAPIDLL_H__
#define __DBCAPIDLL_H__

#include "DBCAPI.h"

/** \file DBCAPI_DLL.h
 * \brief Header file for stub that can dynamically load the main API DLL.
 * The user will need to include DBCAPI_DLL.h in their source files and compile in DBCAPI_DLL.c
 */

#if defined( __cplusplus )
extern "C" {
#endif
typedef dbcapi_bool (*dbcapi_init_func)( const char * app_name, dbcapi_u32 api_version, dbcapi_u32 * max_version );
typedef void (*dbcapi_fini_func)();
typedef dbcapi_connection * (*dbcapi_new_connection_func)( );
typedef void (*dbcapi_free_connection_func)( dbcapi_connection *dbcapi_conn );
typedef dbcapi_connection * (*dbcapi_make_connection_func)( void * arg );
typedef dbcapi_bool (*dbcapi_connect_func)( dbcapi_connection * dbcapi_conn, const char * str );
typedef dbcapi_bool (*dbcapi_connect2_func)( dbcapi_connection * dbcapi_conn );
typedef dbcapi_bool (*dbcapi_disconnect_func)( dbcapi_connection * dbcapi_conn );
typedef dbcapi_bool (*dbcapi_enable_trace_buffering_func)( dbcapi_connection * dbcapi_conn, const char * options );
typedef size_t (*dbcapi_read_buffered_trace_func)( dbcapi_connection * dbcapi_conn, char * dst, size_t len, bool completeLines );
typedef dbcapi_bool (*dbcapi_set_connect_property_func)( dbcapi_connection * dbcapi_conn, const char * property, const char * value );
typedef dbcapi_bool (*dbcapi_set_clientinfo_func)( dbcapi_connection * dbcapi_conn, const char * property, const char * value );
typedef dbcapi_bool (*dbcapi_set_transaction_isolation_func)( dbcapi_connection * dbcapi_conn, dbcapi_u32 isolation_level );
typedef const char * (*dbcapi_get_clientinfo_func)( dbcapi_connection * dbcapi_conn, const char * property );
typedef dbcapi_bool (*dbcapi_get_clientinfo_utf8_func)( dbcapi_connection * conn, const char * property, char * buf, size_t buflen );
typedef dbcapi_bool (*dbcapi_execute_immediate_func)( dbcapi_connection * dbcapi_conn, const char * sql );
typedef dbcapi_stmt * (*dbcapi_prepare_func)( dbcapi_connection * dbcapi_conn, const char * sql_str );
typedef void (*dbcapi_abort_func)( dbcapi_connection *dbcapi_conn );
typedef dbcapi_i32 (*dbcapi_get_function_code_func)( dbcapi_stmt * dbcapi_stmt );
typedef void (*dbcapi_free_stmt_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_set_query_timeout_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_i32 query_timeout );
typedef dbcapi_i32 (*dbcapi_num_params_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_describe_bind_param_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_data * data );
typedef dbcapi_bool (*dbcapi_bind_param_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_data * data );
typedef dbcapi_bool (*dbcapi_send_param_data_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, char * buffer, size_t size );
typedef dbcapi_i32 (*dbcapi_get_param_data_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, size_t offset, void * buffer, size_t size );
typedef dbcapi_bool (*dbcapi_reset_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_get_bind_param_info_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_param_info * info );
typedef dbcapi_bool (*dbcapi_execute_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_stmt * (*dbcapi_execute_direct_func)( dbcapi_connection * dbcapi_conn, const char * sql_str );
typedef dbcapi_bool (*dbcapi_fetch_absolute_func)( dbcapi_stmt * dbcapi_result, dbcapi_i32 row_num );
typedef dbcapi_bool (*dbcapi_fetch_next_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_get_next_result_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i32 (*dbcapi_affected_rows_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i32 (*dbcapi_num_cols_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i32 (*dbcapi_num_rows_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_get_column_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_data_value * buffer );
typedef dbcapi_i32 (*dbcapi_get_data_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, size_t offset, void * buffer, size_t size );
typedef dbcapi_bool (*dbcapi_get_data_info_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_data_info * buffer );
typedef dbcapi_bool (*dbcapi_get_column_info_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_column_info * buffer );
typedef dbcapi_bool (*dbcapi_commit_func)( dbcapi_connection * dbcapi_conn );
typedef dbcapi_bool (*dbcapi_rollback_func)( dbcapi_connection * dbcapi_conn );
typedef dbcapi_bool (*dbcapi_client_version_func)( char * buffer, size_t len );
typedef dbcapi_i32 (*dbcapi_error_func)( dbcapi_connection * dbcapi_conn, char * buffer, size_t size );
typedef size_t (*dbcapi_sqlstate_func)( dbcapi_connection * dbcapi_conn, char * buffer, size_t size );
typedef void (*dbcapi_clear_error_func)( dbcapi_connection * dbcapi_conn );
typedef dbcapi_interface_context *(*dbcapi_init_ex_func)( const char *app_name, dbcapi_u32 api_version, dbcapi_u32 *max_version );
typedef void (*dbcapi_fini_ex_func)( dbcapi_interface_context *context );
typedef dbcapi_connection *(*dbcapi_new_connection_ex_func)( dbcapi_interface_context *context );
typedef dbcapi_connection *(*dbcapi_make_connection_ex_func)( dbcapi_interface_context *context, void *arg );
typedef dbcapi_bool (*dbcapi_client_version_ex_func)( dbcapi_interface_context *context, char *buffer, size_t len );
typedef void (*dbcapi_cancel_func)( dbcapi_connection * dbcapi_conn );
typedef dbcapi_bool (*dbcapi_register_warning_callback_func)( dbcapi_connection * dbcapi_conn, DBCAPI_CALLBACK_PARM callback, void * user_data );
typedef dbcapi_bool (*dbcapi_register_message_callback_func)( dbcapi_connection * dbcapi_conn, DBCAPI_CALLBACK_PARM callback, void * user_data );
typedef dbcapi_bool (*dbcapi_set_batch_size_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 num_rows );
typedef dbcapi_bool (*dbcapi_set_param_bind_type_func)( dbcapi_stmt * dbcapi_stmt, size_t row_size );
typedef dbcapi_u32 (*dbcapi_get_batch_size_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_set_rowset_size_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 num_rows );
typedef dbcapi_u32 (*dbcapi_get_rowset_size_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_set_column_bind_type_func)( dbcapi_stmt * dbcapi_stmt, size_t row_size );
typedef dbcapi_bool (*dbcapi_bind_column_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_data_value * value );
typedef dbcapi_bool (*dbcapi_clear_column_bindings_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i32 (*dbcapi_fetched_rows_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_bool (*dbcapi_set_rowset_pos_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 row_num );
typedef dbcapi_bool (*dbcapi_finish_param_data_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index );
typedef dbcapi_bool (*dbcapi_reset_param_data_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index );
typedef size_t (*dbcapi_error_length_func)( dbcapi_connection * conn );
typedef dbcapi_bool (*dbcapi_get_autocommit_func)( dbcapi_connection * dbcapi_conn, dbcapi_bool * mode );
typedef dbcapi_bool (*dbcapi_set_autocommit_func)( dbcapi_connection * dbcapi_conn, dbcapi_bool mode );
typedef dbcapi_retcode (*dbcapi_get_print_line_func)( dbcapi_stmt * dbcapi_stmt, const dbcapi_i32 host_type, void * buffer,
                                                      size_t * length_indicator, size_t buffer_size, const dbcapi_bool terminate );
#if !defined( SQLA )
typedef dbcapi_i32* (*dbcapi_get_row_status_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i64 (*dbcapi_get_stmt_server_cpu_time_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i64 (*dbcapi_get_stmt_server_memory_usage_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i64 (*dbcapi_get_stmt_server_processing_time_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_retcode (*dbcapi_get_stmt_statement_hash_func)( dbcapi_stmt * dbcapi_stmt, void * buffer, size_t buffer_size );
typedef dbcapi_i64 (*dbcapi_get_resultset_server_cpu_time_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i64 (*dbcapi_get_resultset_server_memory_usage_func)( dbcapi_stmt * dbcapi_stmt );
typedef dbcapi_i64 (*dbcapi_get_resultset_server_processing_time_func)( dbcapi_stmt * dbcapi_stmt );
#endif

typedef dbcapi_bool (*dbcapi_has_next_result_func)(dbcapi_stmt * dbcapi_stmt);
typedef void (*dbcapi_set_stmt_packet_size_func)( dbcapi_stmt * dbcapi_stmt, dbcapi_i32 size );
typedef dbcapi_i32 (*dbcapi_get_stmt_packet_size_func)( dbcapi_stmt * dbcapi_stmt );

#if defined( __cplusplus )
}
#endif

/// @internal
#define function( x ) 	x ## _func x

/** The SAP HANA C API interface structure.
 *
 * Only one instance of this structure is required in your application environment.  This structure
 * is initialized by the dbcapi_initialize_interface method.   It attempts to load the SAP HANA C
 * API DLL or shared object dynamically and looks up all the entry points of the DLL.  The fields in
 * the DBCAPIInterface structure is populated to point to the corresponding functions in the DLL.
 * \sa dbcapi_initialize_interface()
 */
typedef struct DBCAPIInterface {
    /** DLL handle.
     */
    void	* dll_handle;

    /** Flag to know if initialized or not.
     */
    int		  initialized;

    /** Pointer to ::dbcapi_init() function.
     */
    function( dbcapi_init );

    /** Pointer to ::dbcapi_fini() function.
     */
    function( dbcapi_fini );

    /** Pointer to ::dbcapi_new_connection() function.
     */
    function( dbcapi_new_connection );

    /** Pointer to ::dbcapi_free_connection() function.
     */
    function( dbcapi_free_connection );

    /** Pointer to ::dbcapi_make_connection() function.
     */
    function( dbcapi_make_connection );

    /** Pointer to ::dbcapi_connect() function.
     */
    function( dbcapi_connect );

    /** Pointer to ::dbcapi_connect2() function.
     */
    function( dbcapi_connect2 );

    /** Pointer to ::dbcapi_disconnect() function.
     */
    function( dbcapi_disconnect );

    /** Pointer to ::dbcapi_enable_trace_buffering() function.
     */
    function( dbcapi_enable_trace_buffering );

    /** Pointer to ::dbcapi_read_buffered_trace() function.
     */
    function( dbcapi_read_buffered_trace );

    /** Pointer to ::dbcapi_set_connect_property() function.
    */
    function( dbcapi_set_connect_property );

    /** Pointer to ::dbcapi_set_clientinfo() function.
     */
    function( dbcapi_set_clientinfo );

    /** Pointer to ::dbcapi_get_clientinfo() function.
     */
    function( dbcapi_get_clientinfo );

    /** Pointer to ::dbcapi_get_clientinfo_utf8() function.
     */
    function( dbcapi_get_clientinfo_utf8 );

    /** Pointer to ::dbcapi_execute_immediate() function.
     */
    function( dbcapi_execute_immediate );

    /** Pointer to ::dbcapi_prepare() function.
     */
    function( dbcapi_prepare );

    /** Pointer to ::dbcapi_abort() function.
     */
    function( dbcapi_abort );

    /** Pointer to ::dbcapi_get_function_code() function.
     */
    function( dbcapi_get_function_code );

    /** Pointer to ::dbcapi_free_stmt() function.
     */
    function( dbcapi_free_stmt );

    /** Pointer to ::dbcapi_num_params() function.
     */
    function( dbcapi_num_params );

    /** Pointer to ::dbcapi_describe_bind_param() function.
     */
    function( dbcapi_describe_bind_param );

    /** Pointer to ::dbcapi_bind_param() function.
     */
    function( dbcapi_bind_param );

    /** Pointer to ::dbcapi_send_param_data() function.
     */
    function( dbcapi_send_param_data );

    /** Pointer to ::dbcapi_get_param_data() function.
     */
    function( dbcapi_get_param_data );

    /** Pointer to ::dbcapi_reset() function.
     */
    function( dbcapi_reset );

    /** Pointer to ::dbcapi_get_bind_param_info() function.
     */
    function( dbcapi_get_bind_param_info );

    /** Pointer to ::dbcapi_execute() function.
     */
    function( dbcapi_execute );

    /** Pointer to ::dbcapi_execute_direct() function.
     */
    function( dbcapi_execute_direct );

    /** Pointer to ::dbcapi_fetch_absolute() function.
     */
    function( dbcapi_fetch_absolute );

    /** Pointer to ::dbcapi_fetch_next() function.
     */
    function( dbcapi_fetch_next );

    /** Pointer to ::dbcapi_get_next_result() function.
     */
    function( dbcapi_get_next_result );

    /** Pointer to ::dbcapi_affected_rows() function.
     */
    function( dbcapi_affected_rows );

    /** Pointer to ::dbcapi_num_cols() function.
     */
    function( dbcapi_num_cols );

    /** Pointer to ::dbcapi_num_rows() function.
     */
    function( dbcapi_num_rows );

    /** Pointer to ::dbcapi_get_column() function.
     */
    function( dbcapi_get_column );

    /** Pointer to ::dbcapi_get_data() function.
     */
    function( dbcapi_get_data );

    /** Pointer to ::dbcapi_get_data_info() function.
     */
    function( dbcapi_get_data_info );

    /** Pointer to ::dbcapi_get_column_info() function.
     */
    function( dbcapi_get_column_info );

    /** Pointer to ::dbcapi_commit() function.
     */
    function( dbcapi_commit );

    /** Pointer to ::dbcapi_rollback() function.
     */
    function( dbcapi_rollback );

    /** Pointer to ::dbcapi_client_version() function.
     */
    function( dbcapi_client_version );

    /** Pointer to ::dbcapi_error() function.
     */
    function( dbcapi_error );

    /** Pointer to ::dbcapi_sqlstate() function.
     */
    function( dbcapi_sqlstate );

    /** Pointer to ::dbcapi_clear_error() function.
     */
    function( dbcapi_clear_error );

    /** Pointer to ::dbcapi_init_ex() function.
     */
    function( dbcapi_init_ex );

    /** Pointer to ::dbcapi_fini_ex() function.
     */
    function( dbcapi_fini_ex );

    /** Pointer to ::dbcapi_new_connection_ex() function.
     */
    function( dbcapi_new_connection_ex );

    /** Pointer to ::dbcapi_make_connection_ex() function.
     */
    function( dbcapi_make_connection_ex );

    /** Pointer to ::dbcapi_client_version_ex() function.
     */
    function( dbcapi_client_version_ex );

    /** Pointer to ::dbcapi_cancel() function.
     */
    function( dbcapi_cancel );

    /** Pointer to ::dbcapi_set_batch_size() function.
     */
    function( dbcapi_set_batch_size );
    /** Pointer to ::dbcapi_set_param_bind_type() function.
     */
    function( dbcapi_set_param_bind_type );
    /** Pointer to ::dbcapi_get_batch_size() function.
     */
    function( dbcapi_get_batch_size );
    /** Pointer to ::dbcapi_set_rowset_size() function.
     */
    function( dbcapi_set_rowset_size );
    /** Pointer to ::dbcapi_get_rowset_size() function.
     */
    function( dbcapi_get_rowset_size );
    /** Pointer to ::dbcapi_set_column_bind_type() function.
     */
    function( dbcapi_set_column_bind_type );
    /** Pointer to ::dbcapi_bind_column() function.
     */
    function( dbcapi_bind_column );
    /** Pointer to ::dbcapi_clear_column_bindings() function.
     */
    function( dbcapi_clear_column_bindings );
    /** Pointer to ::dbcapi_fetched_rows() function.
     */
    function( dbcapi_fetched_rows );
    /** Pointer to ::dbcapi_set_rowset_pos() function.
     */
    function( dbcapi_set_rowset_pos );

    /** Pointer to ::dbcapi_finish_param_data() function.
     */
    function( dbcapi_finish_param_data );
    /** Pointer to ::dbcapi_reset_param_data() function.
     */
    function( dbcapi_reset_param_data );
    /** Pointer to ::dbcapi_error_length() function.
     */
    function( dbcapi_error_length );
    /** Pointer to ::dbcapi_get_autocommit() function.
     */
    function( dbcapi_get_autocommit );
    /** Pointer to ::dbcapi_set_autocommit() function.
     */
    function( dbcapi_set_autocommit );
    /** Pointer to ::dbcapi_set_transaction_isolation() function.
     */
    function( dbcapi_set_transaction_isolation );
    /** Pointer to ::dbcapi_query_timeout() function.
     */
    function( dbcapi_set_query_timeout );
    /** Pointer to ::dbcapi_register_warning_callback() function.
     */
    function( dbcapi_register_warning_callback );

    /** Pointer to ::dbcapi_register_message_callback() function.
     */
    function( dbcapi_register_message_callback );

    /** Pointer to ::dbcapi_get_print_line() function.
    */
    function( dbcapi_get_print_line );

#if !defined( SQLA )

    /** Pointer to ::dbcapi_get_row_status() function.
    */
    function( dbcapi_get_row_status );

    /** Pointer to ::dbcapi_get_stmt_server_cpu_time() function.
    */
    function( dbcapi_get_stmt_server_cpu_time );

    /** Pointer to ::dbcapi_get_stmt_server_memory_usage() function.
    */
    function( dbcapi_get_stmt_server_memory_usage );

    /** Pointer to ::dbcapi_get_stmt_server_processing_time() function.
    */
    function( dbcapi_get_stmt_server_processing_time );

    /** Pointer to ::dbcapi_get_resultset_server_cpu_time() function.
    */
    function( dbcapi_get_resultset_server_cpu_time );

    /** Pointer to ::dbcapi_get_stmt_statement_hash() function.
    */
    function( dbcapi_get_stmt_statement_hash );

    /** Pointer to ::dbcapi_get_resultset_server_memory_usage() function.
    */
    function( dbcapi_get_resultset_server_memory_usage );

    /** Pointer to ::dbcapi_get_resultset_server_processing_time() function.
    */
    function( dbcapi_get_resultset_server_processing_time );

#endif

    /** Pointer to ::dbcapi_has_next_result() function.
     */
    function( dbcapi_has_next_result );

    /** Pointer to ::dbcapi_set_stmt_packet_size() function.
     */
    function( dbcapi_set_stmt_packet_size );

    /** Pointer to ::dbcapi_get_stmt_packet_size() function.
     */
    function( dbcapi_get_stmt_packet_size );

} DBCAPIInterface;
#undef function

/** Initializes the DBCAPIInterface object and loads the DLL dynamically.
 *
 * Use the following statement to include the function prototype:
 *
 * <pre>
 * \#include "DBCAPI_DLL.h"
 * </pre>
 *
 * This function attempts to load the SAP HANA C API DLL dynamically and looks up all
 * the entry points of the DLL. The fields in the DBCAPIInterface structure are
 * populated to point to the corresponding functions in the DLL. If the optional path
 * argument is NULL, the interface attempts to load the DLL directly (this relies on
 * the environment being setup correctly).
 *
 * \param api An API structure to initialize.
 * \param optional_path_to_dll An optional argument that specifies a path to the SAP HANA C API DLL.
 * \return 1 on successful initialization, and 0 on failure.
 */
int dbcapi_initialize_interface( DBCAPIInterface * api, const char * optional_path_to_dll );

/** Unloads the C API DLL library and resets the DBCAPIInterface structure.
 *
 * Use the following statement to include the function prototype:
 *
 * <pre>
 * \#include "DBCAPI_DLL.h"
 * </pre>
 *
 * Use this method to finalize and free resources associated with the SAP HANA C API DLL.
 *
 * \param api An initialized structure to finalize.
 */

void dbcapi_finalize_interface( DBCAPIInterface * api );

#endif // __DBCAPIDLL_H__
