/* -------------------------------------------------------------------------
 *
 * libpqsw.cpp
 *
 * This file contains the libpq standby write parts of multi node write. 
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/libpqsw.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>
#include "libpq/libpq-int.h"
#include "replication/libpqsw.h"
#include "replication/walreceiver.h"
#include "utils/postinit.h"
#include "optimizer/planner.h"
#include "nodes/parsenodes_common.h"
#include "commands/prepare.h"
#include "tcop/tcopprot.h"

#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

struct pg_conn;
typedef int (*libpqsw_transfer_standby_func)(const char* s, size_t len);
/*
* transfer primary msg to frontend.
*/
int internal_putbytes(const char* s, size_t len);
int pq_flush(void);
PGresult* libpqsw_get_result(PGconn* conn, libpqsw_transfer_standby_func transfer_func);
bool libpqsw_connect(char* conninfo, const char *dbName, const char* userName);
void libpqsw_disconnect(void);
bool libpqsw_send_pbe(const char* buffer, size_t buffer_size);
bool libpqsw_begin_command(const char* commandTag);
bool libpqsw_end_command(const char* commandTag);
void libpqsw_set_redirect(bool redirect);
void libpqsw_set_command_tag(const char* commandTag);
void libpqsw_set_set_command(bool set_command);

static void libpqsw_set_already_connected()
{
    get_redirect_manager()->state.already_connected = true;
}

//nothing to do for not transfer message.
static int libpqsw_skip_master_message(const char* s, size_t len)
{
    return 0;
}

static inline knl_u_libsw_context* get_sw_cxt()
{
    Assert(u_sess != NULL);
    return &(u_sess->libsw_cxt);
}

// create a empty message struct
RedirectMessage* RedirectMessageManager::create_redirect_message(RedirectType msg_type)
{
    // caller must release memory!
    RedirectMessage* cur_msg = (RedirectMessage*)palloc(sizeof(RedirectMessage));
    cur_msg->cur_pos = 0;
    cur_msg->type = msg_type;
    check_strncpy_s(
        strncpy_s(cur_msg->commandTag, sizeof(cur_msg->commandTag), get_sw_cxt()->commandTag, strlen(get_sw_cxt()->commandTag))
        );
    for (int i = 0; i < PBE_MESSAGE_STACK; i++) {
        cur_msg->pbe_stack_msgs[i] = makeStringInfo();
    }
    return cur_msg;
}

void RedirectMessageManager::push_message(int qtype, StringInfo msg, bool need_switch, RedirectType msg_type)
{
    MemoryContext old = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    // NOTICE: we malloc msg, so we need free myself
    if (messages == NIL || need_switch) {
        messages = lappend(messages, create_redirect_message(msg_type));
    }
    
    RedirectMessage* cur_msg = (RedirectMessage*) llast(messages);
    if (message_overflow(cur_msg)) {
        MemoryContextSwitchTo(old);
        ereport(ERROR, (errmsg("pbe message stackoverflow,please check it!")));
    }
    cur_msg->pbe_types[cur_msg->cur_pos] = qtype;
    if (msg_type == RT_NORMAL) {
        // any normal msg will change 
        cur_msg->type = RT_NORMAL;
    }
    last_message = qtype;
    copyStringInfo(cur_msg->pbe_stack_msgs[cur_msg->cur_pos], msg);
    cur_msg->cur_pos ++;
    MemoryContextSwitchTo(old);
}

const StringInfo RedirectMessageManager::get_merge_message(RedirectMessage* msg)
{
    StringInfo result = msg->pbe_stack_msgs[PBE_MESSAGE_MERGE_ID];
    StringInfo tmp = NULL;
    resetStringInfo(result);
    for (int i = 0; i < msg->cur_pos; i++) {
        tmp = msg->pbe_stack_msgs[i];
        appendStringInfoChar(result, (char) msg->pbe_types[i]);
        int n32 = htonl((uint32) ((size_t)tmp->len + sizeof(uint32)));
        appendBinaryStringInfo(result, (char *)&n32, sizeof(uint32));
        appendBinaryStringInfo(result, tmp->data, tmp->len);
    }

    if (result->len > 0 && libpqsw_log_enable()) {
        StringInfo trace_msg = makeStringInfo();
        output_messages(trace_msg, msg);
        libpqsw_trace("%s", trace_msg->data);
        DestroyStringInfo(trace_msg);
    }
    return result;
}

void RedirectMessageManager::output_messages(StringInfo output, RedirectMessage* msg) const
{
    resetStringInfo(output);
    StringInfo tmp = NULL;
    for (int i = 0; i < msg->cur_pos; i++) {
        tmp = msg->pbe_stack_msgs[i];
        appendStringInfoChar(output, (char) msg->pbe_types[i]);
        appendStringInfoChar(output, ' ');
    }
}

// flow database system log config
bool RedirectManager::log_enable()
{
    return u_sess != NULL && u_sess->attr.attr_common.log_statement == LOGSTMT_ALL;
}

/*
 * Judge if enable remote_excute.
 */
bool enable_remote_excute()
{
    if (!g_instance.attr.attr_sql.enableRemoteExcute) {
        return false;
    }
	// quick judge is master node
    if (t_thrd.role == SW_SENDER) {
        return false;
    }

    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ServerMode serverMode = hashmdata->current_mode;
    return serverMode == STANDBY_MODE;
}

// judge if already execute set autocommit = 0;
bool libpqsw_enable_autocommit()
{
    return u_sess->attr.attr_storage.phony_autocommit;
}

// receive primary msg to libpqrac stream.
static bool libpqsw_receive(bool transfer = true)
{
    if (get_sw_cxt()->streamConn == NULL)
        return false;
    PGconn* conn = get_sw_cxt()->streamConn;
    PGresult* result = NULL;
    PGresult* lastResult = NULL;
    bool retStatus = true;
    
    conn->inStart = 0;
    conn->inEnd = 0;
    conn->inCursor = 0;
    libpqsw_transfer_standby_func transfer_func = transfer ? internal_putbytes : libpqsw_skip_master_message;

    while ((result = libpqsw_get_result(conn, transfer_func)) != NULL) {
        if (lastResult != NULL) {
            if (lastResult->resultStatus == PGRES_FATAL_ERROR && result->resultStatus == PGRES_FATAL_ERROR) {
                pqCatenateResultError(lastResult, result->errMsg);
                PQclear(result);
                result = lastResult;
                retStatus = false;

                /*
                 * Make sure PQerrorMessage agrees with concatenated result
                 */
                resetPQExpBuffer(&conn->errorMessage);
                appendPQExpBufferStr(&conn->errorMessage, result->errMsg);
            } else {
                PQclear(lastResult);
            }
        }
        lastResult = result;
        if (0 < conn->inStart) {
            transfer_func(conn->inBuffer, conn->inStart);
        }
        /* Left-justify any data in the buffer to make room */
        if (conn->inStart < conn->inEnd) {
            if (conn->inStart > 0) {
                check_memmove_s(memmove_s(conn->inBuffer,
                    conn->inEnd - conn->inStart,
                    conn->inBuffer + conn->inStart,
                    conn->inEnd - conn->inStart));
                conn->inEnd -= conn->inStart;
                conn->inCursor -= conn->inStart;
                conn->inStart = 0;
            }
        } else {
            /* buffer is logically empty, reset it */
            conn->inStart = conn->inCursor = conn->inEnd = 0;
        }
        if (result->resultStatus == PGRES_COPY_IN || result->resultStatus == PGRES_COPY_OUT ||
            result->resultStatus == PGRES_COPY_BOTH || conn->status == CONNECTION_BAD) {
            break;
        }  
    }
    if (0 < conn->inEnd) {
        transfer_func(conn->inBuffer, conn->inEnd);
    }
    PQclear(lastResult);
    pq_flush();
    return true;
}

/*
* this function will transfer msg to master, we support simple & extend query here.
*/
static bool libpqsw_remote_excute_sql(int retry, const char* sql, uint32 size, const char *dbName,
    const char *userName, const char *commandTag, bool waitResult, bool transfer)
{
    if (get_sw_cxt()->streamConn == NULL) {
        libpqsw_disconnect();
        char conninfo[MAXCONNINFO];
        errno_t rc = EOK;

        rc = memset_s(conninfo, MAXCONNINFO, 0, MAXCONNINFO);
        securec_check(rc, "\0", "\0");

        /* Fetch information required to start streaming */
        rc = strncpy_s(conninfo, MAXCONNINFO, (char*)(t_thrd.walreceiverfuncs_cxt.WalRcv->conninfo), MAXCONNINFO - 1);
        securec_check(rc, "\0", "\0");
        libpqsw_connect(conninfo, dbName, userName);
        libpqsw_set_already_connected();
    } else if(PQstatus(get_sw_cxt()->streamConn) != CONNECTION_OK) {
        libpqsw_disconnect();
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_RESET_BY_PEER),
                errmsg("connection already bad!%s",
                t_thrd.walreceiverfuncs_cxt.WalRcv->conninfo)));
    } else {
        // nothing to do.
    }

    // to transfer sql.
    if (libpqsw_send_pbe(sql, size)) {
        if (waitResult) {
            return libpqsw_receive(transfer);
        }
    } else {
        // send failed, so we need retry!
        libpqsw_disconnect();
        if (retry > 0) {
            return libpqsw_remote_excute_sql(retry - 1, sql, size,
                    dbName, userName, commandTag,
                    waitResult, transfer);
        }
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_RESET_BY_PEER),
                errmsg("libpqsw: send sql failed!%s",
                t_thrd.walreceiverfuncs_cxt.WalRcv->conninfo)));
    }
    return true;
}

RedirectManager* get_redirect_manager()
{
    RedirectManager* redirect_manager = (RedirectManager*)get_sw_cxt()->redirect_manager;
    Assert(redirect_manager != NULL);
    return redirect_manager;
}

// get is transaction state
static bool libpqsw_get_transaction()
{
    return get_redirect_manager()->state.transaction;
}

static bool libpqsw_remote_in_transaction()
{
     return get_sw_cxt()->streamConn
        && get_sw_cxt()->streamConn->xactStatus == PQTRANS_INTRANS;
}

/* get if session seek next */
bool libpqsw_can_seek_next_session()
{
    if (!get_redirect_manager()->get_remote_excute()) {
        return true;
    }
    return !libpqsw_remote_in_transaction() && !libpqsw_get_transaction();
}

void libpqsw_cleanup(int code, Datum arg)
{
    if (u_sess == NULL) {
        return;
    }
    ereport(LIBPQSW_DEFAULT_LOG_LEVEL,
        (errmsg("libpqsw(%ld): cleanup called!",
            get_sw_cxt()->redirect_manager == NULL ? -1 : ((int64)(get_sw_cxt()->redirect_manager)))));
    if (get_sw_cxt()->streamConn != NULL) {
        libpqsw_disconnect();
    }
    if (get_sw_cxt()->redirect_manager != NULL) {
        DELETE_EX_TYPE(get_sw_cxt()->redirect_manager, RedirectManager);
    }
}


// set is in transaction insert
static void libpqsw_set_transaction(bool transaction)
{
    if (transaction != get_redirect_manager()->state.transaction) {
        libpqsw_trace("transaction status change, current commit status:%s", transaction ? "begin": "end");
    }
    get_redirect_manager()->state.transaction = transaction;
}

static bool libpqsw_before_redirect(const char* commandTag, List* query_list, const char* query_string)
{
    RedirectManager* redirect_manager = get_redirect_manager();
    if (!redirect_manager->get_remote_excute()) {
        return false;
    }
    if (commandTag == NULL) {
        commandTag = "";
    }
    bool need_redirect = false;
    if (!libpqsw_enable_autocommit()) {
        if (strcmp(commandTag, "SET") == 0) {
            libpqsw_set_set_command(true);
            return false;
        }
        libpqsw_set_transaction(true);
        need_redirect = true;
    } else if (libpqsw_begin_command(commandTag) || libpqsw_remote_in_transaction()) {
        libpqsw_set_transaction(true);
        need_redirect = true;
    } else if(libpqsw_redirect()) {
        need_redirect = true;
    } else {
        if (strcmp(commandTag, "SET") == 0) {
            libpqsw_set_set_command(true);
            return false;
        }
        if (strcmp(commandTag, "SHOW") == 0) {
            return false;
        }
        if (query_list == NIL) {
            return false;
        }

        ListCell* remote_lc = NULL;
        foreach (remote_lc, query_list) {
            Query* tmp_query = (Query*)lfirst(remote_lc);
            if (!queryIsReadOnly(tmp_query)) {
                need_redirect = true;
                libpqsw_set_redirect(true);
                if (query_string != NULL) {
                    libpqsw_trace("we find new transfer sql by query_list:%s", query_string);
                }
                break;
            }
        }
    }
    // first write sql, so we need to create connection.
    if (need_redirect) {
        libpqsw_set_already_connected();
    }
    return need_redirect;
}

static void libpqsw_after_redirect()
{
    if (!libpqsw_remote_in_transaction()) {
        finish_xact_command();
        libpqsw_set_transaction(false);
    }
}

/* is need send ready_for_query messge to front, if in redirect then false*/
bool libpqsw_need_end()
{
    return get_redirect_manager()->state.need_end;
}
/* udpate if need ready_for_query messge flag */
void libpqsw_set_end(bool is_end)
{
    get_redirect_manager()->state.need_end = is_end;
}

/* udpate redirect flag */
void libpqsw_set_redirect(bool redirect)
{
    get_redirect_manager()->state.redirect = redirect;
}

/* udpate redirect flag */
bool libpqsw_get_redirect()
{
    return get_redirect_manager()->state.redirect;
}


// set is in batch insert
void libpqsw_set_batch(bool batch)
{
    get_redirect_manager()->state.batch = batch;
}

// get is batch state
static bool libpqsw_get_batch()
{
    return get_redirect_manager()->state.batch;
}

/* query if enable redirect*/
bool libpqsw_redirect()
{
    return libpqsw_get_redirect() || libpqsw_get_batch() || libpqsw_get_transaction();
}

/* query if enable set command*/
bool libpqsw_get_set_command()
{
    return get_redirect_manager()->state.set_command;
}

/* if skip readonly check in P or Q message */
bool libpqsw_skip_check_readonly() {
    return get_redirect_manager()->get_remote_excute();
}

bool libpqsw_skip_close_command() {
    // only master node and in SW_SENDER will skip 'C' message.
    // mix use prepared statement in master and slave may get some error
    return g_instance.attr.attr_sql.enableRemoteExcute && t_thrd.role == SW_SENDER;
}

/* query if enable set command*/
void libpqsw_set_set_command(bool set_command)
{
    get_redirect_manager()->state.set_command = set_command;
}

/* 
* wrapper remote excute for extend query (PBE)
*/
static void libpqsw_inner_excute_pbe(bool waitResult, bool updateFlag)
{
    RedirectManager* redirect_manager = get_redirect_manager();
    RedirectMessageManager* message_manager = &(redirect_manager->messages_manager);
    if (message_manager->message_empty()) {
        return;
    }

    foreach_cell(message, message_manager->get_messages()) {
        RedirectMessage* redirect_msg = (RedirectMessage*)lfirst(message);
        const StringInfo pbe_send_message = message_manager->get_merge_message(redirect_msg);
        redirect_msg->cur_pos = 0;
        const char* db_name = u_sess->proc_cxt.MyProcPort->database_name;
        const char* username = u_sess->proc_cxt.MyProcPort->user_name;
        (void)libpqsw_remote_excute_sql(0, pbe_send_message->data,
            pbe_send_message->len,
            db_name,
            username,
            redirect_msg->commandTag,
            waitResult,
            redirect_msg->type == RT_NORMAL);
        if (updateFlag) {
            libpqsw_after_redirect();
        }
    }
    message_manager->reset();
}

static inline int libpqsw_connection_status() {
    return get_sw_cxt()->streamConn == NULL ? -1 : get_sw_cxt()->streamConn->xactStatus;
}

/*
* only support P msg.
*/
static inline void libpqsw_trace_p_msg(int qtype, StringInfo msg)
{
    const char* stmt = msg->data;
    const char* query = msg->data + strlen(stmt) + 1;
    libpqsw_trace("P: stmt=%s, query=%s", stmt, query);
}

/*
* only support B msg.
*/
static inline void libpqsw_trace_b_msg(int qtype, StringInfo msg)
{
    const char* portal = msg->data;
    const char* stmt = msg->data + strlen(portal) + 1;
    libpqsw_trace("B: portal=%s, stmt=%s, trans:%d", portal, stmt,
        libpqsw_connection_status()
        );
}

/*
* only support U msg.
*/
static inline void libpqsw_trace_u_msg(int qtype, StringInfo msg)
{
    int batch_count = ntohl(*(uint32*)(msg->data));
    const char* portal = msg->data + 4;
    const char* stmt = msg->data + 4 + strlen(portal) + 1;
    libpqsw_trace("U: portal=%s, stmt=%s, trans:%d, count:%d",
        portal,
        stmt,
        libpqsw_connection_status(),
        batch_count);
}

/*
* only support C msg.
*/
static inline void libpqsw_trace_c_msg(int qtype, StringInfo msg)
{
    unsigned char close_type = (unsigned char)(msg->data[0]);
    const char* close_target = msg->data + 1;
    libpqsw_trace("C: close_type=%c, target_name=%s, trans:%d, redirect:%s, remote:%s",
        close_type,
        close_target,
        libpqsw_connection_status(),
        libpqsw_redirect() ? "true" : "false",
        libpqsw_remote_in_transaction() ? "true" : "false");
}

/*
* only support other msg.
*/
static inline void libpqsw_trace_other_msg(int qtype, StringInfo msg)
{
    libpqsw_trace("%c: %d data=%s, size=%d, trans:%d, redirect:%s",
        qtype,
        qtype,
        msg->data == NULL ? "" : msg->data,
        msg->len,
        libpqsw_connection_status(),
        libpqsw_redirect() ? "true" : "false");
}

static inline void libpqsw_trace_empty_msg(int qtype, StringInfo msg)
{
    // nothing to do.
}

typedef void (*trace_msg_func)(int qtype, StringInfo info);
static trace_msg_func get_msg_trace_func(int qtype)
{
    trace_msg_func cur_func = libpqsw_trace_empty_msg;
    if (!libpqsw_log_enable()) {
        return cur_func;
    }
    switch(qtype) {
        case 'P':
            cur_func = libpqsw_trace_p_msg;
            break;
        case 'B':
            cur_func = libpqsw_trace_b_msg;
            break;
        case 'U':
            cur_func = libpqsw_trace_u_msg;
            break;
        case 'C':
            cur_func = libpqsw_trace_c_msg;
            break;
        default:
            cur_func = libpqsw_trace_other_msg;
            break;
    }
    return cur_func;
}

static CachedPlanSource* libpqsw_get_plancache(StringInfo msg)
{
    const char* stmt = msg->data + strlen(msg->data) + 1;
    CachedPlanSource* psrc = NULL;
    if (strlen(stmt) != 0) {
        PreparedStatement *pstmt = FetchPreparedStatement(stmt, false, false);
        if (pstmt != NULL) {
            psrc = pstmt->plansource;
        }
    } else {
        psrc = u_sess->pcache_cxt.unnamed_stmt_psrc;
    }
    if (psrc == NULL) {
        libpqsw_warn("we can't find cached plan, stmt=%s", stmt);
    }
    return psrc;
}
/*
* if B message begin, we need search local plancache to query if
* it is start transaction command.
*/
static bool libpqsw_process_bind_message(StringInfo msg)
{
    if (get_redirect_manager()->messages_manager.message_empty()
        && libpqsw_remote_in_transaction()) {
        libpqsw_set_transaction(true);
        return true;
    }
    CachedPlanSource* psrc = libpqsw_get_plancache(msg);
    if (psrc == NULL) {
        return false;
    }
    return libpqsw_before_redirect(psrc->commandTag, psrc->query_list, psrc->query_string);
}

/*
* this message obviously need judge if need transfer.
*/
static void libpqsw_process_transfer_message(int qtype, StringInfo msg)
{
    if (libpqsw_redirect()) {
        // we need update commandTag
        if (qtype == 'B') {
            CachedPlanSource* psrc = libpqsw_get_plancache(msg);
            if (psrc != NULL) {
                libpqsw_set_command_tag(psrc->commandTag);
                libpqsw_before_redirect(psrc->commandTag, psrc->query_list, psrc->query_string);
            }
        }
        return;
    }
    if (qtype == 'U') {
        libpqsw_set_batch(true);
    } else if (qtype == 'B') {
        libpqsw_process_bind_message(msg);
    } else if (qtype == 'E') {
        if (libpqsw_remote_in_transaction()) {
            libpqsw_set_transaction(true);
        }
    } else {
        // nothing to do
    }
}

/*
* Process msg from backend. if return true, qtype message will skiped!
* blow case will process message(AND condition):
  1. not 'P' and 'Q' message // this is query begin
  2. not 'X' and -1 // this is connection end
  3. set enable_remote_excute = true
  4. in standby mode
  5. 'COMMIT' commandTag already execute.
* blow case will process message(OR condition):
  1. remote in transaction mode.
  2. B E U qtype and need transfer
*/
bool libpqsw_process_message(int qtype, StringInfo msg)
{
    RedirectManager* redirect_manager = get_redirect_manager();
	//if disable remote excute
    if (!redirect_manager->get_remote_excute()) {
        return false;
    }
    trace_msg_func trace_func = get_msg_trace_func(qtype);
    trace_func(qtype, msg);
	// the extend query start msg
    if (qtype == 'P') {
        return false;
    }
	// the simple query start msg
    if (qtype == 'Q') {
        return false;
    }
    if (qtype == 'C') {
        // need close standby plancache.
        if (libpqsw_redirect()) {
            if (!libpqsw_skip_close_command()) {
                redirect_manager->push_message(qtype, msg, false, RT_NORMAL);
            }
        }
        return false;
    }
	// exit msg
    if (qtype == 'X' || qtype == -1) {
        libpqsw_receive(true);
        libpqsw_disconnect();
        return false;
    }
	// process U B E msg
    libpqsw_process_transfer_message(qtype, msg);
    bool ready_to_excute = false;
    if (libpqsw_get_set_command()) {
        ready_to_excute = redirect_manager->push_message(qtype, msg, false, RT_SET);
        if (ready_to_excute) {
            libpqsw_inner_excute_pbe(true, true);
        }
        if (qtype == 'S' || qtype == 'Q') {
            libpqsw_set_set_command(false);
        }
        return false;
    }

    if (!libpqsw_redirect()) {
        return false;
    }

    ready_to_excute = redirect_manager->push_message(qtype, msg, false, RT_NORMAL);
    if (ready_to_excute) {
        libpqsw_inner_excute_pbe(true, true);
        libpqsw_set_batch(false);
        libpqsw_set_redirect(false);
        libpqsw_set_set_command(false);
    }
    return true;
}

/* process P type msg, true if need redirect*/
bool libpqsw_process_parse_message(const char* commandTag, List* query_list)
{
    libpqsw_set_command_tag(commandTag);
    bool need_redirect = libpqsw_before_redirect(commandTag, query_list, NULL);
    if (need_redirect) {
        libpqsw_set_end(false);
    } else {
        libpqsw_set_end(true);
    }
    return need_redirect;
}

/* process Q type msg, true if need in redirect mode*/
bool libpqsw_process_query_message(const char* commandTag, List* query_list, const char* query_string,
    size_t query_string_len)
{
    libpqsw_set_command_tag(commandTag);
    bool need_redirect = libpqsw_before_redirect(commandTag, query_list, query_string);
    if (need_redirect) {
        StringInfo curMsg = makeStringInfo();
        initStringInfo(curMsg);
        appendStringInfoString(curMsg, query_string);
        appendStringInfoChar(curMsg, 0);
        if(get_redirect_manager()->push_message('Q', curMsg, true, RT_NORMAL)) {
            libpqsw_inner_excute_pbe(true, true);
        }
        // because we are not skip Q message process, so send_ready_for_query will be true after transfer.
        // but after transter, master will send Z message for front, so we not need to this flag.
        libpqsw_set_end(false);
    } else {
        // we need send_ready_for_query for init.
        libpqsw_set_end(true);
        if (libpqsw_get_set_command()) {
            StringInfo curMsg = makeStringInfo();
            initStringInfo(curMsg);
            appendStringInfoString(curMsg, query_string);
            appendStringInfoChar(curMsg, 0);
            if(get_redirect_manager()->push_message('Q', curMsg, true, RT_SET)) {
                libpqsw_inner_excute_pbe(true, false);
            }
        }
    }
    libpqsw_set_set_command(false);
    libpqsw_set_redirect(false);
    return need_redirect;
}

// is start transaction command
bool libpqsw_begin_command(const char* commandTag)
{
    return commandTag != NULL && (strcmp(commandTag, "BEGIN") == 0 || strcmp(commandTag, "START TRANSACTION") == 0);
}

// is end transaction command
bool libpqsw_end_command(const char* commandTag)
{
    return commandTag != NULL && (strcmp(commandTag, "COMMIT") == 0 || strcmp(commandTag, "ROLLBACK") == 0);
}

// set commandTag
void libpqsw_set_command_tag(const char* commandTag)
{
    get_sw_cxt()->commandTag = commandTag;
}

// session never timeout!
static void libpqsw_session_never_timout(struct pg_conn* conn) {
    PGresult* res = PQexec(conn, "SET session_timeout = 0");
    PQclear(res);
}

static void libpqsw_process_port_trace()
{
    if (!(LIBPQSW_ENABLE_PORT_TRACE)) {
        return;
    }
    char trace_file_path[MAX_PATH_LEN + 1] = {0};
    char real_path[MAX_PATH_LEN + 1] = {0};
    char* loghome = gs_getenv_r("GAUSSLOG");
    int ret = 0;
    if (loghome && '\0' != loghome[0]) {
        check_backend_env(loghome);
        if (realpath(loghome, real_path) == NULL) {
            libpqsw_warn("failed to realpath $GAUSSLOG/pg_log!");
            return;
        }
        ret = snprintf_s(trace_file_path, MAX_PATH_LEN + 1, MAX_PATH_LEN, "%s/pg_log/libpqsw", real_path);
        securec_check_ss(ret, "", "");
    } else {
        ret = snprintf_s(trace_file_path, MAX_PATH_LEN + 1, MAX_PATH_LEN, "./pg_log/libpqsw");
        securec_check_ss(ret, "", "");
    }
    
    // trace_file_path not exist, create trace_file_path path
    if (0 != pg_mkdir_p(trace_file_path, S_IRWXU) && errno != EEXIST) {
        libpqsw_warn("failed to mkdir $GAUSSLOG/pg_log/libpqsw!");
        return;
    }

    char trace_file[MAX_PATH_LEN + 1] = {0};
    ret = snprintf_s(trace_file, MAX_PATH_LEN + 1, MAX_PATH_LEN, "%s/%ld.log",
        trace_file_path, (int64)(get_sw_cxt()->redirect_manager));
    securec_check_ss(ret, "", "");
    FILE* cur_file = fopen(trace_file, "w");
    if (cur_file != NULL) {
        get_sw_cxt()->conn_trace_file = cur_file; 
        PQtrace(get_sw_cxt()->streamConn, cur_file);
    }
}

/*
 * Establish the connection to the primary server like replication stream.
 */
bool libpqsw_connect(char* conninfo, const char *dbName, const char* userName)
{
    char conninfoRepl[MAXCONNINFO + 75];
    int nRet = 0;
    char hostname[255];

    (void)gethostname(hostname, 255);

    nRet = snprintf_s(conninfoRepl,
        sizeof(conninfoRepl),
        sizeof(conninfoRepl) - 1,
        "%s dbname=%s user=%s replication=standbywrite "
        "fallback_application_name=%s "
        "connect_timeout=%d client_encoding=auto",
        conninfo, dbName, userName,
        "sw",
        u_sess->attr.attr_storage.wal_receiver_connect_timeout);

    securec_check_ss(nRet, "", "");
    
    get_sw_cxt()->streamConn = PQconnectdb(conninfoRepl);
    if (PQstatus(get_sw_cxt()->streamConn) != CONNECTION_OK) {
        libpqsw_info("Connecting to remote server :%s ...failed!", conninfoRepl);
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                errmsg("standbywrite could not connect to the remote server,the connection info :%s : %s",
                    conninfo,
                    PQerrorMessage(get_sw_cxt()->streamConn))));
    }
    libpqsw_info("Connecting to remote server :%s ...success!", conninfoRepl);
    libpqsw_session_never_timout(get_sw_cxt()->streamConn);
    libpqsw_process_port_trace();
    return true;
}

/*
 * Disconnect connection to primary, if any.
 */
void libpqsw_disconnect(void)
{
    ereport(LIBPQSW_DEFAULT_LOG_LEVEL,
        (errmsg("libpqsw(%ld): libpqsw_disconnect called, conn is null:%s",
            get_sw_cxt()->redirect_manager == NULL ? -1 : ((int64)(get_sw_cxt()->redirect_manager)),
            get_sw_cxt()->streamConn == NULL ? "true" : "false")));
    if (get_sw_cxt()->streamConn != NULL) {
        if (get_sw_cxt()->conn_trace_file != NULL) {
            PQuntrace(get_sw_cxt()->streamConn);
            fclose(get_sw_cxt()->conn_trace_file);
            get_sw_cxt()->conn_trace_file = NULL;
        }
        PQfinish(get_sw_cxt()->streamConn);
        get_sw_cxt()->streamConn = NULL;
    }
    
    if (get_sw_cxt()->redirect_manager != NULL) {
        get_redirect_manager()->init();
    }
}

// parse primary results.
PGresult* libpqsw_get_result(PGconn* conn, libpqsw_transfer_standby_func transfer_func)
{
    PGresult* res = NULL;

    if (conn == NULL)
        return NULL;

    /* Parse any available data, if our state permits. */
    parseInput(conn);

    /* If not ready to return something, block until we are. */
    while (conn->asyncStatus == PGASYNC_BUSY) {
        int flushResult;

        /*
         * If data remains unsent, send it.  Else we might be waiting for the
         * result of a command the backend hasn't even got yet.
         */
        while ((flushResult = pqFlush(conn)) > 0) {
            if (pqWait(FALSE, TRUE, conn)) {
                flushResult = -1;
                break;
            }
        }
        // because pqReadData will reset inStart to 0, so we must send to frontend before pqReadData.
        if (0 < get_sw_cxt()->streamConn->inStart) {
            transfer_func(get_sw_cxt()->streamConn->inBuffer, get_sw_cxt()->streamConn->inStart);
        }
        /* Wait for some more data, and load it. */
        if (flushResult || pqWait(TRUE, FALSE, conn) || pqReadData(conn) < 0) {
            /*
             * conn->errorMessage has been set by pqWait or pqReadData. We
             * want to append it to any already-received error message.
             */
            libpqsw_trace("libpqsw_get_result->read data failed, conn_state:%d,[ok->%d, bad->%d]",
                conn->status,
                CONNECTION_OK,
                CONNECTION_BAD);
            conn->status = CONNECTION_BAD;
            pqSaveErrorResult(conn);
            conn->asyncStatus = PGASYNC_IDLE;
            
            return pqPrepareAsyncResult(conn);
        }
        /* Parse it. */
        parseInput(conn);
    }

    /* Return the appropriate thing. */
    switch (conn->asyncStatus) {
        case PGASYNC_IDLE:
            res = NULL; /* query is complete */
            break;
        case PGASYNC_READY:
            res = pqPrepareAsyncResult(conn);
            /* Set the state back to BUSY, allowing parsing to proceed. */
            conn->asyncStatus = PGASYNC_BUSY;
            break;
        case PGASYNC_COPY_IN:
            res = getCopyResult(conn, PGRES_COPY_IN);
            break;
        case PGASYNC_COPY_OUT:
            res = getCopyResult(conn, PGRES_COPY_OUT);
            break;
        case PGASYNC_COPY_BOTH:
            res = getCopyResult(conn, PGRES_COPY_BOTH);
            break;
        default:
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("unexpected asyncStatus: %d, remote datanode %s, err: %s\n"),
                (int) conn->asyncStatus, conn->remote_nodename, strerror(errno));
            res = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
            break;
    }

    if (res != NULL) {
        int i;

        for (i = 0; i < res->nEvents; i++) {
            PGEventResultCreate evt;

            evt.conn = conn;
            evt.result = res;
            if (!(res->events[i].proc(PGEVT_RESULTCREATE, &evt, res->events[i].passThrough))) {
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("PGEventProc \"%s\" failed during PGEVT_RESULTCREATE event, remote datanode %s, err: %s\n"),
                    res->events[i].name, conn->remote_nodename, strerror(errno));
                pqSetResultError(res, conn->errorMessage.data);
                res->resultStatus = PGRES_FATAL_ERROR;
                break;
            }
            res->events[i].resultInitialized = TRUE;
        }
    }

    return res;
}

// check connection and protocal version.
static int libpqsw_before_send(PGconn* conn)
{
    if (conn == NULL)
        return false;
    if (PG_PROTOCOL_MAJOR(conn->pversion) < 3) {
        libpqsw_warn("libpqsw_transfer could not run protocal less then 3");
        return false;
    }


    /* clear the error string */
    resetPQExpBuffer(&conn->errorMessage);

    /* Don't try to send if we know there's no live connection. */
    if (conn->status != CONNECTION_OK) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("no connection to the server\n"));
        return false;
    }
    /* initialize async result-accumulation state */
    pqClearAsyncResult(conn);
    return true;
}

// send extend query to master.
bool libpqsw_send_pbe(const char* buffer, size_t buffer_size)
{
    struct pg_conn* conn =  get_sw_cxt()->streamConn;
    bool result = true;
    if (!libpqsw_before_send(conn)) {
        libpqsw_disconnect();
        result = false;
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("libpqsw_send_pbe check failed, master can't connect!")));
    }
    conn->outMsgEnd = conn->outMsgStart = conn->outCount;
    if (pqPutnchar(buffer, buffer_size, conn) < 0) {
        libpqsw_disconnect();
        result = false;
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_STATUS),
                errmsg("libpqsw_send_pbe could not send: %s", PQerrorMessage(conn))));
    }
    pqPutncharMsgEnd(conn);

    /* remember we are using extended query protocol */
    conn->queryclass = PGQUERY_EXTENDED;

    /*
     * Give the data a push.  In nonblock mode, don't complain if we're unable
     * to send it all; PQgetResult() will do any additional flushing needed.
     */
    if (pqFlush(conn) < 0) {
        result = false;
        pqHandleSendFailure(conn);
        return result;
    }

    /* OK, it's launched! */
    conn->asyncStatus = PGASYNC_BUSY;
    return result;
}

