#include "workload/resource_manager.h"
#include "access/transam.h"
#include "access/ustore/knl_whitebox_test.h"
// #include "access/ustore/storage_uheap_verify.h"
#include "access/extreme_rto/page_redo.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_thread.h"
#include "storage/ipc.h"
#include "storage/proc.h"
// #include "storage/procarry.h"
#include "storage/lock/lwlock.h"
#include "threadpool/threadpool.h"
#include "utils/dynahash.h"
#include "utils/postinit.h"
#include "utils/gs_bitmap.h"
#include "pgstat.h"
#include "funcapi.h"
#include "catalog/pg_authid.h"
#include "math.h"

const int EXIT_NUMBER = 2;
#ifndef Min
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#endif

#ifndef MAX
#define MAX(A, B) ((B) > (A) ? (B) : (A))
#endif

const float PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE = 0.8;

void resource_manager_sigh_up_handler();
void resource_manager_sigterm_handler();
void quick_die(SIGNAL_ARGS);
void shutdown_resource_manager();
void adapt_create_dirty_page_rate();
void resource_manager_main();


void resource_manager_sigh_up_handler(SIGNAL_ARGS){
    int save_errno = errno;
    t_thrd.resource_manager_cxt.got_SIGHUP = true;
    if(t_thrd.proc){
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}
void resource_manager_sigterm_handler(SIGNAL_ARGS){
    int save_errno = errno;
    t_thrd.resource_manager_cxt.got_SIGTERM = true;
    if(t_thrd.proc){
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}
void quick_die(SIGNAL_ARGS){
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    exit(EXIT_NUMBER);
}
void shutdown_resource_manager(){
    // ERR_PROC(GAUSS_42108, ERR_LEVEL_LOG,(err_msg(UNDOFORMAT("Resource Manager: shutting down"))));
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    // resbookkeeper_release(t_thrd.utils_cxt.CurrentResourceOwner, RESOUCE_RELEASE_BEFORE_LOCKS, false, true);
    proc_exit(0);
}
void adapt_create_dirty_page_rate(){
    uint64 curr_dirty_page_queue_capacity = g_instance.ckpt_cxt_ctl->dirty_page_queue_size * PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE - get_dirty_page_num();
    uint64 curr_candidate_slot_capacity = get_curr_candidate_nums(false);
    uint64 curr_timestamp = get_time_ms();

    int64 dirty_page_queue_capacity_change_num = (int64)g_instance.resource_manager_cxt.last_dirty_page_queue_capacity - (int64)curr_dirty_page_queue_capacity;
    int64 candidate_slot_capacity_change_num = (int64)g_instance.resource_manager_cxt.last_candidate_slot_capacity - (int64)curr_candidate_slot_capacity;

    uint64 time_diff = curr_timestamp - g_instance.resource_manager_cxt.last_timestamp_calculate_sleep_time;

    double dirty_page_queue_change_speed = (double)dirty_page_queue_capacity_change_num / ((double)time_diff/1000.0);
    double candidate_slot_change_speed = (double)candidate_slot_capacity_change_num / ((double)time_diff/1000.0);

    double delta_dirty_page_queue_change_speed = dirty_page_queue_change_speed - g_instance.resource_manager_cxt.last_dirty_page_queue_change_speed;
    double delta_candidate_slot_change_speed = candidate_slot_change_speed - g_instance.resource_manager_cxt.last_candidate_slot_change_speed;

    double dirty_page_queue_change_speed_change_speed_differentiation = (double)delta_dirty_page_queue_change_speed / (double)time_diff;
    double candidate_slot_change_speed_change_speed_differentiation = (double)delta_candidate_slot_change_speed / (double)time_diff;

    g_instance.resource_manager_cxt.last_dirty_page_queue_speed_diff = dirty_page_queue_change_speed_change_speed_differentiation;
    g_instance.resource_manager_cxt.last_candidate_slot_speed_diff = candidate_slot_change_speed_change_speed_differentiation;

    if(dirty_page_queue_change_speed_change_speed_differentiation < 0){
        dirty_page_queue_change_speed_change_speed_differentiation = 0;
    }
    if(candidate_slot_change_speed_change_speed_differentiation < 0){
        candidate_slot_change_speed_change_speed_differentiation = 0;
    }

    const double MAX_REMAIN_TIME = 40.0;
    const double MIN_REMIAN_TIME = 1.0;

    double dirty_page_queue_remain_time = (double)curr_dirty_page_queue_capacity / (double)dirty_page_queue_change_speed;
    if(dirty_page_queue_change_speed <= 0){
        dirty_page_queue_remain_time = MAX_REMAIN_TIME;
    }

    if(dirty_page_queue_remain_time > MAX_REMAIN_TIME){
        dirty_page_queue_remain_time = MAX_REMAIN_TIME;
    }else if(dirty_page_queue_remain_time < MIN_REMIAN_TIME){
        dirty_page_queue_remain_time = MIN_REMIAN_TIME;
    }

    g_instance.resource_manager_cxt.time_to_fill_dirty_page_queue = dirty_page_queue_remain_time;

    double sleep_time = 0;
    bool sleep_is_zero = false;

    const double unit_sleep_time = 100; // 100 us

    if((candidate_slot_change_speed == 0 && curr_candidate_slot_capacity == 0) || (dirty_page_queue_remain_time && curr_dirty_page_queue_capacity == 0)){
        sleep_is_zero = true;
        sleep_time = unit_sleep_time;
    }
    double candidate_slot_remain_time = (double) curr_candidate_slot_capacity / (double)candidate_slot_change_speed;

    if(candidate_slot_change_speed <= 0){
        candidate_slot_remain_time = MAX_REMAIN_TIME;
    }
    if(candidate_slot_remain_time > MAX_REMAIN_TIME){
        candidate_slot_remain_time = MAX_REMAIN_TIME;
    }else if(candidate_slot_remain_time < MIN_REMIAN_TIME){
        candidate_slot_remain_time = MIN_REMIAN_TIME;
    }
    g_instance.resource_manager_cxt.time_to_fill_candidate_slot = candidate_slot_remain_time;

    double time_to_fill_buffer_zone = MAX_REMAIN_TIME;
    if(dirty_page_queue_remain_time != 0 &&candidate_slot_remain_time != 0){
        time_to_fill_buffer_zone = MIN(dirty_page_queue_remain_time, candidate_slot_remain_time);
    }else if(dirty_page_queue_remain_time == 0 &&candidate_slot_remain_time == 0){
        time_to_fill_buffer_zone = dirty_page_queue_remain_time + candidate_slot_remain_time;
    }
    time_to_fill_buffer_zone /= MAX_REMAIN_TIME;
    g_instance.resource_manager_cxt.time_to_fill_buffer_zone = time_to_fill_buffer_zone;

    double adapt_period = g_instance.attr.attr_storage.adapt_period; // 1s

    double speed_differentiation = dirty_page_queue_change_speed_change_speed_differentiation;
    if(dirty_page_queue_remain_time == candidate_slot_remain_time){
        speed_differentiation = candidate_slot_change_speed_change_speed_differentiation;
    }

    if(speed_differentiation > 0){
        g_instance.resource_manager_cxt.last_speed_diff += unit_sleep_time;
    }else{
        g_instance.resource_manager_cxt.last_speed_diff = 0;
    }

    if(time_to_fill_buffer_zone < MAX_REMAIN_TIME && !sleep_is_zero){
        sleep_time = unit_sleep_time / time_to_fill_buffer_zone + g_instance.resource_manager_cxt.last_speed_diff;
    }

    g_instance.resource_manager_cxt.last_dirty_page_queue_capacity = curr_dirty_page_queue_capacity;
    g_instance.resource_manager_cxt.last_candidate_slot_capacity = curr_candidate_slot_capacity;

    g_instance.resource_manager_cxt.last_dirty_page_queue_change_speed = dirty_page_queue_change_speed;
    g_instance.resource_manager_cxt.last_candidate_slot_change_speed = candidate_slot_change_speed;

    g_instance.resource_manager_cxt.last_timestamp_calculate_sleep_time = curr_timestamp;

    g_instance.ckpt_cxt_ctl->push_pending_flush_queue_sleep = sleep_time;

    pg_usleep(adapt_period);
}
void resource_manager_main(){
      ereport(LOG,
            (errmodule(MOD_INCRE_CKPT),
                errmsg("resource_manager_main start")));
    
    sigjmp_buf local_sigjmp_buf;
    t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid = BOOTSTRAP_SUPERUSERID;
    g_instance.resource_manager_cxt.resourceManagerBEEntry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "resource manager",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    // t_thrd.utils_cxt.CurrentResourceOwner = resbookkeeper_create(NULL, "resource manager",
    //     MMGR_THREAD_GET_MEN_CTRL_GROUP(MEMORY_CONTEXT_STORAGE));  
    MemoryContext resource_manager_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Resource Manager",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    // MmgrMemoryController resource_manager_context = mmgr_memory_set_controller_create(t_thrd.top_mem_cxt,
    //     "Resource Manager",
    //     MMGR_MEMORYSET_DEFAULT_MINSIZE,
    //     MMGR_MEMORYSET_DEFAULT_INITSIZE
    //     MMGR_MEMORYSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(resource_manager_context);//(void)mmgr_memory_controller_switch_to(resource_manager_context); 

    // ERR_PROC(GAUSS_23202, ERR_LEVEL_LOG,(err_msg(UNDOFORMAT("Resource Manager : started."))));

    (void)gspqsignal(SIGHUP, resource_manager_sigh_up_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);  /* request shutdown */
    (void)gspqsignal(SIGTERM, resource_manager_sigterm_handler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, quick_die);       /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);


    // (void)gs_signal_register_thrd_handler(SIGHUP, resource_manager_sigh_up_handler);
    // (void)gs_signal_register_thrd_handler(SIGINT, SIG_IGN);
    // (void)gs_signal_register_thrd_handler(SIGTERM, resource_manager_sigterm_handler);
    // (void)gs_signal_register_thrd_handler(SIGQUIT, quick_die);
    // (void)gs_signal_register_thrd_handler(SIGALRM, SIG_IGN);

    // (void)gs_signal_register_thrd_handler(SIGPIPE, SIG_IGN);
    // (void)gs_signal_register_thrd_handler(SIGUSR1, SIG_IGN);
    // (void)gs_signal_register_thrd_handler(SIGUSR2, SIG_IGN);
    // (void)gs_signal_register_thrd_handler(SIGURG, start_get_backtrace); //这个没找到对应的

    // (void)gs_signal_register_thrd_handler(SIGCHLD, SIG_DFL);
    // (void)gs_signal_register_thrd_handler(SIGTTIN, SIG_DFL);
    // (void)gs_signal_register_thrd_handler(SIGTTOU, SIG_DFL);
    // (void)gs_signal_register_thrd_handler(SIGCONT, SIG_DFL);
    // (void)gs_signal_register_thrd_handler(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if(sigsetjmp(local_sigjmp_buf, 1) != 0){
        smgrcloseall();
        t_thrd.log_cxt.error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();//err_output_errmsg();
        LWLockReleaseAll();//lwlock_release_all();
        pgstat_report_waitevent(WAIT_EVENT_END);//pgstat_report_waitevent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
        AbortBufferIO();//buffer_abort_io();
        //buffer_unlock_bufhdr_for_exception(); 没找到对应函数
        UnlockBuffers();//buffer_unlock_buffers();

        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);// resbookkeeper_release(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        AtEOXact_Buffers(false);//buffer_at_eo_xact(false);
        AtEOXact_SMgr();//smgr_at_eo_xact();

        // release_resowner_when_error(); 没找到对应函数

        (void)MemoryContextSwitchTo(resource_manager_context);//(void)mmgr_memory_controller_switch_to(resource_manager_context);
        FlushErrorState();//err_flush_data();

        MemoryContextResetAndDeleteChildren(resource_manager_context);//mmgr_memory_controller_reset_and_delete_children(resource_manager_context);

        RESUME_INTERRUPTS();

        pg_usleep(1000000L);

    }
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;// t_thrd.log_cxt.err_except_stack = &local_sigjmp_buf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void) gs_signal_unblock_sigusr2();

    pgstat_bestart();//optutil_gsstat_bestart();
    pgstat_report_appname("resource manager");// optutil_gsstat_report_appname("resource manager");
    pgstat_report_activity(STATE_IDLE, NULL);//optutil_gsstat_report_activity(STATE_IDLE, NULL);

    while (true) {
        if (t_thrd.resource_manager_cxt.got_SIGHUP) {
            t_thrd.resource_manager_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        pgstat_report_activity(STATE_RUNNING, NULL);// optutil_gsstat_report_activity(STATE_RUNNING, NULL);

        if (!RecoveryInProgress()) {
            adapt_create_dirty_page_rate();
            if (t_thrd.resource_manager_cxt.got_SIGTERM) {
                shutdown_resource_manager();
            }
        }
    }
    shutdown_resource_manager();
}

Datum gs_stat_resource_manager(PG_FUNCTION_ARGS)
{
    Buffer buf = DatumGetUInt32(PG_GETARG_DATUM(0));
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tup_desc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)){
        // ERR_PROC(GAUSS_10153, ERR_LEVEL_ERROR,
        //          (err_ecode(ERRCODE_FEATURE_NOT_SUPPORTED),
        //           err_msg("set-valued function called in context that cannot accept a set"),
        //           err_cause("Return set status have no struct to pass back."),
        //           err_action("Contact Huawei engineer to support.")));

        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        // ERR_PROC(GAUSS_10154, ERR_LEVEL_ERROR,
        //          (err_ecode(ERRCODE_FEATURE_NOT_SUPPORTED),
        //           err_msg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tup_desc) != TYPEFUNC_COMPOSITE){
        // ERR_PROC(GAUSS_43310, ERR_LEVEL_ERROR,
        //          (err_msg("return type must be a row type"),
        //           err_cause("Probable internal error."),
        //           err_action("Please restart or contact Huawei technical support.")));

        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);// mmgr_memory_controller_switch_to(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tup_desc;
    (void)MemoryContextSwitchTo(oldcontext);// mmgr_memory_controller_switch_to(oldcontext);

    bool nulls[21] = {false};
    Datum values[21];

    uint64 curr_dirty_page_num = get_dirty_page_num();
    double remain_dirty_queue_ratio = 1 - (double)curr_dirty_page_num /
        ((double)g_instance.ckpt_cxt_ctl->dirty_page_queue_size * PAGE_QUEUE_SLOT_USED_MAX_PERCENTAGE);

    uint32 max_candidate_slots = g_instance.attr.attr_storage.NBuffers;
    uint32 curr_candidate_slots = get_curr_candidate_nums(false);
    double candidate_slot_ratio = (double)curr_candidate_slots / (double)max_candidate_slots;

    // dirty page queue stat
    values[ARR_0] = UInt64GetDatum(g_instance.ckpt_cxt_ctl->dirty_page_queue_size);
    values[ARR_1] = UInt64GetDatum(curr_dirty_page_num);
    values[ARR_2] = Float8GetDatum(remain_dirty_queue_ratio);
    values[ARR_3] = Float8GetDatum(g_instance.resource_manager_cxt.last_dirty_page_queue_change_speed);
    values[ARR_4] = Float8GetDatum(g_instance.resource_manager_cxt.time_to_fill_dirty_page_queue);
    values[ARR_5] = Float8GetDatum(g_instance.resource_manager_cxt.last_dirty_page_queue_speed_diff);

    // candidate slot stat
    values[ARR_6] = UInt32GetDatum(curr_candidate_slots);
    values[ARR_7] = UInt32GetDatum(max_candidate_slots);
    values[ARR_8] = Float8GetDatum(candidate_slot_ratio);
    values[ARR_9] = Float8GetDatum(g_instance.resource_manager_cxt.last_candidate_slot_change_speed);
    values[ARR_10] = Float8GetDatum(g_instance.resource_manager_cxt.time_to_fill_candidate_slot);
    values[ARR_11] = Float8GetDatum(g_instance.resource_manager_cxt.last_candidate_slot_speed_diff);
    values[ARR_12] = Float8GetDatum(g_instance.resource_manager_cxt.time_to_fill_buffer_zone);
    values[ARR_13] = Float8GetDatum(g_instance.resource_manager_cxt.last_speed_diff);
    values[ARR_14] = BoolGetDatum(is_dirty_page_queue_full(GetBufferDescriptor(buf-1)));//values[ARR_14] = BoolGetDatum(is_dirty_page_queue_full(BUFFER_GET_DESCRIPTOR(buf-1)));
    values[ARR_15] = Float8GetDatum(g_instance.ckpt_cxt_ctl->push_pending_flush_queue_sleep);
    values[ARR_16] = UInt32GetDatum(g_instance.resource_manager_cxt.expected_flush_num);
    pg_atomic_write_u64(&g_instance.resource_manager_cxt.buffer_pool_flush_num, 0);
    tuplestore_putvalues(tupstore, tup_desc, values, nulls);
    tuplestore_donestoring(tupstore);
    PG_RETURN_VOID();
}


