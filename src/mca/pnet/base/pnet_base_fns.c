/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2020 Intel, Inc.  All rights reserved.
 * Copyright (c) 2016      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "src/include/pmix_config.h"

#include "include/pmix_common.h"
#include "src/include/pmix_globals.h"

#include "src/class/pmix_list.h"
#include "src/mca/preg/preg.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/pmix_environ.h"
#include "src/server/pmix_server_ops.h"

#include "src/mca/pnet/base/base.h"


static pmix_status_t process_maps(char *nspace, char **nodes, char **procs);

/* NOTE: a tool (e.g., prun) may call this function to
 * harvest local envars for inclusion in a call to
 * PMIx_Spawn, or it might be called in response to
 * a call to PMIx_Allocate_resources */
pmix_status_t pmix_pnet_base_allocate(char *nspace,
                                      pmix_info_t info[], size_t ninfo,
                                      pmix_list_t *ilist)
{
    pmix_pnet_base_active_module_t *active;
    pmix_status_t rc;
    pmix_namespace_t *nptr, *ns;
    size_t n;
    char **nodes, **procs;

    if (!pmix_pnet_globals.initialized) {
        return PMIX_ERR_INIT;
    }

    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet:allocate called");

    /* protect against bozo inputs */
    if (NULL == nspace || NULL == ilist) {
        return PMIX_ERR_BAD_PARAM;
    }
    if (PMIX_PEER_IS_SCHEDULER(pmix_globals.mypeer)) {
        nptr = NULL;
        /* find this nspace - note that it may not have
         * been registered yet */
        PMIX_LIST_FOREACH(ns, &pmix_globals.nspaces, pmix_namespace_t) {
            if (0 == strcmp(ns->nspace, nspace)) {
                nptr = ns;
                break;
            }
        }
        if (NULL == nptr) {
            /* add it */
            nptr = PMIX_NEW(pmix_namespace_t);
            if (NULL == nptr) {
                return PMIX_ERR_NOMEM;
            }
            nptr->nspace = strdup(nspace);
            pmix_list_append(&pmix_globals.nspaces, &nptr->super);
        }

        if (NULL != info) {
            /* check for description of the node and proc maps */
            nodes = NULL;
            procs = NULL;
            for (n=0; n < ninfo; n++) {
                if (PMIX_CHECK_KEY(&info[n], PMIX_NODE_MAP)) {
                    rc = pmix_preg.parse_nodes(info[n].value.data.bo.bytes, &nodes);
                    if (PMIX_SUCCESS != rc) {
                        return rc;
                    }
                } else if (PMIX_CHECK_KEY(&info[n], PMIX_PROC_MAP)) {
                    rc = pmix_preg.parse_procs(info[n].value.data.bo.bytes, &procs);
                    if (PMIX_SUCCESS != rc) {
                        return rc;
                    }
                }
            }
            if (NULL != nodes && NULL != procs) {
                /* assemble the pnet node and proc descriptions
                 * NOTE: this will eventually be folded into the
                 * new shared memory system, but we do it here
                 * as the pnet plugins need the information and
                 * the host will not have registered the clients
                 * and nspace prior to calling allocate
                 */
                rc = process_maps(nspace, nodes, procs);
                pmix_argv_free(nodes);
                pmix_argv_free(procs);
                if (PMIX_SUCCESS != rc) {
                    return rc;
                }
            }
            /* process the allocation request */
            PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
                if (NULL != active->module->allocate) {
                    if (PMIX_SUCCESS == (rc = active->module->allocate(nptr, info, ninfo, ilist))) {
                        break;
                    }
                    if (PMIX_ERR_TAKE_NEXT_OPTION != rc) {
                        /* true error */
                        return rc;
                    }
                }
            }
        }
    }

    return PMIX_SUCCESS;
}

/* can only be called by a server */
pmix_status_t pmix_pnet_base_setup_local_network(char *nspace,
                                                 pmix_info_t info[],
                                                 size_t ninfo)
{
    pmix_pnet_base_active_module_t *active;
    pmix_status_t rc;
    pmix_namespace_t *nptr, *ns;

    if (!pmix_pnet_globals.initialized) {
        return PMIX_ERR_INIT;
    }

    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet: setup_local_network called");

    /* protect against bozo inputs */
    if (NULL == nspace) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* find this proc's nspace object */
    nptr = NULL;
    PMIX_LIST_FOREACH(ns, &pmix_globals.nspaces, pmix_namespace_t) {
        if (0 == strcmp(ns->nspace, nspace)) {
            nptr = ns;
            break;
        }
    }
    if (NULL == nptr) {
        /* add it */
        nptr = PMIX_NEW(pmix_namespace_t);
        if (NULL == nptr) {
            return PMIX_ERR_NOMEM;
        }
        nptr->nspace = strdup(nspace);
        pmix_list_append(&pmix_globals.nspaces, &nptr->super);
    }

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->setup_local_network) {
            if (PMIX_SUCCESS != (rc = active->module->setup_local_network(nptr, info, ninfo))) {
                return rc;
            }
        }
    }

    return PMIX_SUCCESS;
}

/* can only be called by a server */
pmix_status_t pmix_pnet_base_setup_fork(const pmix_proc_t *proc, char ***env)
{
    pmix_pnet_base_active_module_t *active;
    pmix_status_t rc;
    pmix_namespace_t *nptr, *ns;

    if (!pmix_pnet_globals.initialized) {
        return PMIX_ERR_INIT;
    }

    /* protect against bozo inputs */
    if (NULL == proc || NULL == env) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* find this proc's nspace object */
    nptr = NULL;
    PMIX_LIST_FOREACH(ns, &pmix_globals.nspaces, pmix_namespace_t) {
        if (0 == strcmp(ns->nspace, proc->nspace)) {
            nptr = ns;
            break;
        }
    }
    if (NULL == nptr) {
        /* add it */
        nptr = PMIX_NEW(pmix_namespace_t);
        if (NULL == nptr) {
            return PMIX_ERR_NOMEM;
        }
        nptr->nspace = strdup(proc->nspace);
        pmix_list_append(&pmix_globals.nspaces, &nptr->super);
    }

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->setup_fork) {
            rc = active->module->setup_fork(nptr, proc, env);
            if (PMIX_SUCCESS != rc && PMIX_ERR_NOT_AVAILABLE != rc) {
                return rc;
            }
        }
    }

    return PMIX_SUCCESS;
}

void pmix_pnet_base_child_finalized(pmix_proc_t *peer)
{
    pmix_pnet_base_active_module_t *active;

    if (!pmix_pnet_globals.initialized) {
        return;
    }

    /* protect against bozo inputs */
    if (NULL == peer) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return;
    }

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->child_finalized) {
            active->module->child_finalized(peer);
        }
    }

    return;
}

void pmix_pnet_base_local_app_finalized(pmix_namespace_t *nptr)
{
    pmix_pnet_base_active_module_t *active;

    if (!pmix_pnet_globals.initialized) {
        return;
    }

    /* protect against bozo inputs */
    if (NULL == nptr) {
        return;
    }

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->local_app_finalized) {
            active->module->local_app_finalized(nptr);
        }
    }

    return;
}

void pmix_pnet_base_deregister_nspace(char *nspace)
{
    pmix_pnet_base_active_module_t *active;
    pmix_namespace_t *nptr, *ns;
    pmix_pnet_job_t *job;
    pmix_pnet_node_t *node;

    if (!pmix_pnet_globals.initialized) {
        return;
    }

    /* protect against bozo inputs */
    if (NULL == nspace) {
        return;
    }

    /* find this nspace object */
    nptr = NULL;
    PMIX_LIST_FOREACH(ns, &pmix_globals.nspaces, pmix_namespace_t) {
        if (0 == strcmp(ns->nspace, nspace)) {
            nptr = ns;
            break;
        }
    }
    if (NULL == nptr) {
        /* nothing we can do */
        return;
    }

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->deregister_nspace) {
            active->module->deregister_nspace(nptr);
        }
    }

    PMIX_LIST_FOREACH(job, &pmix_pnet_globals.jobs, pmix_pnet_job_t) {
        if (0 == strcmp(nspace, job->nspace)) {
            pmix_list_remove_item(&pmix_pnet_globals.jobs, &job->super);
            PMIX_RELEASE(job);
            break;
        }
    }

    PMIX_LIST_FOREACH(node, &pmix_pnet_globals.nodes, pmix_pnet_node_t) {
        pmix_pnet_local_procs_t *lp;
        PMIX_LIST_FOREACH(lp, &node->local_jobs, pmix_pnet_local_procs_t) {
            if (0 == strcmp(nspace, lp->nspace)) {
                pmix_list_remove_item(&node->local_jobs, &lp->super);
                PMIX_RELEASE(lp);
                break;
            }
        }
    }
}

static void cicbfunc(pmix_status_t status,
                     pmix_list_t *inventory,
                     void *cbdata)
{
    pmix_inventory_rollup_t *rollup = (pmix_inventory_rollup_t*)cbdata;
    pmix_kval_t *kv;

    PMIX_ACQUIRE_THREAD(&rollup->lock);
    /* check if they had an error */
    if (PMIX_SUCCESS != status && PMIX_SUCCESS == rollup->status) {
        rollup->status = status;
    }
    /* transfer the inventory */
    if (NULL != inventory) {
        while (NULL != (kv = (pmix_kval_t*)pmix_list_remove_first(inventory))) {
            pmix_list_append(&rollup->payload, &kv->super);
        }
    }
    /* record that we got a reply */
    rollup->replies++;
    /* see if all have replied */
    if (rollup->replies < rollup->requests) {
        /* nope - need to wait */
        PMIX_RELEASE_THREAD(&rollup->lock);
        return;
    }

    /* if we get here, then collection is complete */
    PMIX_RELEASE_THREAD(&rollup->lock);
    if (NULL != rollup->cbfunc) {
        rollup->cbfunc(rollup->status, &rollup->payload, rollup->cbdata);
    }
    PMIX_RELEASE(rollup);
    return;
}

void pmix_pnet_base_collect_inventory(pmix_info_t directives[], size_t ndirs,
                                      pmix_inventory_cbfunc_t cbfunc, void *cbdata)
{
    pmix_pnet_base_active_module_t *active;
    pmix_inventory_rollup_t *myrollup;
    pmix_status_t rc;

    /* we cannot block here as each plugin could take some time to
     * complete the request. So instead, we call each active plugin
     * and get their immediate response - if "in progress", then
     * we record that we have to wait for their answer before providing
     * the caller with a response. If "error", then we know we
     * won't be getting a response from them */

    if (!pmix_pnet_globals.initialized) {
        /* need to call them back so they know */
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERR_INIT, NULL, cbdata);
        }
        return;
    }
    /* create the rollup object */
    myrollup = PMIX_NEW(pmix_inventory_rollup_t);
    if (NULL == myrollup) {
        /* need to call them back so they know */
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERR_NOMEM, NULL, cbdata);
        }
        return;
    }
    myrollup->cbfunc = cbfunc;
    myrollup->cbdata = cbdata;

    /* hold the lock until all active modules have been called
     * to avoid race condition where replies come in before
     * the requests counter has been fully updated */
    PMIX_ACQUIRE_THREAD(&myrollup->lock);

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->collect_inventory) {
            pmix_output_verbose(5, pmix_pnet_base_framework.framework_output,
                                "COLLECTING %s", active->module->name);
            rc = active->module->collect_inventory(directives, ndirs, cicbfunc, (void*)myrollup);
            /* if they return success, then the values were
             * placed directly on the payload - nothing
             * to wait for here */
            if (PMIX_OPERATION_IN_PROGRESS == rc) {
                myrollup->requests++;
            } else if (PMIX_SUCCESS != rc &&
                       PMIX_ERR_TAKE_NEXT_OPTION != rc &&
                       PMIX_ERR_NOT_SUPPORTED != rc) {
                /* a true error - we need to wait for
                 * all pending requests to complete
                 * and then notify the caller of the error */
                if (PMIX_SUCCESS == myrollup->status) {
                    myrollup->status = rc;
                }
            }
        }
    }
    if (0 == myrollup->requests) {
        /* report back */
        PMIX_RELEASE_THREAD(&myrollup->lock);
        if (NULL != cbfunc) {
            cbfunc(myrollup->status, &myrollup->payload, cbdata);
        }
        PMIX_RELEASE(myrollup);
        return;
    }

    PMIX_RELEASE_THREAD(&myrollup->lock);
    return;
}

static void dlcbfunc(pmix_status_t status,
                     void *cbdata)
{
    pmix_inventory_rollup_t *rollup = (pmix_inventory_rollup_t*)cbdata;

    PMIX_ACQUIRE_THREAD(&rollup->lock);
    /* check if they had an error */
    if (PMIX_SUCCESS != status && PMIX_SUCCESS == rollup->status) {
        rollup->status = status;
    }
    /* record that we got a reply */
    rollup->replies++;
    /* see if all have replied */
    if (rollup->replies < rollup->requests) {
        /* nope - need to wait */
        PMIX_RELEASE_THREAD(&rollup->lock);
        return;
    }

    /* if we get here, then delivery is complete */
    PMIX_RELEASE_THREAD(&rollup->lock);
    if (NULL != rollup->opcbfunc) {
        rollup->opcbfunc(rollup->status, rollup->cbdata);
    }
    PMIX_RELEASE(rollup);
    return;
}

void pmix_pnet_base_deliver_inventory(pmix_info_t info[], size_t ninfo,
                                      pmix_info_t directives[], size_t ndirs,
                                      pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    pmix_pnet_base_active_module_t *active;
    pmix_inventory_rollup_t *myrollup;
    pmix_status_t rc;

    /* we cannot block here as each plugin could take some time to
     * complete the request. So instead, we call each active plugin
     * and get their immediate response - if "in progress", then
     * we record that we have to wait for their answer before providing
     * the caller with a response. If "error", then we know we
     * won't be getting a response from them */

    if (!pmix_pnet_globals.initialized) {
        /* need to call them back so they know */
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERR_INIT, cbdata);
        }
        return;
    }
    /* create the rollup object */
    myrollup = PMIX_NEW(pmix_inventory_rollup_t);
    if (NULL == myrollup) {
        /* need to call them back so they know */
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERR_NOMEM, cbdata);
        }
        return;
    }
    myrollup->opcbfunc = cbfunc;
    myrollup->cbdata = cbdata;

    /* hold the lock until all active modules have been called
     * to avoid race condition where replies come in before
     * the requests counter has been fully updated */
    PMIX_ACQUIRE_THREAD(&myrollup->lock);

    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->deliver_inventory) {
            pmix_output_verbose(5, pmix_pnet_base_framework.framework_output,
                                "DELIVERING TO %s", active->module->name);
            rc = active->module->deliver_inventory(info, ninfo, directives, ndirs, dlcbfunc, (void*)myrollup);
            /* if they return success, then the values were
             * immediately archived - nothing to wait for here */
            if (PMIX_OPERATION_IN_PROGRESS == rc) {
                myrollup->requests++;
            } else if (PMIX_SUCCESS != rc &&
                       PMIX_ERR_TAKE_NEXT_OPTION != rc &&
                       PMIX_ERR_NOT_SUPPORTED != rc) {
                /* a true error - we need to wait for
                 * all pending requests to complete
                 * and then notify the caller of the error */
                if (PMIX_SUCCESS == myrollup->status) {
                    myrollup->status = rc;
                }
            }
        }
    }
    if (0 == myrollup->requests) {
        /* report back */
        PMIX_RELEASE_THREAD(&myrollup->lock);
        if (NULL != cbfunc) {
            cbfunc(myrollup->status, cbdata);
        }
        PMIX_RELEASE(myrollup);
        return;
    }

    PMIX_RELEASE_THREAD(&myrollup->lock);
    return;
}

pmix_status_t pmix_pnet_base_register_fabric(pmix_fabric_t *fabric,
                                             const pmix_info_t directives[],
                                             size_t ndirs)
{
    pmix_pnet_base_active_module_t *active;
    pmix_status_t rc;
    pmix_pnet_fabric_t *ft;

    /* ensure our fields of the fabric object are initialized */
    fabric->info = NULL;
    fabric->ninfo = 0;
    fabric->module = NULL;

    PMIX_ACQUIRE_THREAD(&pmix_pnet_globals.lock);

    if (0 == pmix_list_get_size(&pmix_pnet_globals.actives)) {
        PMIX_RELEASE_THREAD(&pmix_pnet_globals.lock);
        return PMIX_ERR_NOT_SUPPORTED;
    }

    /* scan across active modules until one returns success */
    PMIX_LIST_FOREACH(active, &pmix_pnet_globals.actives, pmix_pnet_base_active_module_t) {
        if (NULL != active->module->register_fabric) {
            rc = active->module->register_fabric(fabric, directives, ndirs);
            if (PMIX_SUCCESS == rc) {
                /* track this fabric so we can respond to remote requests */
                ft = PMIX_NEW(pmix_pnet_fabric_t);
                ft->index = fabric->index;
                if (NULL != fabric->name) {
                    ft->name = strdup(fabric->name);
                }
                ft->module = fabric->module;
                pmix_list_append(&pmix_pnet_globals.fabrics, &ft->super);
            } else if (PMIX_ERR_TAKE_NEXT_OPTION != rc) {
                /* just return the error */
                PMIX_RELEASE_THREAD(&pmix_pnet_globals.lock);
                return rc;
            }
        }
    }

    /* unlock prior to return */
    PMIX_RELEASE_THREAD(&pmix_pnet_globals.lock);

    return PMIX_ERR_NOT_FOUND;
}

pmix_status_t pmix_pnet_base_update_fabric(pmix_fabric_t *fabric)
{
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_pnet_fabric_t *active = NULL;
    pmix_pnet_module_t *module;
    pmix_pnet_fabric_t *ft;

    /* protect against bozo input */
    if (NULL == fabric) {
        return PMIX_ERR_BAD_PARAM;
    } else if (NULL == fabric->module) {
        /* this might be a remote request, so look at the
         * list of fabrics we have registered locally and
         * see if we have one with the matching index */
        PMIX_LIST_FOREACH(ft, &pmix_pnet_globals.fabrics, pmix_pnet_fabric_t) {
            if (fabric->index == ft->index) {
                active = fabric->module;
            } else if (NULL != fabric->name && NULL != ft->name
                       && 0 == strcmp(ft->name, fabric->name)) {
                active = fabric->module;
            }
        }
        if (NULL == active) {
            return PMIX_ERR_BAD_PARAM;
        }
    } else {
        active = (pmix_pnet_fabric_t*)fabric->module;
    }
    module = (pmix_pnet_module_t*)active->module;

    if (NULL != module->update_fabric) {
        rc = module->update_fabric(fabric);
    }
    return rc;
}

pmix_status_t pmix_pnet_base_deregister_fabric(pmix_fabric_t *fabric)
{
    pmix_status_t rc = PMIX_SUCCESS;
    pmix_pnet_fabric_t *active = NULL;
    pmix_pnet_module_t *module;
    pmix_pnet_fabric_t *ft;

    /* protect against bozo input */
    if (NULL == fabric) {
        return PMIX_ERR_BAD_PARAM;
    } else if (NULL == fabric->module) {
        /* this might be a remote request, so look at the
         * list of fabrics we have registered locally and
         * see if we have one with the matching index */
        PMIX_LIST_FOREACH(ft, &pmix_pnet_globals.fabrics, pmix_pnet_fabric_t) {
            if (fabric->index == ft->index) {
                active = fabric->module;
            } else if (NULL != fabric->name && NULL != ft->name
                       && 0 == strcmp(ft->name, fabric->name)) {
                active = fabric->module;
            }
        }
        if (NULL == active) {
            return PMIX_ERR_BAD_PARAM;
        }
    } else {
        active = (pmix_pnet_fabric_t*)fabric->module;
    }
    module = (pmix_pnet_module_t*)active->module;

    if (NULL != module->deregister_fabric) {
        rc = module->deregister_fabric(fabric);
    }
    return rc;
}

pmix_status_t pmix_pnet_base_get_vertex_info(pmix_fabric_t *fabric,
                                             uint32_t i,
                                             pmix_info_t **info, size_t *ninfo)
{
    pmix_status_t ret;
    pmix_pnet_fabric_t *active = NULL;
    pmix_pnet_module_t *module;
    pmix_pnet_fabric_t *ft;

    /* protect against bozo input */
    if (NULL == fabric) {
        return PMIX_ERR_BAD_PARAM;
    } else if (NULL == fabric->module) {
        /* this might be a remote request, so look at the
         * list of fabrics we have registered locally and
         * see if we have one with the matching index */
        PMIX_LIST_FOREACH(ft, &pmix_pnet_globals.fabrics, pmix_pnet_fabric_t) {
            if (fabric->index == ft->index) {
                active = fabric->module;
            } else if (NULL != fabric->name && NULL != ft->name
                       && 0 == strcmp(ft->name, fabric->name)) {
                active = fabric->module;
            }
        }
        if (NULL == active) {
            return PMIX_ERR_BAD_PARAM;
        }
    } else {
        active = (pmix_pnet_fabric_t*)fabric->module;
    }
    module = (pmix_pnet_module_t*)ft->module;

    if (NULL == module->get_vertex_info) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    ret = module->get_vertex_info(fabric, i, info, ninfo);

    return ret;
}

pmix_status_t pmix_pnet_base_get_device_index(pmix_fabric_t *fabric,
                                              const pmix_info_t vertex[], size_t ninfo,
                                              uint32_t *i)
{
    pmix_status_t ret;
    pmix_pnet_fabric_t *active = NULL;
    pmix_pnet_module_t *module;
    pmix_pnet_fabric_t *ft;

    /* protect against bozo input */
    if (NULL == fabric) {
        return PMIX_ERR_BAD_PARAM;
    } else if (NULL == fabric->module) {
        /* this might be a remote request, so look at the
         * list of fabrics we have registered locally and
         * see if we have one with the matching index */
        PMIX_LIST_FOREACH(ft, &pmix_pnet_globals.fabrics, pmix_pnet_fabric_t) {
            if (fabric->index == ft->index) {
                active = fabric->module;
            } else if (NULL != fabric->name && NULL != ft->name
                       && 0 == strcmp(ft->name, fabric->name)) {
                active = fabric->module;
            }
        }
        if (NULL == active) {
            return PMIX_ERR_BAD_PARAM;
        }
    } else {
        active = (pmix_pnet_fabric_t*)fabric->module;
    }
    module = (pmix_pnet_module_t*)ft->module;

    if (NULL == module->get_device_index) {
        return PMIX_ERR_NOT_SUPPORTED;
    }

    ret = module->get_device_index(fabric, vertex, ninfo, i);

    return ret;
}

static pmix_status_t process_maps(char *nspace, char **nodes, char **procs)
{
    char **ranks;
    pmix_status_t rc;
    size_t m, n;
    pmix_pnet_job_t *jptr, *job;
    pmix_pnet_node_t *nd, *ndptr;
    pmix_pnet_local_procs_t *lp;
    bool needcheck;

    PMIX_ACQUIRE_THREAD(&pmix_pnet_globals.lock);

    /* bozo check */
    if (pmix_argv_count(nodes) != pmix_argv_count(procs)) {
        rc = PMIX_ERR_BAD_PARAM;
        PMIX_ERROR_LOG(rc);
        PMIX_RELEASE_THREAD(&pmix_pnet_globals.lock);
        return rc;
    }

    /* see if we already know about this job */
    job = NULL;
    if (0 < pmix_list_get_size(&pmix_pnet_globals.jobs)) {
        PMIX_LIST_FOREACH(jptr, &pmix_pnet_globals.jobs, pmix_pnet_job_t) {
            if (0 == strcmp(nspace, jptr->nspace)) {
                job = jptr;
                break;
            }
        }
    }
    if (NULL == job) {
        job = PMIX_NEW(pmix_pnet_job_t);
        job->nspace = strdup(nspace);
        pmix_list_append(&pmix_pnet_globals.jobs, &job->super);
    }

    if (0 < pmix_list_get_size(&pmix_pnet_globals.nodes)) {
        needcheck = true;
    } else {
        needcheck = false;
    }
    for (n=0; NULL != nodes[n]; n++) {
        if (needcheck) {
            /* check and see if we already have data for this node */
            nd = NULL;
            PMIX_LIST_FOREACH(ndptr, &pmix_pnet_globals.nodes, pmix_pnet_node_t) {
                if (0 == strcmp(nodes[n], ndptr->name)) {
                    nd = ndptr;
                    break;
                }
            }
            if (NULL == nd) {
                nd = PMIX_NEW(pmix_pnet_node_t);
                nd->name = strdup(nodes[n]);
                pmix_list_append(&pmix_pnet_globals.nodes, &nd->super);
                /* add this node to the job */
                PMIX_RETAIN(nd);
                nd->index = pmix_pointer_array_add(&job->nodes, nd);
            }
        } else {
            nd = PMIX_NEW(pmix_pnet_node_t);
            nd->name = strdup(nodes[n]);
            pmix_list_append(&pmix_pnet_globals.nodes, &nd->super);
            /* add this node to the job */
            PMIX_RETAIN(nd);
            nd->index = pmix_pointer_array_add(&job->nodes, nd);
        }
        /* check and see if we already have this job on this node */
        PMIX_LIST_FOREACH(lp, &nd->local_jobs, pmix_pnet_local_procs_t) {
            if (0 == strcmp(nspace, lp->nspace)) {
                /* we assume that the input replaces the prior
                 * list of ranks */
                pmix_list_remove_item(&nd->local_jobs, &lp->super);
                PMIX_RELEASE(lp);
                break;
            }
        }
        /* track the local procs */
        lp = PMIX_NEW(pmix_pnet_local_procs_t);
        lp->nspace = strdup(nspace);
        /* separate out the procs - they are a comma-delimited list
         * of rank values */
        ranks = pmix_argv_split(procs[n], ',');
        lp->np = pmix_argv_count(ranks);
        lp->ranks = (pmix_rank_t*)malloc(lp->np * sizeof(pmix_rank_t));
        for (m=0; m < lp->np; m++) {
            lp->ranks[m] = strtoul(ranks[m], NULL, 10);
        }
        pmix_list_append(&nd->local_jobs, &lp->super);
        pmix_argv_free(ranks);
    }

    PMIX_RELEASE_THREAD(&pmix_pnet_globals.lock);
    return PMIX_SUCCESS;
}
