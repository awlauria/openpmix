/*
 * Copyright (c) 2015-2018 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <src/include/pmix_config.h>

#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <time.h>

#include <pmix_common.h>

#include "src/mca/base/pmix_mca_base_var.h"
#include "src/include/pmix_socket_errno.h"
#include "src/include/pmix_globals.h"
#include "src/class/pmix_list.h"
#include "src/util/alfg.h"
#include "src/util/argv.h"
#include "src/util/error.h"
#include "src/util/name_fns.h"
#include "src/util/output.h"
#include "src/util/pmix_environ.h"
#include "src/mca/preg/preg.h"

#include "src/mca/pnet/pnet.h"
#include "src/mca/pnet/base/base.h"
#include "pnet_test.h"

static pmix_status_t test_init(void);
static void test_finalize(void);
static pmix_status_t allocate(pmix_nspace_t *nptr,
                              pmix_info_t *info,
                              pmix_list_t *ilist);
static pmix_status_t setup_local_network(pmix_nspace_t *nptr,
                                         pmix_info_t info[],
                                         size_t ninfo);
static pmix_status_t setup_fork(pmix_nspace_t *nptr,
                                const pmix_proc_t *proc,
                                char ***env);
static void child_finalized(pmix_peer_t *peer);
static void local_app_finalized(pmix_nspace_t *nptr);
static void deregister_nspace(pmix_nspace_t *nptr);
static pmix_status_t collect_inventory(pmix_info_t directives[], size_t ndirs,
                                       pmix_info_cbfunc_t cbfunc, void *cbdata);

pmix_pnet_module_t pmix_test_module = {
    .init = test_init,
    .finalize = test_finalize,
    .allocate = allocate,
    .setup_local_network = setup_local_network,
    .setup_fork = setup_fork,
    .child_finalized = child_finalized,
    .local_app_finalized = local_app_finalized,
    .deregister_nspace = deregister_nspace,
    .collect_inventory = collect_inventory
};

static pmix_status_t test_init(void)
{
    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet: test init");
    return PMIX_SUCCESS;
}

static void test_finalize(void)
{
    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet: test finalize");
}

/* NOTE: if there is any binary data to be transferred, then
 * this function MUST pack it for transport as the host will
 * not know how to do so */
static pmix_status_t allocate(pmix_nspace_t *nptr,
                              pmix_info_t *info,
                              pmix_list_t *ilist)
{
    pmix_kval_t *kv;
    bool seckey;
    pmix_list_t mylist;
    size_t n, nreqs=0;
    pmix_info_t *requests = NULL;
    char *idkey = NULL;
    uint64_t unique_key = 12345;
    pmix_buffer_t buf;
    pmix_status_t rc;

    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet:test:allocate for nspace %s", nptr->nspace);

    /* if I am not the gateway, then ignore this call - should never
     * happen, but check to be safe */
    if (!PMIX_PROC_IS_GATEWAY(pmix_globals.mypeer)) {
        return PMIX_SUCCESS;
    }

    /* check directives to see if a crypto key and/or
     * network resource allocations requested */
    PMIX_CONSTRUCT(&mylist, pmix_list_t);
    if (0 == strncmp(info->key, PMIX_SETUP_APP_ENVARS, PMIX_MAX_KEYLEN) ||
        0 == strncmp(info->key, PMIX_SETUP_APP_ALL, PMIX_MAX_KEYLEN)) {
        kv = PMIX_NEW(pmix_kval_t);
        if (NULL == kv) {
            return PMIX_ERR_NOMEM;
        }
        kv->key = strdup(PMIX_SET_ENVAR);
        kv->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
        if (NULL == kv->value) {
            PMIX_RELEASE(kv);
            return PMIX_ERR_NOMEM;
        }
        kv->value->type = PMIX_ENVAR;
        PMIX_ENVAR_LOAD(&kv->value->data.envar, "PMIX_TEST_ENVAR", "1", ':');
        pmix_list_append(ilist, &kv->super);
        return PMIX_SUCCESS;
    } else if (0 != strncmp(info->key, PMIX_ALLOC_NETWORK, PMIX_MAX_KEYLEN)) {
        /* not a network allocation request */
        return PMIX_SUCCESS;
    }

    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet:test:allocate alloc_network for nspace %s",
                        nptr->nspace);

    /* this info key includes an array of pmix_info_t, each providing
     * a key (that is to be used as the key for the allocated ports) and
     * a number of ports to allocate for that key */
    if (PMIX_DATA_ARRAY != info->value.type ||
        NULL == info->value.data.darray ||
        PMIX_INFO != info->value.data.darray->type ||
        NULL == info->value.data.darray->array) {
        /* they made an error */
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        return PMIX_ERR_BAD_PARAM;
    }
    requests = (pmix_info_t*)info->value.data.darray->array;
    nreqs = info->value.data.darray->size;
    /* cycle thru the provided array and get the ID key */
    for (n=0; n < nreqs; n++) {
        if (0 == strncmp(requests[n].key, PMIX_ALLOC_NETWORK_ID, PMIX_MAX_KEYLEN)) {
            /* check for bozo error */
            if (PMIX_STRING != requests[n].value.type ||
                NULL == requests[n].value.data.string) {
                PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
                return PMIX_ERR_BAD_PARAM;
            }
            idkey = requests[n].value.data.string;
        } else if (0 == strncasecmp(requests[n].key, PMIX_ALLOC_NETWORK_SEC_KEY, PMIX_MAX_KEYLEN)) {
               seckey = PMIX_INFO_TRUE(&requests[n]);
           }
       }

    /* we at least require an attribute key for the response */
    if (NULL == idkey) {
        return PMIX_ERR_BAD_PARAM;
    }

    /* must include the idkey */
    kv = PMIX_NEW(pmix_kval_t);
    if (NULL == kv) {
        return PMIX_ERR_NOMEM;
    }
    kv->key = strdup(PMIX_ALLOC_NETWORK_ID);
    kv->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
    if (NULL == kv->value) {
        PMIX_RELEASE(kv);
        return PMIX_ERR_NOMEM;
    }
    kv->value->type = PMIX_STRING;
    kv->value->data.string = strdup(idkey);
    pmix_list_append(&mylist, &kv->super);

    if (seckey) {
        kv = PMIX_NEW(pmix_kval_t);
        if (NULL == kv) {
            return PMIX_ERR_NOMEM;
        }
        kv->key = strdup(PMIX_ALLOC_NETWORK_SEC_KEY);
        kv->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
        if (NULL == kv->value) {
            PMIX_RELEASE(kv);
            return PMIX_ERR_NOMEM;
        }
        kv->value->type = PMIX_BYTE_OBJECT;
        kv->value->data.bo.bytes = (char*)malloc(sizeof(uint64_t));
        if (NULL == kv->value->data.bo.bytes) {
            PMIX_RELEASE(kv);
            return PMIX_ERR_NOMEM;
        }
        memcpy(kv->value->data.bo.bytes, &unique_key, sizeof(uint64_t));
        kv->value->data.bo.size = sizeof(uint64_t);
        pmix_list_append(&mylist, &kv->super);
    }

    n = pmix_list_get_size(&mylist);
    if (0 < n) {
        PMIX_CONSTRUCT(&buf, pmix_buffer_t);
        /* pack the number of kvals for ease on the remote end */
        PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &buf, &n, 1, PMIX_SIZE);
        /* cycle across the list and pack the kvals */
        while (NULL != (kv = (pmix_kval_t*)pmix_list_remove_first(&mylist))) {
            PMIX_BFROPS_PACK(rc, pmix_globals.mypeer, &buf, kv, 1, PMIX_KVAL);
            PMIX_RELEASE(kv);
            if (PMIX_SUCCESS != rc) {
                PMIX_DESTRUCT(&buf);
                PMIX_LIST_DESTRUCT(&mylist);
                return rc;
            }
        }
        PMIX_LIST_DESTRUCT(&mylist);
        kv = PMIX_NEW(pmix_kval_t);
        kv->key = strdup("pmix-pnet-test-blob");
        kv->value = (pmix_value_t*)malloc(sizeof(pmix_value_t));
        if (NULL == kv->value) {
            PMIX_RELEASE(kv);
            PMIX_DESTRUCT(&buf);
            return PMIX_ERR_NOMEM;
        }
        kv->value->type = PMIX_BYTE_OBJECT;
        PMIX_UNLOAD_BUFFER(&buf, kv->value->data.bo.bytes, kv->value->data.bo.size);
        PMIX_DESTRUCT(&buf);
        pmix_list_append(ilist, &kv->super);
    }

    return PMIX_SUCCESS;
}

static pmix_status_t setup_local_network(pmix_nspace_t *nptr,
                                         pmix_info_t info[],
                                         size_t ninfo)
{
    size_t n, m, nkvals;
    char *nodestring, **nodes;
    pmix_proc_t *procs;
    size_t nprocs;
    pmix_buffer_t bkt;
    int32_t cnt;
    pmix_kval_t *kv;
    pmix_status_t rc;
    pmix_info_t *jinfo, stinfo;
    char *idkey = NULL;

    pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                        "pnet:test:setup_local_network");

    /* get the list of nodes in this job - returns a regex */
    pmix_output(0, "pnet:test setup_local_network NSPACE %s", (NULL == nptr) ? "NULL" : nptr->nspace);
    pmix_preg.resolve_nodes(nptr->nspace, &nodestring);
    if (NULL == nodestring) {
        return PMIX_SUCCESS;
    }
    pmix_preg.parse_nodes(nodestring, &nodes);  // get an argv array of node names
    pmix_output(0, "pnet:test setup_local_network NODES %s", (NULL == nodes) ? "NULL" : "NON-NULL");
    if (NULL == nodes) {
        free(nodestring);
        return PMIX_SUCCESS;
    }
    for (n=0; NULL != nodes[n]; n++) {
        pmix_output(0, "pnet:test setup_local_network NODE: %s", nodes[n]);
    }

    for (n=0; NULL != nodes[n]; n++) {
    /* get an array of pmix_proc_t containing the names of the procs on that node */
      pmix_preg.resolve_peers(nodes[n], nptr->nspace, &procs, &nprocs);
      if (NULL == procs) {
        continue;
    }
    for (m=0; m < nprocs; m++) {
        pmix_output(0, "pnet:test setup_local_network NODE %s: peer %s:%d", nodes[n], procs[m].nspace, procs[m].rank);
    }
        /* do stuff */
        free(procs);
    }

    if (NULL != info) {
       for (n=0; n < ninfo; n++) {
               /* look for my key */
           if (0 == strncmp(info[n].key, "pmix-pnet-test-blob", PMIX_MAX_KEYLEN)) {
                   /* this macro NULLs and zero's the incoming bo */
               PMIX_LOAD_BUFFER(pmix_globals.mypeer, &bkt,
                                info[n].value.data.bo.bytes,
                                info[n].value.data.bo.size);
                   /* unpack the number of kvals */
               cnt = 1;
               PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer,
                                  &bkt, &nkvals, &cnt, PMIX_SIZE);
                   /* setup the info array */
               PMIX_INFO_CREATE(jinfo, nkvals);
                   /* cycle thru the blob and extract the kvals */
               kv = PMIX_NEW(pmix_kval_t);
               cnt = 1;
               PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer,
                                  &bkt, kv, &cnt, PMIX_KVAL);
               m = 0;
               while (PMIX_SUCCESS == rc) {
                   pmix_output_verbose(2, pmix_pnet_base_framework.framework_output,
                                       "recvd KEY %s %s", kv->key,
                                       (PMIX_STRING == kv->value->type) ? kv->value->data.string : "NON-STRING");
                       /* xfer the value to the info */
                   (void)strncpy(jinfo[m].key, kv->key, PMIX_MAX_KEYLEN);
                   PMIX_BFROPS_VALUE_XFER(rc, pmix_globals.mypeer,
                                          &jinfo[m].value, kv->value);
                       /* if this is the ID key, save it */
                   if (0 == strncmp(kv->key, PMIX_ALLOC_NETWORK_ID, PMIX_MAX_KEYLEN)) {
                       idkey = strdup(kv->value->data.string);
                   }
                   ++m;
                   PMIX_RELEASE(kv);
                   kv = PMIX_NEW(pmix_kval_t);
                   cnt = 1;
                   PMIX_BFROPS_UNPACK(rc, pmix_globals.mypeer,
                                      &bkt, kv, &cnt, PMIX_KVAL);
               }
                   /* restore the incoming data */
               info[n].value.data.bo.bytes = bkt.base_ptr;
               info[n].value.data.bo.size = bkt.bytes_used;
               bkt.base_ptr = NULL;
               bkt.bytes_used = 0;

                   /* if they didn't include a network ID, then this is an error */
               if (NULL == idkey) {
                   PMIX_INFO_FREE(jinfo, nkvals);
                   return PMIX_ERR_BAD_PARAM;
               }
                   /* the data gets stored as a pmix_data_array_t on the provided key */
               PMIX_INFO_CONSTRUCT(&stinfo);
               (void)strncpy(stinfo.key, idkey, PMIX_MAX_KEYLEN);
               stinfo.value.type = PMIX_DATA_ARRAY;
               PMIX_DATA_ARRAY_CREATE(stinfo.value.data.darray, nkvals, PMIX_INFO);
               stinfo.value.data.darray->array = jinfo;

                   /* cache the info on the job */
               PMIX_GDS_CACHE_JOB_INFO(rc, pmix_globals.mypeer, nptr,
                                       &stinfo, 1);
               PMIX_INFO_DESTRUCT(&stinfo);
           }
       }
    }

    return PMIX_SUCCESS;
}

static pmix_status_t setup_fork(pmix_nspace_t *nptr,
                                const pmix_proc_t *proc,
                                char ***env)
{
    pmix_cb_t cb;
    pmix_status_t rc;
    pmix_kval_t *kv;
    uint16_t localrank;

    PMIX_CONSTRUCT(&cb, pmix_cb_t);

    cb.key = strdup(PMIX_LOCAL_RANK);
    /* this data isn't going anywhere, so we don't require a copy */
    cb.copy = false;
    /* scope is irrelevant as the info we seek must be local */
    cb.scope = PMIX_SCOPE_UNDEF;
    /* ask for the value for the given proc */
    cb.proc = (pmix_proc_t*)proc;

    PMIX_GDS_FETCH_KV(rc, pmix_globals.mypeer, &cb);
    if (PMIX_SUCCESS != rc) {
        if (PMIX_ERR_INVALID_NAMESPACE != rc) {
            PMIX_ERROR_LOG(rc);
        }
        PMIX_DESTRUCT(&cb);
        return rc;
    }
    /* should just be the one value on the list */
    if (1 != pmix_list_get_size(&cb.kvs)) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        PMIX_DESTRUCT(&cb);
        return PMIX_ERR_BAD_PARAM;
    }
    kv = (pmix_kval_t*)pmix_list_get_first(&cb.kvs);
    if (PMIX_UINT16 != kv->value->type) {
        PMIX_ERROR_LOG(PMIX_ERR_BAD_PARAM);
        PMIX_DESTRUCT(&cb);
        return PMIX_ERR_BAD_PARAM;
    }
    localrank = kv->value->data.uint16;

    pmix_output(0, "pnet:test LOCAL RANK FOR PROC %s: %d", PMIX_NAME_PRINT(proc), (int)localrank);

    PMIX_DESTRUCT(&cb);
    return PMIX_SUCCESS;
}

static void child_finalized(pmix_peer_t *peer)
{
    pmix_output(0, "pnet:test CHILD %s:%d FINALIZED",
                peer->info->pname.nspace, peer->info->pname.rank);
}

static void local_app_finalized(pmix_nspace_t *nptr)
{
    pmix_output(0, "pnet:test NSPACE %s LOCALLY FINALIZED", nptr->nspace);
}

static void deregister_nspace(pmix_nspace_t *nptr)
{
    pmix_output(0, "pnet:test DEREGISTER NSPACE %s", nptr->nspace);
}

static pmix_status_t collect_inventory(pmix_info_t directives[], size_t ndirs,
                                       pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    pmix_output(0, "pnet:test COLLECT INVENTORY");
    return PMIX_ERR_NOT_SUPPORTED;
}