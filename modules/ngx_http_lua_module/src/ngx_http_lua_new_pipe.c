
#ifndef DDEBUG
#define DDEBUG 0
#endif
#include "ddebug.h"


#include "ngx_http_lua_util.h"
#include "ngx_http_lua_new_pipe.h"
#include "ngx_http_lua_contentby.h"
#include <ngx_channel.h>


static int ngx_http_lua_ngx_pipe(lua_State *L);
static void ngx_http_lua_pipe_handler(ngx_event_t *ev);
static ngx_int_t ngx_http_lua_pipe_resume(ngx_http_request_t *r);


typedef struct ngx_http_pipe_ctx_t {
  ngx_http_request_t *r;
  int fd[2];
  int8_t rv;  // return value from notify_fd
} ngx_http_pipe_ctx_t;

static int
ngx_http_lua_ngx_pipe(lua_State *L)
{
    int                          n;
    ngx_http_request_t          *r;
    ngx_http_lua_ctx_t          *ctx;
    ngx_http_pipe_ctx_t         *pctx;
    ngx_http_lua_co_ctx_t       *coctx;
    ngx_connection_t            *dummy_c;

    dd("enter lua ngx pipe: L:%p", L);

    n = lua_gettop(L);
    if (n != 0) {
        return luaL_error(L, "attempt to pass %d arguments, but accepted 0", n);
    }

    r = ngx_http_lua_get_req(L);
    if (r == NULL) {
        return luaL_error(L, "no request found");
    }

    pctx = ngx_palloc(r->pool, sizeof(ngx_http_pipe_ctx_t));
    if (pctx == NULL) {
        return luaL_error(L, "alloc pctx failed");
    }
    pctx->r = r;

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pctx->fd) == -1){
        return luaL_error(L, "socketpair() failed");
    }

    if (ngx_nonblocking(pctx->fd[0]) == -1) {
        ngx_close_channel(pctx->fd, ngx_cycle->log);
        return luaL_error(L, "ngx_nonblocking() failed");
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_lua_module);
    if (ctx == NULL) {
        return luaL_error(L, "no request ctx found");
    }

    ngx_http_lua_check_context(L, ctx, NGX_HTTP_LUA_CONTEXT_REWRITE
                               | NGX_HTTP_LUA_CONTEXT_ACCESS
                               | NGX_HTTP_LUA_CONTEXT_CONTENT
                               | NGX_HTTP_LUA_CONTEXT_TIMER);

    coctx = ctx->cur_co_ctx;
    if (coctx == NULL) {
        return luaL_error(L, "no co ctx found");
    }

    dummy_c = ngx_get_connection(pctx->fd[0], ngx_cycle->log);

    dummy_c->data = coctx;
    dummy_c->read->handler = ngx_http_lua_pipe_handler;
    dummy_c->read->log = ngx_cycle->log;
    dummy_c->write->log = ngx_cycle->log;

    coctx->data = pctx;

    if (ngx_add_event(dummy_c->read, NGX_READ_EVENT, 0)
        == NGX_ERROR)
    {
        ngx_free_connection(dummy_c);
        return luaL_error(L, "add read event failed");
    }

    coctx->cleanup = NULL;

    /* push the return value notify_fd */
    lua_pushinteger(L, pctx->fd[1]);

    return 1;
}


void
ngx_http_lua_pipe_handler(ngx_event_t *ev)
{
    ngx_connection_t        *c;
    ngx_http_request_t      *r;
    ngx_http_lua_ctx_t      *ctx;
    ngx_http_log_ctx_t      *log_ctx;
    ngx_http_pipe_ctx_t     *pctx;
    ngx_http_lua_co_ctx_t   *coctx;
    ngx_connection_t        *dummy_c;

    dummy_c = ev->data;
    coctx = dummy_c->data;

    pctx = coctx->data;
    r = pctx->r;
    c = r->connection;

    ctx = ngx_http_get_module_ctx(r, ngx_http_lua_module);

    if (ctx == NULL) {
        return;
    }

    log_ctx = c->log->data;
    log_ctx->current_request = r;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "lua pipe event handler: \"%V?%V\"", &r->uri, &r->args);

    read(pctx->fd[0], &pctx->rv, 1);

    if (ngx_handle_read_event(ev, 0) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "lua pipe handle read event failed");
        return;
    }

    ngx_close_connection(dummy_c);
    close(pctx->fd[1]);

    ctx->cur_co_ctx = coctx;

    if (ctx->entered_content_phase) {
        (void) ngx_http_lua_pipe_resume(r);

    } else {
        ctx->resume_handler = ngx_http_lua_pipe_resume;
        ngx_http_core_run_phases(r);
    }

    ngx_http_run_posted_requests(c);
}


void
ngx_http_lua_inject_pipe_api(lua_State *L)
{
    lua_pushcfunction(L, ngx_http_lua_ngx_pipe);
    lua_setfield(L, -2, "pipe");
}


static ngx_int_t
ngx_http_lua_pipe_resume(ngx_http_request_t *r)
{
    ngx_int_t                    rc;
    ngx_connection_t            *c;
    ngx_http_lua_ctx_t          *ctx;
    ngx_http_pipe_ctx_t         *pctx;
    ngx_http_lua_co_ctx_t       *coctx;
    ngx_http_lua_main_conf_t    *lmcf;

    ctx = ngx_http_get_module_ctx(r, ngx_http_lua_module);
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->resume_handler = ngx_http_lua_wev_handler;

    lmcf = ngx_http_get_module_main_conf(r, ngx_http_lua_module);

    c = r->connection;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "calling pipe resume");

    coctx = ctx->cur_co_ctx;
    pctx = coctx->data;

    /* push the return value */
    lua_pushinteger(coctx->co, pctx->rv);

    rc = ngx_http_lua_run_thread(lmcf->lua, r, ctx, 1);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "lua run thread returned %d", rc);

    if (rc == NGX_AGAIN) {
        return ngx_http_lua_run_posted_threads(c, lmcf->lua, r, ctx, 1);
    }

    if (rc == NGX_DONE) {
        ngx_http_lua_finalize_request(r, NGX_DONE);
        return ngx_http_lua_run_posted_threads(c, lmcf->lua, r, ctx, 1);
    }

    if (ctx->entered_content_phase) {
        ngx_http_lua_finalize_request(r, rc);
        return NGX_DONE;
    }

    return rc;
}

/* vi:set ft=c ts=4 sw=4 et fdm=marker: */
