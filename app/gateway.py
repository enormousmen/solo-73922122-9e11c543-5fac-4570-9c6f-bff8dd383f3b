"""
单进程影子流量网关 (FastAPI + aiohttp)

- 固定两个本地 GET 报价服务: 主端 PRIMARY_URL / 影子端 SHADOW_URL
- 两端并行调用, 客户端只收到主端的原始状态码和原始字节(逐块透传)
- 影子慢/失败/超时(发起起 2s)只影响影子, 不拖住主响应
- 两端各自流式计算 SHA-256, 两端都完整结束才比较状态码与摘要
- 最多 4 个影子并发, 名额满时跳过影子并注明原因, 主请求照常通过
- 客户端断连同时终止两端; 影子通信实际退出后才归还名额
- 不整份缓存/落盘响应, 每块读取后立即更新哈希并释放

GET /quote 的 200 响应交付前正文校验 (防内容损坏却 HTTP 200 的坏报价):
- 本地报价服务随 200 响应给出 X-Quote-SHA256 (交付字节的 SHA-256 摘要)
- 网关先在内存中收齐主端正文 (上限 64KiB), 核对摘要后才向客户端交付原字节;
  校验未通过前绝不向客户端发送任何报价字节
- 摘要缺失 / 摘要不符 / 正文超过 64KiB / 正文未收完整 -> 统一 502 拒绝,
  拒绝响应只含固定错误文案, 不泄漏任何报价字节或上游细节
- 其他状态码 (非 200) 及 /quote-big 不做该校验, 仍按原规则逐块透传与比对
- 拒绝即定性为终态 (primary_rejected): 终止对应影子调用, 通信实际退出后才
  归还名额; 迟到的影子结果 (含超时/失败/比较结论) 一律不得改写拒绝结论
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import resource
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

PRIMARY_URL = os.environ.get("PRIMARY_URL", "http://primary:8001")
SHADOW_URL = os.environ.get("SHADOW_URL", "http://shadow:8002")
MAX_SHADOW_CALLS = int(os.environ.get("MAX_SHADOW_CALLS", "4"))
SHADOW_TIMEOUT_S = float(os.environ.get("SHADOW_TIMEOUT_S", "2.0"))
READ_CHUNK = 64 * 1024
# GET /quote 200 正文校验: 最多只缓存/交付 64KiB, 超过即拒绝
MAX_QUOTE_BODY = 64 * 1024
MAX_RECORDS = int(os.environ.get("MAX_RECORDS", "100"))
DIGEST_HEADER = "X-Quote-SHA256"

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    # 长度由实际透传的字节流重新决定, 避免上游声明与透传块边界不一致
    "content-length",
}

# 已定性的终态: finalize / 迟到的影子结果均不得覆盖
TERMINAL_RESULTS = {
    "match", "mismatch", "shadow_timeout", "shadow_error",
    "client_disconnected", "primary_error", "primary_incomplete",
    "primary_rejected", "shadow_skipped_no_slot",
}

log = logging.getLogger("gateway")


@dataclass
class ShadowLimiter:
    """影子名额: acquire 到 release 之间覆盖整个影子通信的生命周期。"""
    capacity: int = MAX_SHADOW_CALLS
    in_flight: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self) -> bool:
        async with self.lock:
            if self.in_flight >= self.capacity:
                return False
            self.in_flight += 1
            return True

    async def release(self) -> None:
        async with self.lock:
            self.in_flight -= 1


@dataclass
class CallContext:
    rid: str
    target: str
    started_at: float
    primary_done: bool = False
    shadow_done: bool = False
    saved: bool = False
    record: Optional[dict] = None
    shadow_task: Optional[asyncio.Task] = None


state: dict = {}
# 后台影子任务的强引用集合: 防止生成器返回后 ctx 被回收导致任务被 GC 销毁,
# 任务完成时通过 done_callback 自行移除
background_tasks: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    connector = aiohttp.TCPConnector(limit=64)
    # 主端不用总超时(由客户端断连控制), 仅设连接级保护
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=30)
    state["session"] = aiohttp.ClientSession(connector=connector, timeout=timeout)
    state["limiter"] = ShadowLimiter()
    state["records"] = []
    state["records_lock"] = asyncio.Lock()
    try:
        yield
    finally:
        await state["session"].close()


app = FastAPI(title="shadow-traffic-gateway", lifespan=lifespan)


def now_ms() -> int:
    return int(time.time() * 1000)


async def append_record(rec: dict) -> None:
    async with state["records_lock"]:
        recs = state["records"]
        recs.append(rec)
        if len(recs) > MAX_RECORDS:
            del recs[:-MAX_RECORDS]


def filtered_headers(resp: aiohttp.ClientResponse) -> dict[str, str]:
    return {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP}


def _set_result(rec: dict, result: str) -> bool:
    """
    写结果的统一入口: 已定性终态 (如 client_disconnected) 时, 迟到的影子
    结果 (超时/失败/比较结论) 一律不得覆盖。
    例外: 主端正文校验拒绝 (primary_rejected) 是面向客户端的最终结论,
    即使此前因名额满跳过影子, 也以拒绝为准 (跳过事实保留在 shadow 字段)。
    """
    # 例外(优先于终态检查): 主端正文校验拒绝是面向客户端的最终结论,
    # 即使此前因名额满跳过影子 (shadow_skipped_no_slot), 也以拒绝为准,
    # 跳过事实仍保留在 rec["shadow"] 字段
    if result == "primary_rejected" and rec["result"] == "shadow_skipped_no_slot":
        rec["result"] = result
        return True
    if rec["result"] in TERMINAL_RESULTS:
        log.warning("result %r ignored: record already terminal as %r",
                    result, rec["result"])
        return False
    rec["result"] = result
    return True


async def run_shadow(ctx: CallContext) -> None:
    """
    执行一次影子调用 (从发起起整体 2s 硬超时)。
    无论成功/失败/超时/取消, 通信实际退出后才归还影子名额。
    只更新记录字段, 记录统一由主端生命周期落库, 避免出现半条记录。
    """
    acquired = ctx.record.get("shadow") == "started"
    rec = ctx.record
    try:
        session = state["session"]
        try:
            coro = session.get(SHADOW_URL + ctx.target, allow_redirects=False)
            # wait_for 从任务创建(发起)时刻起计时, 超时只取消影子请求
            async with await asyncio.wait_for(coro, timeout=SHADOW_TIMEOUT_S) as resp:
                shadow_status = resp.status
                shadow_hash = hashlib.sha256()
                async for chunk in resp.content.iter_chunked(READ_CHUNK):
                    shadow_hash.update(chunk)  # 流式哈希, 不缓存整份响应
                shadow_digest = shadow_hash.hexdigest()
        except asyncio.TimeoutError:
            rec["shadow"] = f"timeout after {SHADOW_TIMEOUT_S:g}s"
            _set_result(rec, "shadow_timeout")
            return
        except asyncio.CancelledError:
            # 客户端断连或主端失败/拒绝: 影子随主端一起终止
            rec["shadow"] = "cancelled with primary"
            _set_result(rec, "client_disconnected")
            raise
        except aiohttp.ClientError as exc:
            rec["shadow"] = f"connection error: {type(exc).__name__}"
            _set_result(rec, "shadow_error")
            return
        except Exception as exc:  # 防御: 影子侧任何意外都不能拖垮主流程
            log.exception("shadow task failed: %r", exc)
            rec["shadow"] = f"unexpected error: {type(exc).__name__}: {exc}"
            _set_result(rec, "shadow_error")
            return

        rec["shadow_status"] = shadow_status
        rec["shadow_sha256"] = shadow_digest
        rec["shadow_ms"] = now_ms() - ctx.started_at
        rec["shadow"] = "completed"

        if not ctx.primary_done:
            # 主端尚未完整结束, 暂时不能比较; 最终结果由 finalize 补算
            _set_result(rec, "shadow_completed_primary_incomplete")
            return

        # 主端已定性终态 (如校验拒绝 primary_rejected) 时不得再比较、不得覆盖
        if rec["result"] not in TERMINAL_RESULTS:
            _compare(rec)
    finally:
        ctx.shadow_done = True
        if acquired:
            # 通信实际退出(含超时/取消)后才归还名额
            await state["limiter"].release()
        if not ctx.primary_done:
            # 主端未完整(客户端断连/主端失败): 记录由主端生命周期保存
            return
        # 主端已完整: 影子在此收口, 把最终结果写回已落库的记录(原地更新)
        finalize(rec, ctx)
        if not ctx.saved:
            await save_record(ctx)


def _compare(rec: dict) -> None:
    """两端都完整结束后才调用: 比较状态码与 SHA-256 摘要。"""
    same_status = rec["shadow_status"] == rec["primary_status"]
    same_digest = rec["shadow_sha256"] == rec["primary_sha256"]
    if same_status and same_digest:
        rec["result"] = "match"
    else:
        rec["result"] = "mismatch"
        if not same_status:
            rec["detail"] = (
                f"status differs: primary={rec['primary_status']} "
                f"shadow={rec['shadow_status']}"
            )
        else:
            rec["detail"] = "same status but sha256 differs"


def finalize(rec: dict, ctx: CallContext) -> None:
    """主端完整结束后的统一收口: 补算比较结果。"""
    # 终态(断连/超时/失败等)不得被覆盖
    if rec["result"] in TERMINAL_RESULTS:
        return
    if not ctx.shadow_done:
        # 影子仍在跑(受 2s 硬超时保护): 先挂起, 等其退出时再更新,
        # 绝不能在未收完整时报一致
        rec["result"] = "shadow_pending"
        rec["shadow"] = "in flight"
        return
    # 影子已完整且主端也完整 -> 若之前挂着 incomplete 标记则补算
    if rec["result"] in ("shadow_completed_primary_incomplete", "shadow_pending"):
        _compare(rec)
    elif rec["result"] == "pending":
        # 理论上不进入(无影子任务时走 skipped 分支), 兜底
        rec["result"] = "unknown"


async def save_record(ctx: CallContext) -> None:
    """记录只落库一次: 谁先收口谁保存。"""
    if ctx.saved:
        return
    ctx.saved = True
    await append_record(ctx.record)


async def terminate_shadow(ctx: CallContext) -> None:
    """
    主端失败/拒绝/断连时终止对应影子调用, 并等待其通信实际退出
    (名额由 run_shadow 的 finally 在退出时归还)。
    """
    task = ctx.shadow_task
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await asyncio.shield(task)
    except (asyncio.CancelledError, Exception):
        pass


def reject_response() -> JSONResponse:
    """统一 502 拒绝: 固定文案, 不含任何上游报价字节或细节。"""
    return JSONResponse(
        status_code=502,
        content={"detail": "quote rejected: integrity check failed"},
    )


class _BodyTooLarge(Exception):
    def __init__(self, size: int):
        super().__init__(size)
        self.size = size


async def validated_proxy(request: Request, ctx: CallContext, target: str,
                          primary_resp: aiohttp.ClientResponse) -> Response:
    """
    GET /quote 且主端 200 的交付前正文校验路径:
    先在内存中收齐正文 (硬上限 64KiB), 核对 X-Quote-SHA256 后才交付原字节;
    任何异常都返回 502 且不向客户端泄漏报价字节。
    """
    rec = ctx.record
    chunks: list[bytes] = []
    primary_hash = hashlib.sha256()
    total = 0
    try:
        async for chunk in primary_resp.content.iter_chunked(READ_CHUNK):
            if await request.is_disconnected():
                raise asyncio.CancelledError
            total += len(chunk)
            if total > MAX_QUOTE_BODY:
                # 超限立即停止读取并拒绝: 不缓存也不转发剩余字节
                raise _BodyTooLarge(total)
            primary_hash.update(chunk)
            chunks.append(chunk)
    except asyncio.CancelledError:
        rec["primary"] = "aborted before completion"
        _set_result(rec, "client_disconnected")
        primary_resp.release()
        await terminate_shadow(ctx)
        await save_record(ctx)
        return reject_response()
    except _BodyTooLarge as exc:
        reason = "quote body too large"
        detail = f"primary quote body exceeds {MAX_QUOTE_BODY} bytes (>= {exc.size})"
        return await _reject_primary(ctx, rec, primary_resp, reason, detail)
    except aiohttp.ClientError as exc:
        reason = "quote body incomplete"
        detail = f"primary stream ended before complete: {type(exc).__name__}"
        return await _reject_primary(ctx, rec, primary_resp, reason, detail)
    except Exception as exc:  # 防御: 读取阶段任何意外都按未收完整拒绝, 绝不交付半成品
        log.exception("validated quote read failed: %r", exc)
        return await _reject_primary(
            ctx, rec, primary_resp, "quote body incomplete",
            f"unexpected read error: {type(exc).__name__}")

    # 主端正文已完整收齐
    primary_resp.release()
    ctx.primary_done = True
    rec["primary_status"] = primary_resp.status
    rec["primary_sha256"] = primary_hash.hexdigest()
    rec["primary_ms"] = now_ms() - ctx.started_at

    advertised = primary_resp.headers.get(DIGEST_HEADER)
    if not advertised:
        return await _reject_primary(
            ctx, rec, primary_resp, "digest missing",
            f"{DIGEST_HEADER} header absent on 200 response")
    if advertised.strip().lower() != rec["primary_sha256"]:
        return await _reject_primary(
            ctx, rec, primary_resp, "digest mismatch",
            f"{DIGEST_HEADER} does not match delivered bytes")

    # 校验通过: 交付主端原始状态码与原始字节 (未参与哈希的响应头原样透传)
    rec["primary"] = "completed"
    log.info("quote digest verified, delivering %d bytes rid=%s", total, ctx.rid)
    if ctx.shadow_task is None:
        await save_record(ctx)
        return Response(
            content=b"".join(chunks),
            status_code=primary_resp.status,
            headers=filtered_headers(primary_resp),
            media_type=primary_resp.headers.get("Content-Type"),
        )
    # 影子仍在跑时先落 shadow_pending, 其退出后原地更新; 不等待影子
    finalize(rec, ctx)
    await save_record(ctx)
    return Response(
        content=b"".join(chunks),
        status_code=primary_resp.status,
        headers=filtered_headers(primary_resp),
        media_type=primary_resp.headers.get("Content-Type"),
    )


async def _reject_primary(ctx: CallContext, rec: dict,
                          primary_resp: aiohttp.ClientResponse,
                          reason: str, detail: str) -> JSONResponse:
    """
    定性为终态 primary_rejected: 记录原因、终止影子 (退出后才归还名额)、
    落库, 返回不含任何报价字节的 502。迟到影子结果不得改写该结论。
    """
    rec["primary"] = f"rejected: {reason}"
    rec["reject_reason"] = reason
    rec["detail"] = detail
    _set_result(rec, "primary_rejected")
    log.warning("primary quote rejected rid=%s reason=%s detail=%s",
                ctx.rid, reason, detail)
    primary_resp.release()
    await terminate_shadow(ctx)
    await save_record(ctx)
    return reject_response()


async def proxy(request: Request, target: str, validate_digest: bool) -> Response:
    rid = f"r{now_ms()}-{id(request) % 100000:05d}"
    started = now_ms()
    rec: dict = {
        "id": rid,
        "target": target,
        "started_at": started,
        "result": "pending",
        "primary": "pending",
        "shadow": "not_started",
    }
    ctx = CallContext(rid=rid, target=target, started_at=started, record=rec)

    # 先占影子名额(非阻塞); 满了直接跳过, 但主请求照常通过
    if await state["limiter"].acquire():
        rec["shadow"] = "started"
        task = asyncio.create_task(run_shadow(ctx))
        ctx.shadow_task = task
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
    else:
        rec["shadow"] = "skipped"
        rec["result"] = "shadow_skipped_no_slot"
        rec["detail"] = (
            f"shadow skipped: {MAX_SHADOW_CALLS} shadow calls already in flight"
        )

    session = state["session"]
    try:
        primary_resp = await session.get(PRIMARY_URL + target, allow_redirects=False)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        rec["primary"] = f"connection error: {type(exc).__name__}"
        rec["result"] = "primary_error"
        # 主端连接失败: 终止已发起的影子; 名额由任务自身在退出时归还
        await terminate_shadow(ctx)
        await save_record(ctx)
        return JSONResponse(
            status_code=502,
            content={"detail": "primary quote service unavailable"},
        )

    # GET /quote 的 200 响应: 收齐正文并校验 X-Quote-SHA256 后才交付;
    # 其他状态码及 /quote-big 仍走逐块透传的原规则
    if validate_digest and primary_resp.status == 200:
        return await validated_proxy(request, ctx, target, primary_resp)

    async def body_iter():
        primary_hash = hashlib.sha256()
        try:
            async for chunk in primary_resp.content.iter_chunked(READ_CHUNK):
                if await request.is_disconnected():
                    raise asyncio.CancelledError
                primary_hash.update(chunk)  # 流式哈希, 不缓存整份响应
                yield chunk
        except asyncio.CancelledError:
            # 客户端断连: 同时终止主端读取与影子调用
            rec["primary"] = "aborted before completion"
            rec["result"] = "client_disconnected"
            primary_resp.release()
            await terminate_shadow(ctx)  # 等影子通信实际退出(名额随之归还)
            await save_record(ctx)
            return
        except aiohttp.ClientError as exc:
            # 主端传输中途断开: 主响应不完整, 同样终止影子
            rec["primary"] = f"stream error: {type(exc).__name__}"
            rec["result"] = "primary_incomplete"
            primary_resp.release()
            await terminate_shadow(ctx)
            await save_record(ctx)
            return

        # 主端完整结束: 客户端响应到此立刻收尾, 不等待影子
        primary_resp.release()
        ctx.primary_done = True
        rec["primary_status"] = primary_resp.status
        rec["primary_sha256"] = primary_hash.hexdigest()
        rec["primary_ms"] = now_ms() - started
        rec["primary"] = "completed"

        if ctx.shadow_task is None:
            # 名额满被跳过: 没有影子结果, 不产生一致性结论
            await save_record(ctx)
            return

        # 主端响应到此完整结束并立即返回客户端, 不等待影子
        finalize(rec, ctx)
        await save_record(ctx)  # 影子仍在跑时先落 shadow_pending, 其退出后原地更新

    return StreamingResponse(
        body_iter(),
        status_code=primary_resp.status,
        headers=filtered_headers(primary_resp),
        media_type=primary_resp.headers.get("Content-Type"),
    )


@app.get("/healthz")
async def healthz():
    lim: ShadowLimiter = state["limiter"]
    return {
        "status": "ok",
        "primary": PRIMARY_URL,
        "shadow": SHADOW_URL,
        "shadow_in_flight": lim.in_flight,
        "shadow_capacity": lim.capacity,
        "shadow_timeout_s": SHADOW_TIMEOUT_S,
    }


@app.get("/api/records")
async def get_records():
    async with state["records_lock"]:
        return list(reversed(state["records"]))


@app.get("/api/state")
async def get_state():
    lim: ShadowLimiter = state["limiter"]
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "shadow_in_flight": lim.in_flight,
        "shadow_capacity": lim.capacity,
        "records": len(state["records"]),
        "max_rss_mib": round(rss_kb / 1024, 2),
    }


# 报价相关 GET 请求走代理; 网页与管理 API 不拦截
@app.get("/quote")
async def quote_proxy(request: Request):
    target = request.url.path
    if request.url.query:
        target += "?" + request.url.query
    # /quote 的 200 响应需要 X-Quote-SHA256 正文校验
    return await proxy(request, target, validate_digest=True)


@app.get("/quote-big")
async def quote_big_proxy(request: Request):
    target = request.url.path
    if request.url.query:
        target += "?" + request.url.query
    # 大响应不缓存, 仍按原规则逐块透传与摘要比对
    return await proxy(request, target, validate_digest=False)


app.mount("/", StaticFiles(
    directory=os.path.join(os.path.dirname(__file__), "web"), html=True),
    name="web")
