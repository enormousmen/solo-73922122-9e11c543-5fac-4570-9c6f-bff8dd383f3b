"""关键功能自测脚本: 通过真实 HTTP 并发压测网关行为。"""
import asyncio
import hashlib
import json
import sys
import time

import aiohttp

GW = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8020"
PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


PENDING_RESULTS = {"shadow_pending", "shadow_completed_primary_incomplete", "pending"}


async def wait_record(s, gw, marker, timeout_s=5.0):
    """按 marker 轮询等待本次运行的对应记录到达终态后返回。"""
    full_marker = f"marker={marker}" if not marker.startswith("marker=") else marker
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        recs = await (await s.get(f"{gw}/api/records")).json()
        # 精确匹配完整 marker, 避免命中历史运行遗留的同前缀记录
        hit = [x for x in recs if full_marker in x["target"].split("&")
               or x["target"].endswith(full_marker)]
        if hit and hit[0]["result"] not in PENDING_RESULTS:
            return hit[0]
        await asyncio.sleep(0.15)
    raise AssertionError(f"terminal record with {full_marker} not found")


async def main():
    run_id = f"run{int(time.time() * 1000) % 1000000}"
    async with aiohttp.ClientSession() as s:
        # 1. 基础一致: 主端原始字节透传 + match
        url = f"{GW}/quote?symbol=ZZZ&marker={run_id}-t1"
        async with s.get(url) as r:
            body = await r.read()
            gw_status = r.status
        rec = await wait_record(s, GW, f"marker={run_id}-t1")
        check("1 一致请求报 match", rec["result"] == "match", rec["result"])
        check("1 客户端收主端200", gw_status == 200)
        check("1 透传字节=主端原始字节",
              rec["primary_sha256"] == hashlib.sha256(body).hexdigest())

        # 2. diff 制造差异
        async with s.get(f"{GW}/quote?diff=1&marker={run_id}-t2") as r:
            await r.read()
        rec = await wait_record(s, GW, f"marker={run_id}-t2")
        check("2 diff 报 mismatch", rec["result"] == "mismatch", rec.get("detail", ""))
        check("2 摘要确实不同", rec["primary_sha256"] != rec["shadow_sha256"])

        # 3. 影子超时不拖住主端
        t0 = time.monotonic()
        async with s.get(f"{GW}/quote?s_delay=3&marker={run_id}-t3") as r:
            await r.read()
        elapsed = time.monotonic() - t0
        check("3 影子超时时主端快速返回", elapsed < 0.5, f"{elapsed:.3f}s")
        rec = await wait_record(s, GW, f"marker={run_id}-t3")
        check("3 记录为 shadow_timeout", rec["result"] == "shadow_timeout", rec["shadow"])

        # 4. 名额: 6 个真并发(各自独立 TCP 连接), 影子延迟占住名额, 主端零延迟
        async def one(i):
            # 独立 connector 保证 6 个请求同时在途, 而不是单连接流水线串行
            async with aiohttp.ClientSession() as cs:
                async with cs.get(f"{GW}/quote?s_delay=1.2&marker={run_id}-t4-{i}") as r:
                    return r.status, time.monotonic()
        t0 = time.monotonic()
        results = await asyncio.gather(*(one(i) for i in range(6)))
        wall = time.monotonic() - t0
        check("4 六个主请求全部200", all(st == 200 for st, _ in results))
        # 主端无延迟: 若被影子名额/影子通信阻塞, 墙钟会接近 1.2s
        check("4 主请求不被影子名额阻塞", wall < 0.8, f"wall={wall:.2f}s")
        await asyncio.sleep(0.3)
        recs = (await (await s.get(f"{GW}/api/records")).json())
        batch = [x for x in recs if f"marker={run_id}-t4-" in x["target"]]
        skipped = [x for x in batch if x["result"] == "shadow_skipped_no_slot"]
        # 6 个请求有毫秒级启动抖动: 期望 2~3 个跳过(容量 4), 关键是满额即跳且主端不阻塞
        check("4 名额满导致跳过(6并发/4名额)", 2 <= len(skipped) <= 3,
              f"skipped={len(skipped)}/6")
        check("4 跳过注明原因", all("already in flight" in x.get("detail", "")
                                    for x in skipped))

        # 5. 名额归还: 等测试4的影子通信实际退出后, 新请求影子恢复
        for _ in range(30):
            st = await (await s.get(f"{GW}/api/state")).json()
            if st["shadow_in_flight"] == 0:
                break
            await asyncio.sleep(0.2)
        async with s.get(f"{GW}/quote?marker={run_id}-t5") as r:
            await r.read()
        rec = await wait_record(s, GW, f"marker={run_id}-t5")
        check("5 名额已归还影子恢复", rec["shadow"] == "completed", rec["shadow"])

        # 6. 32MiB 流式: 字节完整、摘要一致、网关不缓存
        async with s.get(f"{GW}/quote-big?marker={run_id}-t6") as r:
            h = hashlib.sha256()
            total = 0
            async for chunk in r.content.iter_chunked(65536):
                h.update(chunk)
                total += len(chunk)
        check("6 32MiB 字节数=33554432", total == 32 * 1024 * 1024, str(total))
        rec = await wait_record(s, GW, f"marker={run_id}-t6")
        check("6 大响应两端 match", rec["result"] == "match", rec["result"])
        check("6 客户端流式摘要=主端摘要",
              h.hexdigest() == rec["primary_sha256"])
        state = await (await s.get(f"{GW}/api/state")).json()
        check("6 网关峰值RSS<96MiB",
              (state["max_rss_mib"] or 0) < 96, f"{state['max_rss_mib']}MiB")

        # 7. 客户端提前断连: 两端都终止, 名额最终归还
        try:
            async with s.get(f"{GW}/quote-big?chunk_ms=50&marker={run_id}-t7") as r:
                count = 0
                async for _ in r.content.iter_chunked(65536):
                    count += 1
                    if count >= 3:
                        break
        except Exception:
            pass
        # 记录在影子通信实际退出后才落库: 轮询等待
        disc = []
        for _ in range(30):
            await asyncio.sleep(0.2)
            recs = (await (await s.get(f"{GW}/api/records")).json())
            disc = [x for x in recs if f"chunk_ms=50&marker={run_id}-t7" in x["target"]]
            if disc:
                break
        state = await (await s.get(f"{GW}/api/state")).json()
        check("7 断连有记录", len(disc) == 1, str(len(disc)))
        check("7 断连结果标记", disc and disc[0]["result"] == "client_disconnected",
              disc[0]["result"] if disc else "")
        check("7 断连后影子名额归零", state["shadow_in_flight"] == 0,
              str(state["shadow_in_flight"]))

        # 8. 正常 /quote 200 随附 X-Quote-SHA256, 且摘要覆盖交付字节
        async with s.get(f"{GW}/quote?marker={run_id}-t8") as r:
            hdr = r.headers.get("X-Quote-SHA256")
            body = await r.read()
            st8 = r.status
        check("8 200 随附 X-Quote-SHA256", bool(hdr))
        check("8 摘要=交付字节 SHA-256",
              hdr == hashlib.sha256(body).hexdigest())
        check("8 正常报价仍为 200 且交付原文", st8 == 200 and b"128.50" in body)

        # 9. 坏摘要: 502 拦截, 不泄漏任何报价字节, 拒绝为终态且影子被终止
        async with s.get(f"{GW}/quote?bad_digest=1&marker={run_id}-t9") as r:
            bad = await r.read()
            st9 = r.status
        check("9 坏摘要返回 502", st9 == 502, str(st9))
        check("9 拒绝正文不含报价", b"128.50" not in bad and b"ACME" not in bad,
              bad[:60].decode(errors="replace"))
        rec = await wait_record(s, GW, f"marker={run_id}-t9")
        check("9 记录 primary_rejected", rec["result"] == "primary_rejected",
              rec["result"])
        check("9 记录拒绝原因", rec.get("reject_reason") == "digest mismatch",
              rec.get("reject_reason", ""))
        check("9 影子随拒绝终止", rec["shadow"] == "cancelled with primary",
              rec["shadow"])

        # 10. 正文超 64KiB: 502 拦截 (pad 后总字节 = 64KiB+1)
        async with s.get(f"{GW}/quote?pad=65422&marker={run_id}-t10") as r:
            over = await r.read()
            st10 = r.status
        check("10 超 64KiB 返回 502", st10 == 502 and b"128.50" not in over, str(st10))
        rec = await wait_record(s, GW, f"marker={run_id}-t10")
        check("10 拒绝原因为超限", rec["result"] == "primary_rejected"
              and rec.get("reject_reason") == "quote body too large",
              rec.get("reject_reason", ""))

        # 11. 恰好 64KiB 边界: 正常交付 (基础正文 115B + pad 65421)
        async with s.get(f"{GW}/quote?pad=65421&marker={run_id}-t11") as r:
            edge = await r.read()
            st11 = r.status
        check("11 恰好64KiB放行", st11 == 200 and len(edge) == 65536,
              f"{st11}/{len(edge)}")

        # 12. 正文未收完整 (cut): 502 拦截
        async with s.get(f"{GW}/quote?cut=1&marker={run_id}-t12") as r:
            cut = await r.read()
            st12 = r.status
        check("12 未收完整返回 502", st12 == 502 and b"128.50" not in cut, str(st12))
        rec = await wait_record(s, GW, f"marker={run_id}-t12")
        check("12 拒绝原因为不完整", rec["result"] == "primary_rejected"
              and rec.get("reject_reason") == "quote body incomplete",
              rec.get("reject_reason", ""))

        # 13. 非 200 状态码不做摘要校验, 原样透传 (404 无摘要头也放行)
        async with s.get(f"{GW}/quote?status=404&marker={run_id}-t13") as r:
            n200 = await r.read()
            st13 = r.status
            hdr13 = r.headers.get("X-Quote-SHA256")
        check("13 非200原样透传", st13 == 404 and b"128.50" in n200 and not hdr13,
              f"{st13}/hdr={hdr13}")

        # 14. 迟到的影子结果不得改写拒绝结论 (主端坏摘要快速拒绝, 影子延迟晚结束)
        async with s.get(
                f"{GW}/quote?bad_digest=1&s_delay=1.5&marker={run_id}-t14") as r:
            await r.read()
        rec = await wait_record(s, GW, f"marker={run_id}-t14")
        check("14 迟到影子不改写拒绝", rec["result"] == "primary_rejected",
              rec["result"])
        for _ in range(25):
            st = await (await s.get(f"{GW}/api/state")).json()
            if st["shadow_in_flight"] == 0:
                break
            await asyncio.sleep(0.2)
        # 再拉一次确认影子退出后结论仍未被覆盖
        recs = await (await s.get(f"{GW}/api/records")).json()
        rec = [x for x in recs if f"marker={run_id}-t14" in x["target"]][0]
        check("14 影子退出后结论仍为拒绝", rec["result"] == "primary_rejected",
              rec["result"])
        state = await (await s.get(f"{GW}/api/state")).json()
        check("14 名额最终归还", state["shadow_in_flight"] == 0,
              str(state["shadow_in_flight"]))

        # 15. 拒绝之后后续正常报价立即恢复 200 交付
        async with s.get(f"{GW}/quote?marker={run_id}-t15") as r:
            after = await r.read()
            st15 = r.status
            hdr15 = r.headers.get("X-Quote-SHA256")
        check("15 拒绝后正常报价恢复", st15 == 200
              and hdr15 == hashlib.sha256(after).hexdigest(),
              str(st15))

    print(f"\n汇总: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("失败项:", FAIL)
        sys.exit(1)


asyncio.run(main())
