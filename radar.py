#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gas-radar —— 多链 Gas 观察台

回答一个很实际的问题：**现在发交易贵不贵？什么时候便宜？**

做法：不止看「当前 gas price」这一个瞬间的数字（它每分钟都在跳），
而是按最近 24 小时**逐块抽样**，得出每条链的拥堵曲线，
再告诉你哪个时间窗相对便宜。

只用标准库（urllib / json / argparse），无依赖，clone 即跑。
全部是公开 RPC 的只读调用。

用法：
    python3 radar.py
    python3 radar.py --chains ethereum,bsc,base,arbitrum --samples 48
    python3 radar.py --tz 8
输出：
    output/report.md   人看的报告
    output/gas.json    机器读的明细
"""

import argparse
import concurrent.futures as futures
import json
import os
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError

CHAINS = {
    "ethereum": {
        "label": "以太坊",
        "native": "ETH",
        "block_time": 12.0,
        "rpcs": [
            "https://ethereum-rpc.publicnode.com",
            "https://eth.drpc.org",
            "https://1rpc.io/eth",
        ],
        "explorer": "https://etherscan.io/gastracker",
    },
    "bsc": {
        "label": "BNB Chain",
        "native": "BNB",
        "block_time": 0.75,
        "rpcs": [
            "https://bsc-dataseed.binance.org",
            "https://bsc-rpc.publicnode.com",
            "https://bsc-dataseed1.defibit.io",
        ],
        "explorer": "https://bscscan.com/gastracker",
    },
    "base": {
        "label": "Base",
        "native": "ETH",
        "block_time": 2.0,
        "rpcs": ["https://mainnet.base.org", "https://base-rpc.publicnode.com"],
        "explorer": "https://basescan.org/gastracker",
    },
    "arbitrum": {
        "label": "Arbitrum One",
        "native": "ETH",
        "block_time": 0.25,
        "rpcs": ["https://arb1.arbitrum.io/rpc", "https://arbitrum-one-rpc.publicnode.com"],
        "explorer": "https://arbiscan.io/gastracker",
    },
}

# 有些链（如 Arbitrum）的区块 gasLimit 是个巨大的哨兵值（2^50），不是真实容量，
# 于是 utilization 恒等于 0 —— 用它做拥堵判断会得出完全错误的结论。超过这个量级就判定不可用。
SENTINEL_GAS_LIMIT = 10 ** 15
# 利用率低于这个水平时，链本身长期空旷，利用率也不再是有意义的拥堵信号
MIN_USEFUL_UTIL = 0.02


# ---------------------------------------------------------------- RPC


class Rpc(object):
    """多节点轮询 + 重试的极简 JSON-RPC 客户端（把响应体带进异常，方便定位）。"""

    def __init__(self, urls, timeout=12, retries=2):
        self.urls = list(urls)
        self.timeout = timeout
        self.retries = retries
        self._idx = 0
        self.calls = 0

    def call(self, method, params):
        last = None
        for attempt in range(self.retries + 1):
            url = self.urls[self._idx % len(self.urls)]
            body = json.dumps(
                {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            ).encode()
            req = urllib.request.Request(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    # 部分公开节点会 403 掉默认的 Python-urllib UA
                    "User-Agent": "gas-radar/1.0 (+https://github.com)",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    data = json.loads(r.read().decode())
                self.calls += 1
                if data.get("error"):
                    raise RuntimeError("RPC error: %s" % data["error"])
                return data.get("result")
            except (HTTPError, URLError, ValueError, RuntimeError, OSError) as e:
                last = e
                self._idx += 1
                if attempt < self.retries:
                    time.sleep(0.5)
        raise RuntimeError("所有节点都失败: %s" % last)

    def block_number(self):
        return int(self.call("eth_blockNumber", []), 16)

    def gas_price(self):
        return int(self.call("eth_gasPrice", []), 16)

    def block(self, n):
        return self.call("eth_getBlockByNumber", [hex(n), False])

    def batch_blocks(self, numbers, workers=8):
        """并发抓区块 —— 串行抓几十个块要等很久，这里开线程池。"""
        out = {}
        with futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(self.block, n): n for n in numbers}
            for f in futures.as_completed(futs):
                n = futs[f]
                try:
                    b = f.result()
                except Exception:
                    b = None
                if b:
                    out[n] = b
        return out


# ---------------------------------------------------------------- 解析


def parse_block(b):
    try:
        gas_used = int(b["gasUsed"], 16)
        gas_limit = int(b["gasLimit"], 16)
        ts = int(b["timestamp"], 16)
        ntx = len(b.get("transactions") or [])
        number = int(b["number"], 16)
    except (KeyError, TypeError, ValueError):
        return None
    base_fee = None
    if b.get("baseFeePerGas"):
        try:
            base_fee = int(b["baseFeePerGas"], 16)
        except ValueError:
            base_fee = None
    return {
        "number": number,
        "gas_used": gas_used,
        "gas_limit": gas_limit,
        "util": (gas_used / gas_limit) if gas_limit else None,
        "ts": ts,
        "txs": ntx,
        "base_fee": base_fee,
    }


def gwei(wei):
    return None if wei is None else wei / 1e9


def pick_signal(samples):
    """选这条链真正能用的拥堵信号：优先区块利用率，不可用时退回 gas price。

    返回 (字段名, 展示名, 说明或 None)
    """
    utils = [s["util"] for s in samples if s["util"] is not None]
    fees = [s["base_fee"] for s in samples if s["base_fee"]]
    max_limit = max((s["gas_limit"] for s in samples), default=0)
    avg_util = statistics.mean(utils) if utils else 0.0

    if max_limit >= SENTINEL_GAS_LIMIT:
        note = (
            "该链区块 `gasLimit` 是哨兵值（%d ≈ 2^50），不是真实容量，"
            "利用率恒为 0 → 时段分析改用 **gas price**。" % max_limit
        )
        return ("base_fee", "gas price", note) if fees else ("util", "区块利用率", note)
    if avg_util < MIN_USEFUL_UTIL:
        note = "该链 24 小时平均利用率仅 %.1f%%，长期空旷 → 时段分析改用 **gas price**。" % (
            avg_util * 100
        )
        return ("base_fee", "gas price", note) if fees else ("util", "区块利用率", note)
    return ("util", "区块利用率", None)


def by_hour(samples, key, tz_offset=8, top=3):
    """按【当地小时】聚合 24h 抽样，返回该时段均值最低的若干个小时。"""
    buckets = {}
    for s in samples:
        v = s.get(key)
        if v is None:
            continue
        local = datetime.fromtimestamp(s["ts"], tz=timezone(timedelta(hours=tz_offset)))
        buckets.setdefault(local.hour, []).append(v)
    rows = [(h, statistics.mean(v), len(v)) for h, v in buckets.items()]
    rows.sort(key=lambda x: x[1])
    return rows[:top], rows


def is_flat(vals):
    """信号在 24 小时内几乎没有差异吗？（例如 Arbitrum 的 gas price 长期恒定）

    这种情况下列「便宜时段」是假的 —— 没有便宜不便宜之分，随时发都一样。
    """
    vals = [v for v in vals if v]
    if len(vals) < 2:
        return True
    lo, hi = min(vals), max(vals)
    return lo > 0 and (hi / lo) < 1.05


def fmt_sig(v, key):
    if v is None:
        return "—"
    return "%.2f gwei" % gwei(v) if key == "base_fee" else "%.1f%%" % (v * 100)


def verdict(now_v, avg_v, key):
    if not now_v or not avg_v:
        return "数据不足"
    d = now_v / avg_v
    name = "gas price" if key == "base_fee" else "利用率"
    if d >= 1.15:
        return "偏贵（%s 比 24 小时均值高 %d%%）" % (name, round((d - 1) * 100))
    if d <= 0.85:
        return "便宜（%s 比 24 小时均值低 %d%%）" % (name, round((1 - d) * 100))
    return "正常（%s 与 24 小时均值相当）" % name


# ---------------------------------------------------------------- 报告


def build(reports, tz_offset, samples_n):
    L = []
    L.append("# 多链 Gas 观察台")
    L.append("")
    L.append(
        "- 采样：每条链最近 24 小时均匀抽取 **%d** 个区块（不是只看当前这一个瞬间）"
        % samples_n
    )
    L.append("- 时区：UTC%+d" % tz_offset)
    L.append("- 数据来源：公开 RPC 节点的只读 `eth_getBlockByNumber` / `eth_gasPrice`")
    L.append("")

    L.append("## 结论：现在该不该发交易")
    L.append("")
    L.append("| 链 | 当前 gas | 24h 均价 | 判据 | 现在 | 最便宜的时段（当地） |")
    L.append("|---|---|---|---|---|---|")
    for r in reports:
        if r.get("error"):
            L.append("| %s | — | — | — | 查询失败 | — |" % r["label"])
            continue
        if r["flat"]:
            cheap = "全天无差异（随时可发）"
        else:
            cheap = "、".join(
                "%02d:00（%s）" % (h, fmt_sig(u, r["signal"])) for h, u, _ in r["cheap_top"]
            )
        L.append(
            "| %s | %.3f gwei | %.3f gwei | %s | %s | %s |"
            % (
                r["label"],
                gwei(r["gas_price"]) or 0,
                gwei(r["avg_gas_price_proxy"]) or 0,
                r["sig_label"],
                r["verdict"],
                cheap,
            )
        )
    L.append("")

    for r in reports:
        L.append("## %s（%s）" % (r["label"], r["chain"]))
        L.append("")
        if r.get("error"):
            L.append("查询失败：%s" % r["error"])
            L.append("")
            continue
        if r.get("signal_note"):
            L.append("> ⚠️ %s" % r["signal_note"])
            L.append("")
        L.append("| 指标 | 值 |")
        L.append("|---|---|")
        L.append("| 最新区块 | #%d |" % r["head"])
        L.append("| 当前 gas price | **%.4f gwei** |" % (gwei(r["gas_price"]) or 0))
        if r.get("base_fee") is not None:
            L.append("| 最新块 baseFee | %.4f gwei |" % (gwei(r["base_fee"]) or 0))
        L.append("| 24h 平均区块利用率 | %.1f%% |" % ((r["avg_util"] or 0) * 100))
        L.append(
            "| 近 12 块利用率 | %s |"
            % (" ".join("%.0f%%" % (u * 100) for u in r["recent_util"][:12]) or "—")
        )
        L.append("| 24h 平均每块交易数 | %d |" % r["avg_txs"])
        L.append("| 24h 利用率峰值 | %.1f%% |" % (r["max_util"] * 100))
        L.append("| 判据 | %s |" % r["sig_label"])
        L.append("")
        if r["flat"]:
            if r["signal"] == "base_fee":
                rng = "%.4f–%.4f gwei" % (gwei(r["sig_min"]), gwei(r["sig_max"]))
            else:
                rng = "%.1f%%–%.1f%%" % (r["sig_min"] * 100, r["sig_max"] * 100)
            L.append("### 便宜时段")
            L.append("")
            L.append(
                "✅ **不存在「便宜时段」** —— 这条链的 %s 在 24 小时内基本没有变化"
                "（区间 %s）。什么时候发都一样，不必为挑时间而等。" % (r["sig_label"], rng)
            )
        else:
            L.append("### 便宜时段排名（按 24 小时抽样聚合，当地时区）")
            L.append("")
            L.append("| 时段 | 平均%s | 抽样块数 |" % r["sig_label"])
            L.append("|---|---|---|")
            for h, u, n in r["cheap_all"]:
                L.append("| %02d:00 | %s | %d |" % (h, fmt_sig(u, r["signal"]), n))
        L.append("")

    L.append("## 怎么用这份报告")
    L.append("")
    L.append("1. **看判据那一列，别一律看利用率。** 利用率是「区块里 gas 被用掉的比例」，")
    L.append("   对以太坊这类链它比瞬时 gas price 更能反映真实拥堵；")
    L.append("   但对 Arbitrum 这类链它**根本不可用**（gasLimit 是哨兵值，利用率恒为 0），")
    L.append("   所以工具会自动退回用 gas price 做时段对比 —— 报告里会写明。")
    L.append("2. **转账、授权这类不急的操作，等便宜的时段再发。** 时段是按你所在时区算的。")
    L.append("3. **不要只看「当前 gas price」一个数字下判断** —— 它跳得很快，")
    L.append("   L2 的 gas 还受 L1 数据成本影响，波动更明显。")
    L.append("")
    L.append(
        "> 说明：这里给的是**拥堵程度的相对判断**，不是精确手续费报价。"
        "实际手续费 = gas 用量 × gas price，金额还取决于你这笔操作有多复杂。"
    )
    L.append("")
    return "\n".join(L)


# ---------------------------------------------------------------- 主流程


def analyse(chain, cfg, samples_n, tz_offset, timeout):
    rpc = Rpc(cfg["rpcs"], timeout=timeout)
    head = rpc.block_number()
    try:
        gp = rpc.gas_price()
    except Exception:
        gp = None

    # 按链的出块速度，把「最近 24h」映射成区块区间，再均匀抽样
    span = min(int(24 * 3600 / cfg["block_time"]), head)
    step = max(1, span // samples_n)
    numbers = [n for n in (head - i * step for i in range(samples_n)) if n > 0]

    raw = rpc.batch_blocks(numbers)
    samples = [s for s in (parse_block(b) for b in raw.values()) if s]
    samples.sort(key=lambda s: s["number"])
    if not samples:
        raise RuntimeError("一个区块都没抓到")

    signal, sig_label, note = pick_signal(samples)

    utils = [s["util"] for s in samples if s["util"] is not None]
    avg_util = statistics.mean(utils) if utils else None

    vals = [s[signal] for s in samples if s.get(signal) is not None]
    avg_sig = statistics.mean(vals) if vals else None

    cheap_top, cheap_all = by_hour(samples, signal, tz_offset)

    recent = [s for s in samples if s["number"] > head - 12] or samples[-12:]
    recent_vals = [s[signal] for s in recent if s.get(signal) is not None]
    recent_avg = statistics.mean(recent_vals) if recent_vals else None

    fees = [s["base_fee"] for s in samples if s["base_fee"]]
    avg_fee = statistics.mean(fees) if fees else (gp or 0)

    return {
        "chain": chain,
        "label": cfg["label"],
        "head": head,
        "gas_price": gp,
        "base_fee": samples[-1]["base_fee"],
        "avg_gas_price_proxy": avg_fee,
        "avg_util": avg_util,
        "max_util": max(utils) if utils else 0.0,
        "avg_txs": int(statistics.mean([s["txs"] for s in samples])),
        "recent_util": [s["util"] for s in recent if s["util"] is not None],
        "signal": signal,
        "sig_label": sig_label,
        "signal_note": note,
        "avg_signal": avg_sig,
        "flat": is_flat(vals),
        "sig_min": min(vals) if vals else 0,
        "sig_max": max(vals) if vals else 0,
        "verdict": verdict(recent_avg, avg_sig, signal),
        "cheap_top": cheap_top,
        "cheap_all": cheap_all,
        "samples": len(samples),
        "rpc_calls": rpc.calls,
    }


def main():
    ap = argparse.ArgumentParser(description="多链 Gas 观察台：现在贵不贵、什么时候便宜")
    ap.add_argument(
        "--chains",
        default="ethereum,bsc,base,arbitrum",
        help="链，逗号分隔（可选 %s）" % ",".join(CHAINS),
    )
    ap.add_argument("--samples", type=int, default=36, help="每条链抽样区块数，默认 36")
    ap.add_argument("--tz", type=int, default=8, help="展示用 UTC 偏移，默认 8（北京时间）")
    ap.add_argument("--out", default="output", help="输出目录")
    ap.add_argument("--timeout", type=float, default=12.0)
    args = ap.parse_args()

    chains = [c.strip() for c in args.chains.split(",") if c.strip()]
    bad = [c for c in chains if c not in CHAINS]
    if bad:
        print("❌ 不支持的链：%s（可选：%s）" % (",".join(bad), ",".join(CHAINS)), file=sys.stderr)
        return 2

    os.makedirs(args.out, exist_ok=True)
    reports = []
    for c in chains:
        print("→ 采样 %s ..." % CHAINS[c]["label"])
        try:
            r = analyse(c, CHAINS[c], args.samples, args.tz, args.timeout)
            reports.append(r)
            print(
                "  当前 %.4f gwei · 24h 利用率 %.1f%% · 判据=%s · %s"
                % (
                    gwei(r["gas_price"]) or 0,
                    (r["avg_util"] or 0) * 100,
                    r["sig_label"],
                    r["verdict"],
                )
            )
            if r["signal_note"]:
                print("  ⚠️ %s" % r["signal_note"])
        except Exception as e:
            reports.append({"chain": c, "label": CHAINS[c]["label"], "error": str(e)})
            print("  ❌ 失败：%s" % e)

    if not [r for r in reports if not r.get("error")]:
        print("❌ 所有链都失败了", file=sys.stderr)
        return 1

    md = build(reports, args.tz, args.samples)
    rp = os.path.join(args.out, "report.md")
    jp = os.path.join(args.out, "gas.json")
    with open(rp, "w", encoding="utf-8") as f:
        f.write(md)
    with open(jp, "w", encoding="utf-8") as f:
        json.dump(
            {"tz_offset": args.tz, "samples_per_chain": args.samples, "chains": reports},
            f,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    print("")
    print("=" * 56)
    print("报告：%s" % rp)
    print("明细：%s" % jp)
    print("=" * 56)
    return 0


if __name__ == "__main__":
    sys.exit(main())
