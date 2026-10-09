# -*- coding: utf-8 -*-
"""
FreeFire Level Up Bot - Professional Real-Time Dashboard Backend
Live counters, mode detection (BR / Lone Wolf), match complete tracking, pause control.
"""

import asyncio
import json
import os
import time
from collections import deque
from typing import Dict, List, Any, Optional
from aiohttp import web

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
ACCOUNTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "accounts.json")

# ==================== EXP TABLE ====================
EXP_TABLE: Dict[int, int] = {
    1: 0, 2: 48, 3: 202, 4: 544, 5: 1012, 6: 1844, 7: 2792, 8: 3800,
    9: 4870, 10: 6004, 11: 7192, 12: 8448, 13: 9760, 14: 11140, 15: 12566,
    16: 14060, 17: 15610, 18: 17224, 19: 18902, 20: 20632, 21: 22424, 22: 24278,
    23: 26192, 24: 28166, 25: 30200, 26: 32294, 27: 34448, 28: 37804, 29: 41274,
    30: 44870, 31: 48582, 32: 53394, 33: 58566, 34: 64096, 35: 69994, 36: 76460,
    37: 83506, 38: 91128, 39: 99322, 40: 108092, 41: 120144, 42: 133266, 43: 147472,
    44: 162760, 45: 179126, 46: 196572, 47: 215368, 48: 235516, 49: 257010, 50: 279860,
    51: 304056, 52: 348318, 53: 394982, 54: 444044, 55: 495508, 56: 549364, 57: 633756,
    58: 721744, 59: 813336, 60: 908522, 61: 1041438, 62: 1180352, 63: 1325266,
    64: 1476184, 65: 1634300, 66: 1840946, 67: 2056594, 68: 2281242, 69: 2514880,
    70: 2757530, 71: 3059506, 72: 3372284, 73: 3699456, 74: 4041030, 75: 4397002,
    76: 4829104, 77: 5282204, 78: 5756304, 79: 6251408, 80: 6776502, 81: 7381324,
    82: 8043154, 83: 8752982, 84: 9510808, 85: 10316338, 86: 11277190, 87: 12291748,
    88: 13360304, 89: 14482858, 90: 15659418, 91: 17026708, 92: 18453950, 93: 19941280,
    94: 21488570, 95: 23095858, 96: 24763138, 97: 26490428, 98: 28378704, 99: 30124996,
    100: 32032884
}


def calculate_level_progress(level: int, current_exp: int) -> Dict[str, Any]:
    level = max(1, min(100, int(level or 1)))
    next_level = min(100, level + 1)
    base_exp = EXP_TABLE.get(level, 0)
    target_exp = EXP_TABLE.get(next_level, base_exp + 50000)

    needed_for_level = max(1, target_exp - base_exp)
    earned_in_level = max(0, current_exp - base_exp)
    remaining_exp = max(0, target_exp - current_exp)
    progress_pct = min(100.0, max(0.0, (earned_in_level / needed_for_level) * 100.0))

    return {
        "next_level": next_level,
        "base_exp": base_exp,
        "target_exp": target_exp,
        "needed_for_level": needed_for_level,
        "earned_in_level": earned_in_level,
        "remaining_exp": remaining_exp,
        "progress_pct": round(progress_pct, 1),
    }


def get_game_mode(level: int, match_type: Optional[str] = None) -> Dict[str, str]:
    if match_type:
        mt = str(match_type).upper()
        if mt in ("BR", "BATTLE_ROYALE", "BATTLE ROYALE"):
            return {"mode": "BR", "label": "Battle Royale"}
        if mt in ("LONE_WOLF", "LW", "LONE WOLF"):
            return {"mode": "LONE_WOLF", "label": "Lone Wolf"}

    lvl = int(level or 1)
    if lvl < 3:
        return {"mode": "BR", "label": "Battle Royale (Level < 3)"}
    return {"mode": "LONE_WOLF", "label": "Lone Wolf (Level 3+)"}


# ==================== BOT STATE ====================
class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 300

        self.total_matches_started = 0
        self.total_matches_finished = 0
        self.total_gained_exp = 0

        self.start_time = time.time()
        self.exp_history: deque = deque(maxlen=720)
        self._last_history_push = 0.0

        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.auth_to_game_id: Dict[str, str] = {}
        self.game_to_auth_id: Dict[str, str] = {}
        self.account_token_map: Dict[str, str] = {}

        self.paused_accounts: set = set()
        self.refresh_callbacks: Dict[str, Any] = {}
        self.active_writers: Dict[str, set] = {}

    # ---------- Logging ----------
    def log(self, message: str, level: str = "info", uid: Optional[str] = None):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": str(message),
            "uid": str(uid) if uid else None,
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs = self.logs[-self.max_logs:]

    # ---------- Socket Writers ----------
    def register_writer(self, uid: str, writer):
        uid_str = str(uid)
        self.active_writers.setdefault(uid_str, set()).add(writer)

    def unregister_writer(self, uid: str, writer):
        uid_str = str(uid)
        if uid_str in self.active_writers:
            self.active_writers[uid_str].discard(writer)
            if not self.active_writers[uid_str]:
                self.active_writers.pop(uid_str, None)

    def close_writers_for_account(self, uid: str):
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            candidates.add(str(self.game_to_auth_id[uid_str]))

        for c in list(candidates):
            for w in list(self.active_writers.get(c, [])):
                try:
                    if hasattr(w, "close") and (not hasattr(w, "is_closing") or not w.is_closing()):
                        w.close()
                except Exception:
                    pass
            self.active_writers.pop(c, None)

    # ---------- Account Registration ----------
    def register_account(
        self,
        uid: str,
        nickname: str,
        region: str,
        level: int,
        exp: int,
        likes: int = 0,
        token: Optional[str] = None,
        auth_uid: Optional[str] = None,
    ):
        uid_str = str(uid)
        auth_uid_str = str(auth_uid) if auth_uid else self.game_to_auth_id.get(uid_str, "")

        if auth_uid_str:
            self.auth_to_game_id[auth_uid_str] = uid_str
            self.game_to_auth_id[uid_str] = auth_uid_str
            self.account_token_map[auth_uid_str] = uid_str
            self.account_token_map[uid_str] = auth_uid_str

        if token:
            self.account_token_map[uid_str] = token
            self.account_token_map[token[:16]] = uid_str
            if auth_uid_str:
                self.account_token_map[auth_uid_str] = token

        prog = calculate_level_progress(level or 1, exp)
        lvl_val = int(level or 1)
        mode_info = get_game_mode(lvl_val)

        if uid_str not in self.accounts:
            self.accounts[uid_str] = {
                "uid": uid_str,
                "auth_uid": auth_uid_str or "",
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "ID",
                "level": lvl_val,
                "next_level": prog["next_level"],
                "mode": mode_info["mode"],
                "mode_label": mode_info["label"],
                "actual_mode": None,
                "initial_exp": int(exp or 0),
                "current_exp": int(exp or 0),
                "gained_exp": 0,
                "remaining_exp": prog["remaining_exp"],
                "target_exp": prog["target_exp"],
                "needed_for_level": prog["needed_for_level"],
                "earned_in_level": prog["earned_in_level"],
                "progress_pct": prog["progress_pct"],
                "likes": int(likes or 0),
                "status": "PAUSED" if self.is_paused(uid_str) else "ONLINE",
                "is_paused": self.is_paused(uid_str),
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "token": token or "",
                "start_time": time.time(),
                "paused_at": time.time() if self.is_paused(uid_str) else None,
                "total_pause_duration": 0.0,
                "last_updated": time.strftime("%H:%M:%S"),
            }
        else:
            acc = self.accounts[uid_str]
            if auth_uid_str:
                acc["auth_uid"] = auth_uid_str
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level:
                acc["level"] = lvl_val
            if token:
                acc["token"] = token
            acc["current_exp"] = int(exp or 0)
            acc["gained_exp"] = max(0, int(exp or 0) - acc["initial_exp"])
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["likes"] = int(likes or 0)
            if not acc.get("actual_mode"):
                acc["mode"] = mode_info["mode"]
                acc["mode_label"] = mode_info["label"]
            acc["last_updated"] = time.strftime("%H:%M:%S")

        self.recalc_totals()

    # ---------- EXP Update ----------
    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return

        acc = self.accounts[uid_str]
        old_exp = acc["current_exp"]
        old_level = int(acc.get("level", 1))

        acc["current_exp"] = int(current_exp)
        if level is not None and level > 0:
            acc["level"] = int(level)

        new_level = int(acc["level"])
        acc["gained_exp"] = max(0, acc["current_exp"] - acc["initial_exp"])

        if not acc.get("actual_mode"):
            mode_info = get_game_mode(new_level)
            acc["mode"] = mode_info["mode"]
            acc["mode_label"] = mode_info["label"]

        prog = calculate_level_progress(new_level, acc["current_exp"])
        acc.update({
            "next_level": prog["next_level"],
            "remaining_exp": prog["remaining_exp"],
            "target_exp": prog["target_exp"],
            "needed_for_level": prog["needed_for_level"],
            "earned_in_level": prog["earned_in_level"],
            "progress_pct": prog["progress_pct"],
            "last_updated": time.strftime("%H:%M:%S"),
        })

        if new_level > old_level:
            self.log(
                f"🎉 LEVEL UP! {acc['nickname']} ({uid_str}) → Level {new_level}",
                "success",
                uid_str,
            )
            if old_level < 3 <= new_level:
                self.log(
                    f"🔄 MODE SWITCH! {acc['nickname']} → Lone Wolf unlocked",
                    "success",
                    uid_str,
                )

        diff = acc["current_exp"] - old_exp
        if diff > 0:
            self.log(
                f"★ {acc['nickname']} ({uid_str}) +{diff:,} EXP | "
                f"Lv{new_level} [{acc['mode']}] {prog['progress_pct']}% "
                f"({prog['remaining_exp']:,} to Lv{prog['next_level']})",
                "success",
                uid_str,
            )

        self.recalc_totals()

    # ---------- Match Mode Tracking ----------
    def set_match_mode(self, uid: str, match_type: str):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        mode_info = get_game_mode(
            self.accounts[uid_str].get("level", 1),
            match_type=match_type
        )
        self.accounts[uid_str]["mode"] = mode_info["mode"]
        self.accounts[uid_str]["mode_label"] = mode_info["label"]
        self.accounts[uid_str]["actual_mode"] = mode_info["mode"]
        self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    def clear_match_mode(self, uid: str):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        self.accounts[uid_str]["actual_mode"] = None
        lvl = self.accounts[uid_str].get("level", 1)
        mode_info = get_game_mode(lvl)
        self.accounts[uid_str]["mode"] = mode_info["mode"]
        self.accounts[uid_str]["mode_label"] = mode_info["label"]

    # ---------- Match Tracking ----------
    def increment_match_started(self):
        self.total_matches_started += 1

    def increment_match_finished(self, uid: str):
        self.total_matches_finished += 1
        uid_str = str(uid)
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_match_time"] = time.strftime("%H:%M:%S")
            self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")
            self.log(
                f"✅ Match #{self.accounts[uid_str]['matches_played']} COMPLETED "
                f"for {self.accounts[uid_str]['nickname']} ({uid_str})",
                "success",
                uid_str,
            )
        else:
            self.log(f"✅ Match COMPLETED (unknown uid: {uid_str})", "success", uid_str)

    def increment_match(self, uid: str):
        self.increment_match_finished(uid)

    # ---------- Status ----------
    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        if not self.is_paused(uid_str):
            self.accounts[uid_str]["status"] = status
        if active_matches is not None:
            self.accounts[uid_str]["active_matches"] = int(active_matches)
        self.accounts[uid_str]["last_updated"] = time.strftime("%H:%M:%S")

    # ---------- Pause ----------
    def is_paused(self, uid: str) -> bool:
        uid_str = str(uid)
        if uid_str in self.paused_accounts:
            return True
        game_id = self.auth_to_game_id.get(uid_str)
        if game_id and game_id in self.paused_accounts:
            return True
        auth_uid = self.game_to_auth_id.get(uid_str)
        if auth_uid and auth_uid in self.paused_accounts:
            return True
        acc = self.accounts.get(uid_str)
        if acc and acc.get("is_paused"):
            return True
        return False

    def toggle_pause(self, uid: str) -> bool:
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(self.auth_to_game_id[uid_str])
        if uid_str in self.game_to_auth_id:
            candidates.add(self.game_to_auth_id[uid_str])

        target_key = uid_str
        target_acc = None
        for c in candidates:
            if c in self.accounts:
                target_acc = self.accounts[c]
                target_key = c
                break

        is_now_paused = not self.is_paused(uid_str)

        if is_now_paused:
            for c in candidates:
                self.paused_accounts.add(c)
                self.close_writers_for_account(c)
            if target_acc:
                target_acc["is_paused"] = True
                target_acc["paused_at"] = time.time()
                target_acc["status"] = "PAUSED"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"⏸ {nick} ({target_key}) PAUSED", "warning", target_key)
        else:
            for c in candidates:
                self.paused_accounts.discard(c)
            if target_acc:
                target_acc["is_paused"] = False
                if target_acc.get("paused_at"):
                    dur = time.time() - target_acc["paused_at"]
                    target_acc["total_pause_duration"] = (
                        target_acc.get("total_pause_duration", 0.0) + dur
                    )
                    target_acc["paused_at"] = None
                target_acc["status"] = "ONLINE"
            nick = target_acc.get("nickname", target_key) if target_acc else target_key
            self.log(f"▶ {nick} ({target_key}) RESUMED", "success", target_key)

        if "on_pause_toggle" in self.refresh_callbacks:
            try:
                asyncio.create_task(
                    self.refresh_callbacks["on_pause_toggle"](target_key, is_now_paused)
                )
            except Exception:
                pass

        return is_now_paused

    def toggle_pause_all(self) -> bool:
        any_active = any(not self.is_paused(k) for k in list(self.accounts.keys()))
        for k in list(self.accounts.keys()):
            current_paused = self.is_paused(k)
            if any_active and not current_paused:
                self.toggle_pause(k)
            elif not any_active and current_paused:
                self.toggle_pause(k)
        return any_active

    # ---------- Uptime ----------
    def get_account_uptime(self, uid_str: str) -> int:
        acc = self.accounts.get(uid_str)
        if not acc:
            mapped = self.game_to_auth_id.get(uid_str) or self.auth_to_game_id.get(uid_str)
            if mapped:
                acc = self.accounts.get(mapped)
        if not acc:
            return 0
        start_t = acc.get("start_time", time.time())
        total_pause = acc.get("total_pause_duration", 0.0)
        if acc.get("is_paused") and acc.get("paused_at"):
            return max(0, int(acc["paused_at"] - start_t - total_pause))
        return max(0, int(time.time() - start_t - total_pause))

    # ---------- Totals & History ----------
    def recalc_totals(self):
        self.total_gained_exp = sum(int(a.get("gained_exp", 0)) for a in self.accounts.values())

    def push_exp_history(self):
        now = time.time()
        if now - self._last_history_push < 10:
            return
        self._last_history_push = now
        self.exp_history.append({
            "t": int(now),
            "exp": int(self.total_gained_exp),
        })

    def get_live_stats(self) -> Dict[str, Any]:
        total_active = sum(int(a.get("active_matches", 0)) for a in self.accounts.values())
        uptime_sec = max(1, int(time.time() - self.start_time))
        exp_per_hour = int((self.total_gained_exp / uptime_sec) * 3600)
        matches_per_hour = int((self.total_matches_started / uptime_sec) * 3600)
        matches_completed_per_hour = int((self.total_matches_finished / uptime_sec) * 3600)
        return {
            "total_accounts": len(self.accounts),
            "total_matches_started": self.total_matches_started,
            "total_matches_finished": self.total_matches_finished,
            "total_matches_completed": self.total_matches_finished,
            "matches_completed_per_hour": matches_completed_per_hour,
            "total_active_matches": total_active,
            "total_gained_exp": self.total_gained_exp,
            "exp_per_hour": exp_per_hour,
            "matches_per_hour": matches_per_hour,
            "uptime": uptime_sec,
        }


bot_state = BotState()


# ==================== HTTP HANDLERS ====================

FALLBACK_HTML = """<!DOCTYPE html>
<html><head><title>Loading...</title></head>
<body style="background:#061014;color:#67e8f9;font-family:system-ui;text-align:center;padding:60px;">
<h1>Loading Dashboard...</h1>
<p>templates/index.html not found</p>
</body></html>"""


async def handle_index(request: web.Request) -> web.Response:
    content = FALLBACK_HTML
    if os.path.exists(TEMPLATE_PATH):
        try:
            with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            pass
    return web.Response(text=content, content_type="text/html", charset="utf-8")


async def handle_get_stats(request: web.Request) -> web.Response:
    bot_state.push_exp_history()

    accounts_data = list(bot_state.accounts.values())
    accounts_data.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)

    for acc in accounts_data:
        uid_k = str(acc.get("uid", ""))
        acc["uptime_seconds"] = bot_state.get_account_uptime(uid_k)
        acc["is_paused"] = bot_state.is_paused(uid_k)
        if acc["is_paused"]:
            acc["status"] = "PAUSED"

    live = bot_state.get_live_stats()

    return web.json_response({
        **live,
        "accounts": accounts_data,
        "logs": bot_state.logs[-100:],
        "exp_history": list(bot_state.exp_history),
    })


async def handle_add_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({"status": "error", "error": "Invalid JSON"}, status=400)

        existing: List[Dict[str, Any]] = []
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except Exception:
                existing = []

        if data.get("uid") and data.get("password"):
            uid = str(data["uid"]).strip()
            pwd = str(data["password"]).strip()
            if not uid or not pwd:
                return web.json_response({"status": "error", "error": "UID & password required"})

            if uid in bot_state.account_workers:
                try:
                    bot_state.account_workers[uid].cancel()
                except Exception:
                    pass
                bot_state.account_workers.pop(uid, None)

            existing = [a for a in existing if str(a.get("uid", "")) != uid]
            existing.append({"uid": uid, "password": pwd})
            identifier = uid

        elif data.get("token"):
            token = str(data["token"]).strip()
            if not token:
                return web.json_response({"status": "error", "error": "Token required"})

            tok_key = token[:16]
            for k in list(bot_state.account_workers.keys()):
                if k == tok_key or k.startswith(tok_key[:10]):
                    try:
                        bot_state.account_workers[k].cancel()
                    except Exception:
                        pass
                    bot_state.account_workers.pop(k, None)

            existing = [a for a in existing if a.get("token") != token]
            existing.append({"token": token})
            identifier = f"Token_{token[:8]}..."

        else:
            return web.json_response({"status": "error", "error": "Invalid payload"}, status=400)

        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)

        bot_state.log(f"New account added: {identifier}", "success")

        cb = bot_state.refresh_callbacks.get("on_account_added")
        if cb:
            asyncio.create_task(cb(data))

        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        req_uid = str(data.get("uid", "")).strip()
        req_auth_uid = str(data.get("auth_uid", "")).strip()

        if not req_uid and not req_auth_uid:
            return web.json_response({"status": "error", "error": "UID required"}, status=400)

        candidates = {c for c in (req_uid, req_auth_uid) if c}
        for cid in list(candidates):
            if cid in bot_state.game_to_auth_id:
                candidates.add(bot_state.game_to_auth_id[cid])
            if cid in bot_state.auth_to_game_id:
                candidates.add(bot_state.auth_to_game_id[cid])

        target_tokens = set()
        for cid in list(candidates):
            acc = bot_state.accounts.get(cid) or {}
            if acc.get("auth_uid"):
                candidates.add(str(acc["auth_uid"]))
            if acc.get("uid"):
                candidates.add(str(acc["uid"]))
            if acc.get("token"):
                target_tokens.add(str(acc["token"]))

            cred = bot_state.account_credentials.get(cid) or {}
            if cred.get("auth_uid"):
                candidates.add(str(cred["auth_uid"]))
            if cred.get("account_id"):
                candidates.add(str(cred["account_id"]))
            for key in ("token", "access_token", "auth_token"):
                if cred.get(key):
                    target_tokens.add(str(cred[key]))

        cache_file = os.path.join(os.path.dirname(__file__), "token_cache.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                dirty = False
                for k in list(cache.keys()):
                    v = cache[k] if isinstance(cache[k], dict) else {}
                    if (
                        str(k) in candidates
                        or str(v.get("account_id", "")) in candidates
                        or str(v.get("auth_uid", "")) in candidates
                    ):
                        candidates.add(str(k))
                        candidates.add(str(v.get("account_id", "")))
                        candidates.add(str(v.get("auth_uid", "")))
                        del cache[k]
                        dirty = True
                if dirty:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        json.dump(cache, f, indent=2)
            except Exception:
                pass

        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                kept = []
                for acc in existing:
                    a_uid = str(acc.get("uid", "")).strip()
                    a_tok = str(acc.get("token", "")).strip()
                    if a_uid in candidates or a_tok in target_tokens:
                        continue
                    kept.append(acc)
                with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                    json.dump(kept, f, indent=2, ensure_ascii=False)
            except Exception:
                pass

        devices_file = os.path.join(os.path.dirname(__file__), "devices.json")
        if os.path.exists(devices_file):
            try:
                with open(devices_file, "r", encoding="utf-8") as f:
                    devices = json.load(f)
                dirty = False
                for k in list(devices.keys()):
                    if str(k) in candidates:
                        del devices[k]
                        dirty = True
                if dirty:
                    with open(devices_file, "w", encoding="utf-8") as f:
                        json.dump(devices, f, indent=4)
            except Exception:
                pass

        for cid in candidates:
            bot_state.accounts.pop(cid, None)
            bot_state.account_credentials.pop(cid, None)
            bot_state.auth_to_game_id.pop(cid, None)
            bot_state.game_to_auth_id.pop(cid, None)
            bot_state.account_token_map.pop(cid, None)
            bot_state.paused_accounts.discard(cid)

        to_cancel = []
        for k, worker in list(bot_state.account_workers.items()):
            if str(k) in candidates:
                to_cancel.append(k)
                try:
                    worker.cancel()
                except Exception:
                    pass
        for k in to_cancel:
            bot_state.account_workers.pop(k, None)

        for cid in candidates:
            bot_state.close_writers_for_account(cid)

        cb = bot_state.refresh_callbacks.get("on_account_deleted")
        if cb:
            try:
                asyncio.create_task(cb(list(candidates)))
            except Exception:
                pass

        bot_state.log(f"Account deleted: {req_uid or req_auth_uid}", "warning", req_uid)
        bot_state.recalc_totals()

        return web.json_response({"status": "ok", "deleted": list(candidates)})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def handle_refresh_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        cb = bot_state.refresh_callbacks.get("on_refresh_account")
        if cb:
            asyncio.create_task(cb(uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_restart_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        cb = bot_state.refresh_callbacks.get("on_restart_account")
        if cb:
            asyncio.create_task(cb(uid))
        else:
            cb2 = bot_state.refresh_callbacks.get("on_refresh_account")
            if cb2:
                asyncio.create_task(cb2(uid))
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_pause(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        if not uid:
            return web.json_response({"status": "error", "error": "UID required"})
        is_paused = bot_state.toggle_pause(uid)
        return web.json_response({"status": "ok", "is_paused": is_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_toggle_pause_all(request: web.Request) -> web.Response:
    try:
        all_paused = bot_state.toggle_pause_all()
        return web.json_response({"status": "ok", "all_paused": all_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})


async def handle_clear_logs(request: web.Request) -> web.Response:
    bot_state.logs.clear()
    return web.json_response({"status": "ok"})


# ==================== STALE ACCOUNT CLEANUP ====================

async def handle_stale_accounts(request: web.Request) -> web.Response:
    try:
        persisted: List[Dict[str, Any]] = []
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    persisted = json.load(f)
            except Exception:
                persisted = []

        result = []
        for acc in persisted:
            if not isinstance(acc, dict):
                continue
            acc_uid = str(acc.get("uid", "")).strip()
            acc_tok = str(acc.get("token", "")).strip()

            live = None
            for k, v in bot_state.accounts.items():
                if acc_uid and str(v.get("auth_uid", "")).strip() == acc_uid:
                    live = v
                    break
                if acc_uid and str(v.get("uid", "")).strip() == acc_uid:
                    live = v
                    break
                if acc_tok and str(v.get("token", "")).strip() == acc_tok:
                    live = v
                    break

            result.append({
                "uid": acc_uid or "(token-only)",
                "token_preview": (acc_tok[:16] + "...") if acc_tok else "",
                "has_live": live is not None,
                "live_status": live.get("status", "NOT_CONNECTED") if live else "NOT_CONNECTED",
                "live_nickname": live.get("nickname", "") if live else "",
            })

        return web.json_response({"status": "ok", "accounts": result})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def handle_cleanup_stale(request: web.Request) -> web.Response:
    try:
        if not os.path.exists(ACCOUNTS_FILE):
            return web.json_response({"status": "ok", "removed": 0, "removed_ids": []})

        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            persisted = json.load(f)

        live_ids = set()
        live_tokens = set()
        for k, v in bot_state.accounts.items():
            if v.get("auth_uid"):
                live_ids.add(str(v["auth_uid"]))
            if v.get("uid"):
                live_ids.add(str(v["uid"]))
            if v.get("token"):
                live_tokens.add(str(v["token"]))

        kept = []
        removed = 0
        removed_ids = []
        for acc in persisted:
            if not isinstance(acc, dict):
                continue
            acc_uid = str(acc.get("uid", "")).strip()
            acc_tok = str(acc.get("token", "")).strip()

            is_live = False
            if acc_uid and acc_uid in live_ids:
                is_live = True
            if acc_tok and acc_tok in live_tokens:
                is_live = True

            if is_live:
                kept.append(acc)
            else:
                removed += 1
                removed_ids.append(acc_uid or f"token:{acc_tok[:12]}")

        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(kept, f, indent=2, ensure_ascii=False)

        bot_state.log(f"🧹 Cleaned {removed} stale account(s)", "warning")
        return web.json_response({
            "status": "ok",
            "removed": removed,
            "removed_ids": removed_ids
        })
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)


async def start_web_dashboard(host: str = "0.0.0.0", port: int = 20333):
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/refresh", handle_refresh_account)
    app.router.add_post("/api/account/restart", handle_restart_account)
    app.router.add_post("/api/account/pause", handle_toggle_pause)
    app.router.add_post("/api/account/pause_all", handle_toggle_pause_all)
    app.router.add_post("/api/logs/clear", handle_clear_logs)
    app.router.add_get("/api/accounts/persisted", handle_stale_accounts)
    app.router.add_post("/api/accounts/cleanup_stale", handle_cleanup_stale)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner