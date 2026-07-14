"""反馈系统 — 用户反馈/建议/Bug/创意收集 + 后台管理面板

功能概览:
  提交 (群聊/私聊均可):
    反馈 <内容>            默认分类「其他」
    建议 <内容> / bug <内容> / 修改 <内容> / 创意 <内容>  按分类提交
    反馈                   (无内容) 发送 MD 格式的填写指引 + 快捷按钮

  查询:
    我的反馈               列出自己最近的反馈及处理状态
    查询反馈 <编号>        查看单条详情 (含管理员回复); 仅本人或管理员可查

  管理 (唯一管理员/框架主人):
    回复反馈 <编号> <内容>  回复后用户查询即可看到
    处理反馈 <编号> <状态>  状态: 待处理/处理中/已完成/已拒绝
    删除反馈 <编号>

  防刷:
    提交冷却 (默认60秒) / 每人每日上限 (默认5条) / 长度限制 (5-500字)
    内容过百度文本审核 (默认开启, 不合规拒绝提交)

  Web 面板:
    统计卡片 (全部/待处理/已回复), 反馈列表 (状态/分类筛选、分页),
    回复 / 修改 / 删除 / 状态变更, 常规设置
"""

import asyncio
import contextlib
import json
import os
import sqlite3
import time
from datetime import datetime

import aiohttp
from aiohttp import web

from core.base.config import cfg
from core.base.logger import PLUGIN, get_logger
from core.plugin.decorators import handler, on_load, on_unload
from core.plugin.web_pages import register_page, register_route, unregister_page

__plugin_meta__ = {
    'name': '反馈系统',
    'author': 'ElainaBot',
    'description': '用户反馈/建议/Bug/创意收集, 支持查询进度、管理员回复, 含 Web 管理面板',
    'version': '1.0.0',
    'github': 'https://github.com/miaolik/fkxt',
    'license': 'MIT',
}

log = get_logger(PLUGIN, '反馈系统')

# ==================== 路径 / 常量 ====================

_BASE = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.join(_BASE, 'data')
os.makedirs(_DATA_DIR, exist_ok=True)
_DB_PATH = os.path.join(_DATA_DIR, 'feedback.db')
_HTML_PATH = os.path.join(_BASE, 'panel.html')

_PAGE_KEY = 'feedback-system'
_API = '/api/ext/feedback'

PAGE_SIZE = 10

# 分类: 指令前缀 -> 分类名
TYPES = {'反馈': '其他', '建议': '建议', 'bug': 'Bug', 'Bug': 'Bug', 'BUG': 'Bug',
         '修改': '修改', '创意': '创意'}
TYPE_NAMES = ('建议', 'Bug', '修改', '创意', '其他')

# 状态
ST_PENDING = '待处理'
ST_DOING = '处理中'
ST_DONE = '已完成'
ST_REJECTED = '已拒绝'
STATUS_NAMES = (ST_PENDING, ST_DOING, ST_DONE, ST_REJECTED)

MIN_LEN = 5
MAX_LEN = 500

_DEFAULT_CONFIG = {
    'enabled': '1',              # 总开关
    'cooldown': '60',            # 提交冷却 (秒)
    'daily_limit': '5',          # 每人每日提交上限
    'censor_enabled': '1',       # 内容百度审核
    'super_admin': '',           # 唯一管理员 user_id (可回复/处理/删除任意反馈)
    'baidu_key': '',
    'baidu_secret': '',
}

_ICON = (
    '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" '
    'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>'
)

# ==================== SQLite (全异步) ====================

_conn_lock = asyncio.Lock()
_conn: sqlite3.Connection | None = None


def _ensure_db() -> sqlite3.Connection:
    """惰性创建连接并建表; 调用方须持有 _conn_lock。"""
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS feedbacks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT DEFAULT '',
                group_id   TEXT DEFAULT '',
                type       TEXT DEFAULT '其他',
                content    TEXT DEFAULT '',
                status     TEXT DEFAULT '待处理',
                reply      TEXT DEFAULT '',
                replied_at TEXT DEFAULT '',
                created_at TEXT DEFAULT ''
            );
            """
        )
        _conn.commit()
    return _conn


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# ---- config ----

async def _get_cfg(key: str, default: str = '') -> str:
    async with _conn_lock:
        row = _ensure_db().execute('SELECT value FROM config WHERE key=?', (key,)).fetchone()
    if row is None:
        return _DEFAULT_CONFIG.get(key, default)
    return row['value']


async def _set_cfg(key: str, value: str) -> None:
    async with _conn_lock:
        _ensure_db().execute(
            'INSERT INTO config (key, value) VALUES (?, ?) '
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (key, str(value)),
        )
        _ensure_db().commit()


async def _all_cfg() -> dict:
    out = dict(_DEFAULT_CONFIG)
    async with _conn_lock:
        rows = _ensure_db().execute('SELECT key, value FROM config').fetchall()
    for r in rows:
        out[r['key']] = r['value']
    return out


# ---- feedbacks ----

async def _add_feedback(user_id: str, group_id: str, ftype: str, content: str) -> int:
    async with _conn_lock:
        cur = _ensure_db().execute(
            'INSERT INTO feedbacks (user_id, group_id, type, content, status, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, group_id, ftype, content, ST_PENDING, _now()),
        )
        _ensure_db().commit()
        return cur.lastrowid


async def _get_feedback(fid: int) -> dict | None:
    async with _conn_lock:
        row = _ensure_db().execute('SELECT * FROM feedbacks WHERE id=?', (fid,)).fetchone()
    return dict(row) if row else None


async def _update_feedback(fid: int, **fields) -> bool:
    if not fields:
        return False
    keys = ', '.join(f'{k}=?' for k in fields)
    async with _conn_lock:
        cur = _ensure_db().execute(
            f'UPDATE feedbacks SET {keys} WHERE id=?', (*fields.values(), fid))
        _ensure_db().commit()
        return cur.rowcount > 0


async def _delete_feedback(fid: int) -> bool:
    async with _conn_lock:
        cur = _ensure_db().execute('DELETE FROM feedbacks WHERE id=?', (fid,))
        _ensure_db().commit()
        return cur.rowcount > 0


async def _list_feedbacks(limit: int, offset: int, status: str = '', ftype: str = '',
                          user_id: str = '', keyword: str = '') -> tuple[list, int]:
    where, args = [], []
    if status:
        if status == '已回复':
            where.append("reply != ''")
        elif status == '未回复':
            where.append("reply = ''")
        else:
            where.append('status=?')
            args.append(status)
    if ftype:
        where.append('type=?')
        args.append(ftype)
    if user_id:
        where.append('user_id=?')
        args.append(user_id)
    if keyword:
        where.append('(content LIKE ? OR reply LIKE ?)')
        args.extend([f'%{keyword}%', f'%{keyword}%'])
    cond = ('WHERE ' + ' AND '.join(where)) if where else ''
    async with _conn_lock:
        conn = _ensure_db()
        total = conn.execute(f'SELECT COUNT(*) AS c FROM feedbacks {cond}', args).fetchone()['c']
        rows = conn.execute(
            f'SELECT * FROM feedbacks {cond} ORDER BY id DESC LIMIT ? OFFSET ?',
            (*args, limit, offset),
        ).fetchall()
    return [dict(r) for r in rows], total


async def _feedback_stats() -> dict:
    async with _conn_lock:
        conn = _ensure_db()
        total = conn.execute('SELECT COUNT(*) AS c FROM feedbacks').fetchone()['c']
        pending = conn.execute(
            'SELECT COUNT(*) AS c FROM feedbacks WHERE status=?', (ST_PENDING,)).fetchone()['c']
        replied = conn.execute(
            "SELECT COUNT(*) AS c FROM feedbacks WHERE reply != ''").fetchone()['c']
    return {'total': total, 'pending': pending, 'replied': replied}


async def _user_today_count(user_id: str) -> int:
    today = datetime.now().strftime('%Y-%m-%d')
    async with _conn_lock:
        row = _ensure_db().execute(
            "SELECT COUNT(*) AS c FROM feedbacks WHERE user_id=? AND created_at LIKE ?",
            (user_id, f'{today}%'),
        ).fetchone()
    return row['c']


async def _user_last_time(user_id: str) -> str:
    async with _conn_lock:
        row = _ensure_db().execute(
            'SELECT created_at FROM feedbacks WHERE user_id=? ORDER BY id DESC LIMIT 1',
            (user_id,),
        ).fetchone()
    return row['created_at'] if row else ''


# ==================== 百度内容审核 ====================

_BAIDU_TOKEN_CACHE = {'token': '', 'exp': 0}

# 内置默认百度文本审核凭据, 面板填写后优先生效
_BAIDU_DEFAULT_KEY = 'CbiEkk2sYNG0x80tltUxJfKa'
_BAIDU_DEFAULT_SECRET = 'Wa6ZEBSatuN5QQ8C1A2hSOcNZb7FX1Fq'


async def _baidu_censor_token(key: str, secret: str) -> str:
    now = int(time.time())
    if _BAIDU_TOKEN_CACHE['token'] and _BAIDU_TOKEN_CACHE['exp'] > now + 60:
        return _BAIDU_TOKEN_CACHE['token']
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
        async with sess.post('https://aip.baidubce.com/oauth/2.0/token', params={
                'grant_type': 'client_credentials', 'client_id': key, 'client_secret': secret}) as r:
            data = await r.json(content_type=None)
    token = data.get('access_token', '')
    if token:
        _BAIDU_TOKEN_CACHE['token'] = token
        _BAIDU_TOKEN_CACHE['exp'] = now + int(data.get('expires_in', 2592000))
    return token


async def _censor_text(text: str) -> tuple[bool, str]:
    """内容审核: 返回 (是否通过, 原因)。未开启审核直接通过;
    审核异常/疑似按通过处理, 仅明确不合规拒绝。"""
    if await _get_cfg('censor_enabled', '1') != '1':
        return True, ''
    key = (await _get_cfg('baidu_key', '')) or _BAIDU_DEFAULT_KEY
    secret = (await _get_cfg('baidu_secret', '')) or _BAIDU_DEFAULT_SECRET
    if not key or not secret:
        return True, ''
    try:
        token = await _baidu_censor_token(key, secret)
        if not token:
            return True, ''
        url = 'https://aip.baidubce.com/rest/2.0/solution/v1/text_censor/v2/user_defined'
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
            async with sess.post(url, params={'access_token': token},
                                 data={'text': text[:1000]}) as r:
                data = await r.json(content_type=None)
        if data.get('conclusionType') == 2:
            items = data.get('data') or []
            reason = items[0].get('msg', '内容违规') if items else '内容违规'
            return False, reason
        return True, ''
    except Exception as exc:
        log.warning(f'百度内容审核调用失败: {exc}')
        return True, ''


# ==================== 权限 ====================

def _is_owner(event) -> bool:
    """框架主人 (owner_ids)。"""
    if not event.user_id:
        return False
    bot_cfg = cfg.get_bot_config(event.appid)
    return bool(bot_cfg) and event.user_id in (bot_cfg.get('owner_ids') or [])


async def _is_admin(event) -> bool:
    """唯一管理员 (面板配置) 或框架主人。"""
    super_admin = (await _get_cfg('super_admin', '')).strip()
    if super_admin and (event.user_id or '') == super_admin:
        return True
    return _is_owner(event)


# ==================== 指令 ====================

_HELP_MD = (
    '## 📮 反馈系统\n'
    '按以下格式直接发送即可提交：\n'
    '> **反馈** 你的内容 —— 一般反馈\n'
    '> **建议** 你的内容 —— 功能建议\n'
    '> **bug** 你的内容 —— 问题/故障上报\n'
    '> **修改** 你的内容 —— 修改请求\n'
    '> **创意** 你的内容 —— 新点子\n'
    '***\n'
    f'内容 {MIN_LEN}-{MAX_LEN} 字，提交后会返回编号。\n'
    '发送 **我的反馈** 查看进度，**查询反馈 编号** 查看详情和回复。'
)

_HELP_BUTTONS = [[
    {'text': '我的反馈', 'data': '我的反馈', 'enter': True},
]]


def _build_help_buttons():
    rows = []
    for row in _HELP_BUTTONS:
        btns = []
        for b in row:
            btns.append({'render_data': {'label': b['text'], 'style': 1},
                         'action': {'type': 2, 'permission': {'type': 2},
                                    'data': b['data'], 'enter': b.get('enter', True)}})
        rows.append({'buttons': btns})
    return {'content': {'rows': rows}} if rows else None


@handler(r'^(反馈|建议|bug|Bug|BUG|修改|创意)(?:\s+([\s\S]+))?$', name='提交反馈',
         desc='反馈/建议/bug/修改/创意 <内容> 提交反馈; 无内容时发送填写指引')
async def cmd_feedback(event, match):
    if await _get_cfg('enabled', '1') != '1':
        return
    prefix, content = match.group(1), (match.group(2) or '').strip()
    if not content:
        if prefix in ('反馈',):
            return await event.reply(_HELP_MD, skip_suffix=True)
        return
    uid = event.user_id or ''
    if not uid:
        return

    if len(content) < MIN_LEN:
        return await event.reply(f'⚠️ 内容太短啦，至少 {MIN_LEN} 字，说得越详细越好~')
    if len(content) > MAX_LEN:
        return await event.reply(f'⚠️ 内容超过 {MAX_LEN} 字上限，精简一下再发~')

    # 冷却
    cooldown = max(0, int(await _get_cfg('cooldown', '60') or 60))
    last = await _user_last_time(uid)
    if cooldown and last:
        with contextlib.suppress(ValueError):
            elapsed = (datetime.now() - datetime.strptime(last, '%Y-%m-%d %H:%M:%S')).total_seconds()
            if elapsed < cooldown:
                return await event.reply(f'⚠️ 提交太频繁了，{int(cooldown - elapsed)} 秒后再试~')

    # 每日上限
    daily = max(0, int(await _get_cfg('daily_limit', '5') or 5))
    if daily and await _user_today_count(uid) >= daily:
        return await event.reply(f'⚠️ 今天已提交 {daily} 条反馈啦，明天再来吧~')

    # 内容审核
    ok, reason = await _censor_text(content)
    if not ok:
        return await event.reply(f'⚠️ 内容未通过审核（{reason}），请修改后重新提交')

    ftype = TYPES.get(prefix, '其他')
    fid = await _add_feedback(uid, event.group_id or '', ftype, content)
    await event.reply(
        f'## ✅ 反馈提交成功\n'
        f'> 编号：**#{fid}**\n'
        f'> 分类：{ftype}\n'
        f'> 状态：{ST_PENDING}\n'
        '***\n'
        f'发送 **查询反馈 {fid}** 可查看处理进度和回复~',
        skip_suffix=True,
    )


@handler(r'^我的反馈$', name='我的反馈', desc='查看自己提交的反馈及处理状态')
async def cmd_my_feedbacks(event, match):
    uid = event.user_id or ''
    if not uid:
        return
    rows, total = await _list_feedbacks(10, 0, user_id=uid)
    if not rows:
        return await event.reply('你还没有提交过反馈哦，发送「反馈」查看提交方式~')
    lines = [f'## 📋 我的反馈（共 {total} 条）']
    for r in rows:
        flag = '💬' if r['reply'] else '⏳'
        lines.append(f"> {flag} **#{r['id']}** [{r['type']}] {r['status']} — "
                     f"{r['content'][:20]}{'…' if len(r['content']) > 20 else ''}")
    lines.append('***\n发送 **查询反馈 编号** 查看详情')
    await event.reply('\n'.join(lines), skip_suffix=True)


@handler(r'^查询反馈\s*(\d+)$', name='查询反馈', desc='查询反馈 <编号> 查看详情和回复')
async def cmd_query_feedback(event, match):
    fid = int(match.group(1))
    fb = await _get_feedback(fid)
    if not fb:
        return await event.reply(f'⚠️ 反馈 #{fid} 不存在')
    if fb['user_id'] != (event.user_id or '') and not await _is_admin(event):
        return await event.reply('⚠️ 只能查询自己提交的反馈哦')
    lines = [
        f'## 📮 反馈 #{fid}',
        f"> 分类：{fb['type']}",
        f"> 状态：{fb['status']}",
        f"> 时间：{fb['created_at']}",
        '***',
        fb['content'],
    ]
    if fb['reply']:
        lines += ['***', f"💬 **回复**（{fb['replied_at']}）：", fb['reply']]
    else:
        lines += ['***', '⏳ 暂未回复，请耐心等待~']
    await event.reply('\n'.join(lines), skip_suffix=True)


@handler(r'^回复反馈\s*(\d+)\s+([\s\S]+)$', name='回复反馈',
         desc='(管理员) 回复反馈 <编号> <内容>')
async def cmd_reply_feedback(event, match):
    if not await _is_admin(event):
        return
    fid, reply = int(match.group(1)), match.group(2).strip()
    fb = await _get_feedback(fid)
    if not fb:
        return await event.reply(f'⚠️ 反馈 #{fid} 不存在')
    await _update_feedback(fid, reply=reply, replied_at=_now(),
                           status=ST_DONE if fb['status'] == ST_PENDING else fb['status'])
    await event.reply(f'✅ 已回复反馈 #{fid}，用户发送「查询反馈 {fid}」即可看到')


@handler(r'^处理反馈\s*(\d+)\s+(待处理|处理中|已完成|已拒绝)$', name='处理反馈',
         desc='(管理员) 处理反馈 <编号> <状态>')
async def cmd_status_feedback(event, match):
    if not await _is_admin(event):
        return
    fid, status = int(match.group(1)), match.group(2)
    if not await _update_feedback(fid, status=status):
        return await event.reply(f'⚠️ 反馈 #{fid} 不存在')
    await event.reply(f'✅ 反馈 #{fid} 状态已改为「{status}」')


@handler(r'^设置反馈管理员\s+(\S+)$', name='设置反馈管理员',
         desc='设置反馈系统唯一管理员', owner_only=True)
async def cmd_set_admin(event, match):
    uid = match.group(1).strip()
    await _set_cfg('super_admin', uid)
    await event.reply(f'✅ 已设置反馈唯一管理员: {uid}')


@handler(r'^删除反馈\s*(\d+)$', name='删除反馈', desc='删除自己的反馈 (管理员可删任意)')
async def cmd_delete_feedback(event, match):
    fid = int(match.group(1))
    fb = await _get_feedback(fid)
    if not fb:
        return await event.reply(f'⚠️ 反馈 #{fid} 不存在')
    if fb['user_id'] != (event.user_id or '') and not await _is_admin(event):
        return await event.reply('⚠️ 只能删除自己提交的反馈哦')
    await _delete_feedback(fid)
    await event.reply(f'✅ 反馈 #{fid} 已删除')


# ==================== Web 面板接口 ====================

def _json(data, status=200):
    return web.json_response(data, status=status, dumps=lambda o: json.dumps(o, ensure_ascii=False, default=str))


async def _body(request):
    try:
        return await request.json()
    except Exception:
        return {}


@register_route('GET', f'{_API}/status')
async def api_status(request):
    cfg = await _all_cfg()
    stats = await _feedback_stats()
    return _json({
        'success': True,
        'data': {
            'enabled': cfg.get('enabled', '1') == '1',
            'cooldown': int(cfg.get('cooldown', '60') or 60),
            'daily_limit': int(cfg.get('daily_limit', '5') or 5),
            'censor_enabled': cfg.get('censor_enabled', '1') == '1',
            'super_admin': cfg.get('super_admin', ''),
            'baidu_key': cfg.get('baidu_key', ''),
            'baidu_secret': cfg.get('baidu_secret', ''),
            **stats,
        },
    })


@register_route('POST', f'{_API}/config')
async def api_config(request):
    body = await _body(request)
    if 'enabled' in body:
        await _set_cfg('enabled', '1' if body['enabled'] else '0')
    if 'censor_enabled' in body:
        await _set_cfg('censor_enabled', '1' if body['censor_enabled'] else '0')
    if 'cooldown' in body:
        try:
            cd = max(0, int(body['cooldown']))
        except (TypeError, ValueError):
            return _json({'success': False, 'message': 'cooldown 必须为非负整数'}, status=400)
        await _set_cfg('cooldown', str(cd))
    if 'daily_limit' in body:
        try:
            dl = max(0, int(body['daily_limit']))
        except (TypeError, ValueError):
            return _json({'success': False, 'message': 'daily_limit 必须为非负整数'}, status=400)
        await _set_cfg('daily_limit', str(dl))
    if 'super_admin' in body:
        await _set_cfg('super_admin', str(body['super_admin'] or '').strip())
    if 'baidu_key' in body:
        await _set_cfg('baidu_key', str(body['baidu_key'] or '').strip())
    if 'baidu_secret' in body:
        await _set_cfg('baidu_secret', str(body['baidu_secret'] or '').strip())
    return _json({'success': True, 'message': '已保存'})


@register_route('GET', f'{_API}/list')
async def api_list(request):
    try:
        page = max(1, int(request.query.get('page', '1')))
    except (TypeError, ValueError):
        page = 1
    status = request.query.get('status', '').strip()
    ftype = request.query.get('type', '').strip()
    keyword = request.query.get('q', '').strip()
    rows, total = await _list_feedbacks(PAGE_SIZE, (page - 1) * PAGE_SIZE,
                                        status=status, ftype=ftype, keyword=keyword)
    return _json({'success': True, 'data': rows, 'total': total,
                  'page': page, 'page_size': PAGE_SIZE})


@register_route('POST', f'{_API}/reply')
async def api_reply(request):
    body = await _body(request)
    try:
        fid = int(body.get('id'))
    except (TypeError, ValueError):
        return _json({'success': False, 'message': '参数不足'}, status=400)
    reply = str(body.get('reply') or '').strip()
    fb = await _get_feedback(fid)
    if not fb:
        return _json({'success': False, 'message': f'反馈 #{fid} 不存在'}, status=404)
    fields = {'reply': reply, 'replied_at': _now() if reply else ''}
    if reply and fb['status'] == ST_PENDING:
        fields['status'] = ST_DONE
    await _update_feedback(fid, **fields)
    return _json({'success': True, 'message': '已回复' if reply else '已清除回复'})


@register_route('POST', f'{_API}/update')
async def api_update(request):
    body = await _body(request)
    try:
        fid = int(body.get('id'))
    except (TypeError, ValueError):
        return _json({'success': False, 'message': '参数不足'}, status=400)
    fields = {}
    if 'content' in body:
        fields['content'] = str(body['content'] or '').strip()
    if 'type' in body:
        t = str(body['type'] or '').strip()
        if t not in TYPE_NAMES:
            return _json({'success': False, 'message': f'type 只能为 {"/".join(TYPE_NAMES)}'}, status=400)
        fields['type'] = t
    if 'status' in body:
        s = str(body['status'] or '').strip()
        if s not in STATUS_NAMES:
            return _json({'success': False, 'message': f'status 只能为 {"/".join(STATUS_NAMES)}'}, status=400)
        fields['status'] = s
    if not fields:
        return _json({'success': False, 'message': '无可更新字段'}, status=400)
    if not await _update_feedback(fid, **fields):
        return _json({'success': False, 'message': f'反馈 #{fid} 不存在'}, status=404)
    return _json({'success': True, 'message': '已更新'})


@register_route('POST', f'{_API}/delete')
async def api_delete(request):
    body = await _body(request)
    try:
        fid = int(body.get('id'))
    except (TypeError, ValueError):
        return _json({'success': False, 'message': '参数不足'}, status=400)
    if not await _delete_feedback(fid):
        return _json({'success': False, 'message': f'反馈 #{fid} 不存在'}, status=404)
    return _json({'success': True, 'message': '已删除'})


# ==================== 生命周期 ====================

@on_load
async def _init():
    async with _conn_lock:
        _ensure_db()
    register_page(
        key=_PAGE_KEY,
        label='反馈系统',
        source='plugin',
        source_name='反馈系统',
        icon=_ICON,
        html_file=_HTML_PATH,
    )
    log.info('反馈系统插件已加载')


@on_unload
def _cleanup():
    global _conn
    unregister_page(_PAGE_KEY)
    if _conn is not None:
        with contextlib.suppress(Exception):
            _conn.close()
        _conn = None
    log.info('反馈系统插件已卸载')
