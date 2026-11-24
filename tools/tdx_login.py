#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通达信客户端自动登录（Win32）。

通达信每次启动都会弹登录对话框，停在登录界面时 TQ 量化接口不可用
（表现为 "初始化错误：请确认是否打开通达信客户端"）。

对话框上的按钮是皮肤自绘的：真实的 Button 控件 vis=False，直接 BM_CLICK
不触发。用 WM_COMMAND(BN_CLICKED | ctrl_id) 让对话框走默认按钮处理才有效
（ctrl_id=1 即 IDOK，对应"登录"）。

已验证：PostMessage(dlg, WM_COMMAND, IDOK, hBtn) -> 窗口标题从
        "通达信金融终端 V7.72" 变为 "通达信金融终端 V7.72 - [行情-知识库]"
"""
import ctypes
from ctypes import wintypes

user32 = ctypes.WinDLL("user32", use_last_error=True)

EnumWindowsProc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

WM_COMMAND = 0x0111
BM_CLICK = 0x00F5
BN_CLICKED = 0x0000
IDOK = 1
IDCANCEL = 2

# 登录对话框上的默认按钮文字（"登录"）
BTN_LOGIN = "登录"
# 主窗口标题里出现这个片段，说明已经进到行情界面（而不是停在登录框）
LOGGED_IN_MARK = " - ["

user32.GetClassNameW.restype = wintypes.INT
user32.GetWindowTextW.restype = wintypes.INT
user32.GetDlgCtrlID.restype = ctypes.c_int
user32.PostMessageW.restype = wintypes.BOOL
user32.IsWindowVisible.restype = wintypes.BOOL


def _window_text(hwnd):
    buf = ctypes.create_unicode_buffer(512)
    user32.GetWindowTextW(hwnd, buf, 512)
    return buf.value


def _class_name(hwnd):
    buf = ctypes.create_unicode_buffer(256)
    user32.GetClassNameW(hwnd, buf, 256)
    return buf.value


def _pid_of(hwnd):
    pid = wintypes.DWORD()
    user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
    return pid.value


def _top_windows(pid):
    found = []

    def cb(hwnd, _):
        if _pid_of(hwnd) == pid:
            found.append(hwnd)
        return True

    user32.EnumWindows(EnumWindowsProc(cb), 0)
    return found


def _children(hwnd):
    found = []

    def cb(child, _):
        found.append(child)
        return True

    user32.EnumChildWindows(hwnd, EnumWindowsProc(cb), 0)
    return found


def main_window_title(pid):
    """通达信主窗口标题（取最长/最可见的那个）"""
    best = ""
    for hwnd in _top_windows(pid):
        if not user32.IsWindowVisible(hwnd):
            continue
        title = _window_text(hwnd)
        if len(title) > len(best):
            best = title
    return best


def is_logged_in(pid):
    """已经进到行情界面（不是停在登录框）"""
    return LOGGED_IN_MARK in main_window_title(pid)


def find_login_button(pid):
    """在通达信的对话框里找"登录"按钮 -> (dlg_hwnd, btn_hwnd, ctrl_id)"""
    for hwnd in _top_windows(pid):
        if _class_name(hwnd) != "#32770":
            continue
        for child in _children(hwnd):
            if _class_name(child) != "Button":
                continue
            if _window_text(child) == BTN_LOGIN:
                return hwnd, child, user32.GetDlgCtrlID(child)
    return None


def click_login(pid):
    """触发登录对话框的默认按钮。

    Returns:
        "clicked"  - 已触发
        "no_dialog"- 没找到登录对话框
        "failed"   - 找到但消息投递失败
    """
    found = find_login_button(pid)
    if not found:
        return "no_dialog"
    dlg, btn, ctrl_id = found
    if ctrl_id <= 0:
        ctrl_id = IDOK
    wparam = (BN_CLICKED << 16) | (ctrl_id & 0xFFFF)
    ok = user32.PostMessageW(dlg, WM_COMMAND, wparam, btn)
    if not ok:
        # 退路：直接 BM_CLICK
        ok = user32.PostMessageW(btn, BM_CLICK, 0, 0)
    return "clicked" if ok else "failed"


def ensure_login(pid, wait_seconds=60):
    """确保通达信登录到位。

    Returns:
        "already" - 本来就已登录
        "clicked" - 已点登录，等待中
        "timeout" - 点击后等待超时仍未进入行情界面
        "no_dialog"- 没有登录对话框（可能正在加载）
    """
    if is_logged_in(pid):
        return "already"
    result = click_login(pid)
    if result != "clicked":
        return result
    import time
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        time.sleep(3)
        if is_logged_in(pid):
            return "clicked"
    return "timeout"


if __name__ == "__main__":
    import subprocess
    import sys

    out = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq TdxW.exe", "/NH"],
        capture_output=True, text=True,
        # 不弹控制台窗口（tasklist 是控制台程序，见 _NO_WINDOW 注释）
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ).stdout
    pids = [int(x) for x in "".join(c if c.isdigit() else " " for c in out).split()]
    if not pids:
        print("NO_TDX_PROCESS")
        sys.exit(1)
    pid = pids[0]
    print("pid =", pid, "| title =", main_window_title(pid))
    print("ensure_login ->", ensure_login(pid))
    print("title =", main_window_title(pid))
