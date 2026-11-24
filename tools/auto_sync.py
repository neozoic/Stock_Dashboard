r"""
auto_sync.py - 监控 Stock_Dashboard 目录,变动时自动 commit + push 到 GitHub + Gitee

用法:
    python tools/auto_sync.py
    或后台: tools\install_task.bat (注册到 Task Scheduler,开机自启)
    或单次: tools\sync_once.bat  (手动触发一次 sync)

触发阈值(可改):
    MIN_FILES = 1   # 至少 1 个文件变动
    MIN_LINES = 10  # 或至少 10 行 diff
DEBOUNCE_SECONDS = 300  # 文件变更后 5 分钟无新变动才触发 commit
PUSH_REMOTES = ["github", "gitee"]
"""
import os
import subprocess
import time
import datetime
import sys
from pathlib import Path

# 控制台程序(git/tasklist/netstat/taskkill/powershell)在 pythonw 父进程下
# 会自己开一个黑色控制台窗口并抢焦点(2026-11-23 实测: auto_sync 每 30 秒
# 弹一次)。CREATE_NO_WINDOW 让子进程静默运行，不占焦点。
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)

PROJECT_DIR = str(Path(__file__).resolve().parent.parent)
MIN_FILES = 1
MIN_LINES = 10
DEBOUNCE_SECONDS = 300
PUSH_REMOTES = ["github", "gitee"]
CHECK_INTERVAL = 30
LOG_FILE = os.path.join(PROJECT_DIR, "logs", "auto_sync.log")
LOCK_FILE = os.path.join(PROJECT_DIR, "logs", "auto_sync.lock")


def acquire_single_instance_lock():
    """单实例锁：install_task / start_sync / sync_once 三入口不可并存（git 操作会互踩）。
    拿到锁返回文件句柄（进程退出自动释放）；已有实例在跑则返回 None。"""
    import msvcrt
    os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
    fh = open(LOCK_FILE, "a+b")
    try:
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        return fh
    except OSError:
        fh.close()
        return None


def log(msg):
    line = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def run_git(args):
    r = subprocess.run(
        ["git", "-C", PROJECT_DIR] + args,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        creationflags=_NO_WINDOW,
    )
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def get_pending_changes():
    """返回 (file_count, line_diff, has_changes); has_changes=False 表示无变动。"""
    code, out, _ = run_git(["status", "--porcelain"])
    if code != 0 or not out:
        return 0, 0, False
    files = [l for l in out.splitlines() if l.strip()]
    file_count = len(files)
    # 计算 staged + unstaged 的 line diff
    code, out, _ = run_git(["diff", "--numstat"])
    line_diff = 0
    for l in out.splitlines():
        parts = l.split()
        if len(parts) >= 2:
            try:
                # 'X Y file' 中的 X (added) + Y (deleted) 视为 diff
                line_diff += int(parts[0]) + int(parts[1])
            except ValueError:
                pass
    return file_count, line_diff, True


def is_significant(file_count, line_diff):
    return file_count >= MIN_FILES or line_diff >= MIN_LINES


def commit_and_push():
    file_count, line_diff, _ = get_pending_changes()
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"auto-sync: {ts} ({file_count} files, {line_diff} lines)"
    log(f"commit: {msg}")

    code, _, err = run_git(["add", "-A"])
    if code != 0:
        log(f"add 失败: {err}")
        return False
    # "nothing to commit" 在 stdout 而非 stderr；改用 diff --cached --quiet 判空（0=无暂存变动）
    code, _, _ = run_git(["diff", "--cached", "--quiet"])
    if code == 0:
        log("无暂存变动,跳过")
        return False
    code, out, err = run_git(["commit", "-m", msg])
    if code != 0:
        log(f"commit 失败: {err or out}")
        return False

    for remote in PUSH_REMOTES:
        log(f"push {remote} ...")
        code, out, err = run_git(["push", remote, "main"])
        if code == 0:
            log(f"  -> {remote} 推送成功")
        else:
            log(f"  -> {remote} 推送失败: {err[:200]}")
    return True


def remote_ahead_count(remote):
    """本地 main 比远端领先几个 commit；拿不到远端信息时返回 None"""
    code, out, _ = run_git(["rev-list", "--count", f"{remote}/main..main"])
    if code == 0:
        try:
            return int(out.strip() or 0)
        except ValueError:
            return None
    # 本地没有 remote/main 引用时，用 ls-remote 兜底
    code, out, _ = run_git(["ls-remote", remote, "refs/heads/main"])
    if code != 0 or not out.strip():
        return None
    remote_sha = out.split()[0]
    code, out2, _ = run_git(["rev-list", "--count", f"{remote_sha}..main"])
    if code != 0:
        return None
    try:
        return int(out2.strip() or 0)
    except ValueError:
        return None


def push_unpushed():
    """推送"已提交但未推送"的 commit。

    auto_sync 只在检测到未提交改动时才 commit+push, 所以手工 commit 的
    commit 会被漏掉, 一直卡在本地。这里单独补一次推送。
    """
    pushed = False
    for remote in PUSH_REMOTES:
        n = remote_ahead_count(remote)
        if not n:
            continue
        log(f"push {remote} ({n} 个已提交未推送)")
        code, out, err = run_git(["push", remote, "main"])
        if code == 0:
            log(f"  -> {remote} 推送成功")
            pushed = True
        else:
            log(f"  -> {remote} 推送失败: {err[:200]}")
    return pushed


def main_loop():
    log(f"auto_sync 启动: 监控 {PROJECT_DIR}")
    log(f"  阈值: files >= {MIN_FILES} 或 lines >= {MIN_LINES}, debounce {DEBOUNCE_SECONDS}s")

    last_change_time = None
    last_summary = (0, 0)
    idle_ticks = 0

    try:
        while True:
            fc, ld, has = get_pending_changes()
            if has:
                if (fc, ld) != last_summary:
                    log(f"检测到变动: {fc} 文件, {ld} 行 (待 debounce)")
                    last_summary = (fc, ld)
                last_change_time = time.time()
                idle_ticks = 0
            else:
                # 无变动
                if last_change_time and (time.time() - last_change_time) > DEBOUNCE_SECONDS:
                    if is_significant(*last_summary):
                        log(f"debounce 已过({DEBOUNCE_SECONDS}s),提交 {last_summary[0]} 文件 / {last_summary[1]} 行")
                        commit_and_push()
                    else:
                        log(f"变动未达阈值,跳过: {last_summary}")
                    last_change_time = None
                    last_summary = (0, 0)
                else:
                    # 空闲时顺手补推"已提交未推送"的 commit (约每 5 分钟一次)
                    idle_ticks += 1
                    if idle_ticks % 10 == 0:
                        push_unpushed()
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        log("用户中断,退出")
        sys.exit(0)


if __name__ == "__main__":
    _lock = acquire_single_instance_lock()
    if _lock is None:
        log("已有 auto_sync 实例在运行，本次退出（单实例保护）")
        sys.exit(0)
    if "--once" in sys.argv:
        # 单次模式:立即提交并推送
        log("单次同步模式(--once)")
        fc, ld, has = get_pending_changes()
        if has and is_significant(fc, ld):
            commit_and_push()
        elif has:
            log(f"变动未达阈值(已配置: files>={MIN_FILES} lines>={MIN_LINES}): {fc} 文件 / {ld} 行,跳过")
        else:
            log("无变动")
        # 补推已提交但未推送的 commit
        push_unpushed()
    else:
        main_loop()
