"""
.env 加载器
===========

把仓库根目录的 `.env` 文件读进 `os.environ`。

为什么需要它：
  - 看板里 `DASHBOARD_TOKEN` / `DASHBOARD_PORT` / `FEISHU_*` 这些配置项
    以前只能靠"启动前 set 环境变量"或"改 start_dashboard.bat"来给，
    而 keepalive 是用计划任务起 pythonw 的，没法在里面 set。
  - `.env` 已经被 .gitignore 排除（.gitignore:12-16），是放密钥的正确位置。

优先级：
  真正的环境变量 > .env 里的值。
  （与 start_dashboard.bat 里 `if not defined` 的语义一致 —— 外部
   已经给过的值不被覆盖。）

安全性：
  - load_env() 只返回**键名集合**，不返回值，避免密钥被误打进日志。
  - 本模块导入时不做任何事（没有副作用），只在显式调用 load_env() 时才读文件。

用法：
    import env_loader
    env_loader.load_env()
"""

import os

__all__ = ["parse_env_text", "load_env", "env_file_path"]


def env_file_path() -> str:
    """.env 的默认位置：与本文件同级的 .env（相对 __file__ 定位，不写死绝对路径）"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def parse_env_text(text: str) -> dict:
    """解析 .env 文本 → {KEY: VALUE}

    规则：
      - 空行、# 开头的行跳过（# 在行首，前面可有空白）
      - KEY=VALUE，按第一个 = 切分；KEY 两侧空白剥掉
      - VALUE 两侧空白剥掉
      - VALUE 若整体被一对引号包住（'...' 或 "..."），剥掉这对引号
      - 没有 = 的行跳过（不报错，.env 常被人手写坏）
      - 不做变量展开（$VAR 不解析）—— 避免顺序依赖

    同一文件里重复出现的 KEY，后者覆盖前者。
    """
    out = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        out[key] = value
    return out


def load_env(path: str = None, override: bool = False) -> set:
    """把 .env 读进 os.environ，返回**本次实际写入的键名集合**

    参数：
      path    .env 路径；默认取同目录的 .env
      override 为 True 时覆盖已存在的环境变量；默认 False（已存在的优先）

    行为：
      - 文件不存在 → 返回空集合，**不报错**（.env 是可选的）
      - 值为空的键会被跳过（`KEY=` 视作未配置）—— 免得用空串覆盖掉
        本来就有的环境变量
    """
    target = path or env_file_path()
    try:
        with open(target, "r", encoding="utf-8") as fh:
            text = fh.read()
    except FileNotFoundError:
        return set()
    except OSError:
        return set()

    loaded = set()
    for key, value in parse_env_text(text).items():
        if not value:
            continue
        if not override and key in os.environ:
            continue
        os.environ[key] = value
        loaded.add(key)
    return loaded
