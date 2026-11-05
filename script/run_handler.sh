#!/bin/bash
# 手动触发单个处理器（在部署目录或任意目录执行均可）
#
# 用法:
#   ./run_handler.sh                    列出可用处理器
#   ./run_handler.sh <名称>             执行指定处理器
#
# 名称支持两种写法:
#   模块名:    delete_workflow / split_workflow / migrate_workflow ...
#   类名小写:  deleteworkflowhandler / splitworkflowhandler ...
#
# 注意: 仍会检查 enabled 和 database 配置，未启用或配置错误不会执行。

docker exec -i orderhisclean python - "$1" <<'PYEOF'
import sys
for p in ("src", "/app/src"):
    if p not in sys.path:
        sys.path.insert(0, p)

from job_scheduler import _build_handler, _discover_handlers

target = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
classes = _discover_handlers()

if not target:
    print("可用处理器 (模块名 -> 类名小写):")
    for cls in classes:
        print(f"  {cls.__module__} -> {cls.__name__.lower()}")
    sys.exit(0)

matched = [c for c in classes
           if c.__name__.lower() == target or c.__module__.lower() == target]
if not matched:
    print(f"未找到处理器: {target}")
    sys.exit(1)

handler = _build_handler(matched[0])
if handler is None:
    print(f"{matched[0].__name__} 未启用或配置不完整（enabled/database），未执行")
    sys.exit(1)

handler.run()
PYEOF
