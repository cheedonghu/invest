"""DeepSeek LLM 客户端。

原基于 hello-agents 框架的实现已移除，待自行实现。
API key 与模型 ID 从 backend/app/config/config.toml 的 [agent] 段读取：

    from backend.app.config.config import config
    config.agent["DEEPSEEK_API_KEY"]
    config.agent["LLM_MODEL_ID"]
"""

from __future__ import annotations
