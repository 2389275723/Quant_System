# Runtime Model Integration

## 目标

给 SCS 增加一层可切换的多模型接入能力。

## 当前目录

- `runtime/model_registry.json`
- `runtime/role_model_map.json`
- `runtime/routing_policy.json`
- `runtime/providers/openai.json`
- `runtime/providers/anthropic.json`
- `runtime/providers/google_gemini.json`
- `runtime/providers/openai_compatible.json`
- `runtime/providers/openrouter.json`
- `runtime/providers/newapi.json`
- `.env.example`

## 推荐分工

### 总控 / 终局 / 稽核 / 深层治理
优先使用：
- 长上下文强
- 逻辑稳定
- 不容易乱编的模型

### 正文写作 / 润色
优先使用：
- 文笔更强
- 改写更顺
- 情绪推进更自然的模型

### 低成本辅助步骤
优先使用：
- 摘要
- 格式化
- 索引整理
- 自动汇总

## 当前新书最高级建议栈

- `V1.0`
- `V1.1a`
- `V1.1b`
- `V1.1c`
- `V1.2b`

新书默认不带 `V1.2a`。
