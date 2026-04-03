# V1.1d Prose Execution Layer

## 目标

这一层不管“这一章写什么”，而是管“这一章怎么写”。

它专门拦截以下问题：
- 总结式收束
- 作者跳出来替读者理解
- 概念先于场面
- 高压场景里的过量设定讲解
- 章末模板钩子
- 文风发老、解释欲过重、动作推进不足

## 层级定位

建议作为：
- `V1.1d Prose Execution Layer`

接入顺序：

```text
chapter_contract
→ chapter_run_packet
→ writer_draft
→ prose_execution_gate
→ reviewer_review
→ rewrite
→ ending_hook_gate
→ archive
```

## 核心模块

- `prose_execution_gate.json`
- `ending_hook_gate.json`

## 核心规则

1. 高压动作段中，解释句连续超过 2 句即判危险
2. 单段必须至少有一个动作推进、变化或风险落点
3. 旁白不能替读者总结意义
4. 章末禁止使用总结式模板尾句
5. 概念必须晚于场面，先让读者看到，再让读者明白
6. 人物认知不能超出当前生死场景的承受边界

## 适用场景

尤其适合：
- 高概念网文
- 大设定长篇
- 容易写成说明书的规则战/体系战
- 容易章末上价值、解释上头的写作场景
