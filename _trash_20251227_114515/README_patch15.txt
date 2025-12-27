Patch15 说明（必看）
================

你现在的 config.yaml 里同时出现了：
- universe.exclude_prefixes: []   （=允许300/688）
- universe.exclude_markets: [STAR, GEM] （=想排除科创/创业）

旧代码只认 exclude_prefixes，导致“你明明写了 exclude_markets 但还是没过滤”。

本补丁做了两件事：
1) Night Job 读取配置时：如果 exclude_prefixes 为空，但 exclude_markets 有 STAR/GEM，则自动转换为 prefixes（STAR->688/689, GEM->300/301）进行过滤。
2) 系统设置页保存时：同时写入 exclude_prefixes & exclude_markets，避免再次出现“显示/实际不一致”。

应用后你应该看到：
- 科创/创业/BJ 在 Night Job 的 picks 中不再出现
- research/picks_daily.csv 一定会生成（Night Job 完成后）

如何确认过滤生效？
- 打开 research/picks_daily.csv，检查是否存在 ts_code 以 300/301/688/689 开头或后缀 .BJ 的股票（应该没有）
