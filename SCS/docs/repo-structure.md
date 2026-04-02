# SCS 仓库结构

```text
SCS/
├─ README.md
├─ docs/
│  ├─ architecture.md
│  ├─ getting-started.md
│  ├─ recommended-stacks.md
│  ├─ route-layer.md
│  ├─ workbench-layer.md
│  ├─ deep-governance.md
│  ├─ legacy-migration.md
│  ├─ auto-governance.md
│  └─ auto-governance-runtime.md
├─ templates/
│  ├─ 核心基础模板
│  ├─ 工作台模板
│  ├─ 深层治理模板
│  ├─ 旧项目迁移模板
│  └─ 自动治理模板
├─ presets/
│  ├─ new_project_standard.json
│  ├─ new_project_complex.json
│  ├─ legacy_project_migration.json
│  └─ full_governance.json
└─ manifests/
   ├─ repo_manifest.json
   └─ publish_manifest_example.json
```

## 说明

- `docs/`：方法论、层级设计、推荐实战栈
- `templates/`：可直接复制的 JSON 起始模板
- `presets/`：不同实战包的推荐模块组合
- `manifests/`：仓库元信息和发布示例文件
