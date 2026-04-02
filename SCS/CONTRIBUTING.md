# Contributing

## Repository Nature
This repository is currently organized as a structured SCS documentation-and-template package.
Its main assets are:
- docs
- manifests
- presets
- JSON templates
- bootstrap pack templates

## Recommended Contribution Order
When adding new material, follow this order:

1. Clarify whether the new file belongs to:
   - Core
   - Workbench
   - Deep Governance
   - Legacy & Migration
   - Auto Governance
   - Bootstrap Pack
2. Add or update the corresponding template or doc
3. Add pack-level or layer-level guidance if needed
4. Update navigation files when the new addition matters for discoverability

## Naming Guidance
- Keep JSON template names descriptive and stable
- Prefer layer-consistent naming
- Keep pack-specific templates inside `templates/bootstrap/<pack_name>/`

## Contribution Priorities
Highest priority:
- missing core templates
- missing pack-critical templates
- navigation and discoverability improvements

Second priority:
- new docs for practical usage
- preset refinement
- repository polish files

## Practical Rule
Do not add large amounts of new structure unless it clearly improves real project use.
Repository growth should follow practical value, not expansion for its own sake.
