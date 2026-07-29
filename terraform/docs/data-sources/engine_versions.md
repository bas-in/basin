# basin_engine_versions (Data Source)

List of engine versions in the catalog (channels, recommended flag,
EOL dates).

## Example Usage

```hcl
data "basin_engine_versions" "catalog" {}

output "recommended" {
  value = [for v in data.basin_engine_versions.catalog.versions : v.version if v.is_recommended]
}
```

## Attribute Reference

- `versions` — list of objects with `version`, `channel`,
  `released_at`, `eol_at`, `release_notes_url`, `is_recommended`,
  `notes`.
