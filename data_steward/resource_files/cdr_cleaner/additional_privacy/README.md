## Changes Overview

A new boolean column `ct_plus_suppressed` has been added to the following CSV files:

### Modified Files

| File Name |
|-----------|
| `ct_additional_privacy_suppressions.csv` |
| `ct_rt_publicly_reportable_suppressions.csv` |
| `ct_observation_postcoordinated_suppressions.csv` |
| `ct_retroactive_privacy_suppression.csv` |

### Column Details

| Property | Value |
|----------|-------|
| **Column Name** | `ct_plus_suppressed` |
| **Data Type** | Boolean |
| **Initial Value** | `true` for all rows |
| **Behavior Impact** | Behavior-neutral by construction |

### Additional Notes

> ⚠️ **Important**: Even though the column value is `true`, data remains suppressed in CT+. A companion ticket is responsible for flipping the CT+ expanded categories to `false`.

## Privacy rule definitions

`privacy_rule_definitions.csv` records what each `privacy_rule` label in the four CT-consumed CSVs means. Look a category up there before inferring it from a filename or a concept list.

| Column | Meaning |
|--------|---------|
| `privacy_rule` | The label exactly as it appears in a CT-consumed CSV. |
| `data_type` | `EHR` or `PPI`, the kind of data the label applies to. |
| `definition` | Plain-language description of what the label covers. |
| `publicly_reportable` | `yes`, `no` or `not stated`, as supplied by the privacy team. |
| `source_ticket` | The ticket whose attachment supplied the label's concept rows. |

One row per distinct atomic label, currently 43. A cell holding a `;`-joined combination such as `liveborn; multiples` names two categories that each have their own row; the combination itself never gets one. `tests/unit_tests/data_steward/privacy_rule_definitions_test.py` fails if a CSV gains a label with no definition, or if a definition names a label no CSV uses.

### The three label conventions

A single category can appear under three spellings, one per source file, and they are the same category:

| Convention | File | Example |
|------------|------|---------|
| Lowercase category name | `ct_additional_privacy_suppressions.csv` | `assault/homicide` |
| The supplying list's filename | `ct_rt_publicly_reportable_suppressions.csv` | `assault.csv` |
| Title-case prose | `ct_retroactive_privacy_suppression.csv` | `Assault/Homicide` |

`ct_observation_postcoordinated_suppressions.csv` follows the lowercase convention and contributes only `location`, which it shares with `ct_additional_privacy_suppressions.csv`.

Same-category labels are not the same concept list. `assault/homicide` is maltreatment recorded on the participant's own record, while `assault.csv` is the external-cause codes; `military_operations` is deployment as a family circumstance, while `military.csv` is injury caused by war operations. The definitions file states each one separately rather than collapsing them.

### Three traps

`publicly_reportable` is not the same thing as membership of `ct_rt_publicly_reportable_suppressions.csv`. Four labels in that file (`birth.csv`, `delivery-proc.csv`, `perinatal.csv`, `unknown COD.csv`) are not flagged publicly reportable, and several labels that are flagged (`assault/homicide`, `suicide`, `terrorism`) sit in `ct_additional_privacy_suppressions.csv` instead. Read the column, not the filename.

Three labels are not categories at all, and are defined so their absence does not read as an oversight: `06apr20 NIH List` is a dated cross-category delivery, `old_spreadsheet_list` has an unrecoverable source, and the empty label carries five ICD9CM rows that were never assigned a category.

`pd.read_csv` turns the empty label into `NaN`, in this file and in `ct_retroactive_privacy_suppression.csv` alike, and `NaN` never compares equal to itself. A pandas merge on `privacy_rule` therefore drops that category silently. Read these files with `csv.DictReader`, which yields the empty string, or pass `keep_default_na=False`.
