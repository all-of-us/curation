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
| `source_ticket` | The ticket that introduced the label into the repo. |

`data_type` and `publicly_reportable` are transcribed from the `Categories` tab of the RT and CT 2024q3r3 suppression workbook attached to DC-3772, which is the only record of either. That tab also has a `Definition` column, and it is blank on every row, so the `definition` text here was written from the shipped concept lists rather than transcribed.

`source_ticket` is the ticket that first added the label, recovered from git history rather than from which cleaning rule reads the file today. The two differ: `ct_rt_publicly_reportable_suppressions.csv` was created by DC-3772 even though the DC-3749 rules read it. A later ticket may revise a label's rows without appearing here, and DC-3979 did exactly that to `perinatal` (124 rows to 41), `suffocation` (12 to 9), `liveborn_infants` (9 to 5) and `location` (181 to 183).

One row per distinct atomic label, currently 43. A cell holding a `;`-joined combination such as `liveborn; multiples` names two categories that each have their own row; the combination itself never gets one. `tests/unit_tests/data_steward/privacy_rule_definitions_test.py` fails if a CSV gains a label with no definition, or if a definition names a label no CSV uses.

### The three label conventions

One category name can appear under three spellings, one per source file:

| Convention | File | Example |
|------------|------|---------|
| Lowercase category name | `ct_additional_privacy_suppressions.csv` | `assault/homicide` |
| The supplying list's filename | `ct_rt_publicly_reportable_suppressions.csv` | `assault.csv` |
| Title-case prose | `ct_retroactive_privacy_suppression.csv` | `Assault/Homicide` |

`ct_observation_postcoordinated_suppressions.csv` follows the lowercase convention and contributes only `location`, which it shares with `ct_additional_privacy_suppressions.csv`.

Sharing a name does not make two labels the same category. `assault/homicide` is maltreatment recorded on the participant's own record, while `assault.csv` is the external-cause codes; `military_operations` is deployment as a family circumstance, while `military.csv` is injury caused by war operations. The retroactive file's `Legal Intervention` is custody and legal process, while `legal_intervention` and `legal.csv` are injury inflicted by law enforcement. The definitions file states each label separately rather than collapsing them, so match on the exact spelling and the file it came from.

### Three traps

`publicly_reportable` is not the same thing as membership of `ct_rt_publicly_reportable_suppressions.csv`. Six of the fifteen labels in that file are not flagged publicly reportable: `perinatal.csv` is flagged `no`, and `birth.csv`, `delivery-proc.csv`, `unknown COD.csv`, `06apr20 NIH List` and `old_spreadsheet_list` are `not stated`. Conversely, ten of the eighteen labels in `ct_additional_privacy_suppressions.csv` are flagged `yes`: `assault/homicide`, `date of birth`, `legal_intervention`, `liveborn_infants`, `military_operations`, `motor vehicle accident`, `operations_of_war`, `suffocation`, `suicide` and `terrorism`. Read the column, not the filename.

Three labels are not categories at all, and are defined so their absence does not read as an oversight: `06apr20 NIH List` is a dated cross-category delivery, `old_spreadsheet_list` has an unrecoverable source, and the empty label carries five ICD9CM rows that were never assigned a category.

`pd.read_csv` turns the empty label into `NaN`, in this file and in `ct_retroactive_privacy_suppression.csv` alike, and `NaN` never compares equal to itself. A `merge` on `privacy_rule` is safe, because pandas matches null keys to each other, but any equality test silently misses those rows: `df.privacy_rule == ''` and `df.privacy_rule.isin([...])` both return zero matches, as does `privacy_rule = ''` in BigQuery against a NULL. Filter with `.isna()`, read the file with `csv.DictReader`, which yields the empty string, or pass `keep_default_na=False`.
