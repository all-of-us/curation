## The `ct_plus_suppressed` column

`ct_plus_suppressed` marks whether a concept stays suppressed in Controlled Tier Plus. DL-2419 added it with every row `true`, and DL-2421 set the CT+ expanded categories to `false`. Only the CT+ rule variants read it, so CT and RT behaviour is unchanged.

| File | Rows | `false` |
|------|------|---------|
| `ct_additional_privacy_suppressions.csv` | 1,829 | 321 |
| `ct_rt_publicly_reportable_suppressions.csv` | 10,921 | 10,323 |
| `ct_observation_postcoordinated_suppressions.csv` | 5 | 0 |
| `ct_retroactive_privacy_suppression.csv` | 40 | 0 |

The RT files and `ct_nph_observation_concept_suppressions.csv` do not carry the column.

### Expanded categories

The two files label the same element differently: `ct_additional_privacy_suppressions.csv` uses a category name, `ct_rt_publicly_reportable_suppressions.csv` the filename of the list it came from. An element is only released when both labels are flipped.

| Data element | Category name | Source filename | `false` rows |
|---|---|---|---|
| Assault and homicide | `assault/homicide` | `assault.csv` | 203 + 700 |
| Motor vehicle accident | `motor vehicle accident` | `vehicle.csv` | 37 + 6,773 |
| Military operations and operations of war | `military_operations`, `operations_of_war` | `military.csv` | 27 + 2 + 1,474 |
| Legal intervention | `legal_intervention` | `legal.csv` | 23 + 346 |
| Drowning | | `drowning.csv` | 304 |
| Suffocation | `suffocation` | `suffocation.csv` | 9 + 236 |
| Suicide | `suicide` | `suicide.csv` | 15 + 119 |
| Terrorism | `terrorism` | `terrorism.csv` | 3 + 167 |
| Multiple gestation | `multiples` | | 2 |
| Prolonged weightlessness | | `space.csv` | 10 |
| Diagnosis codes subject to public knowledge | | `unknown COD.csv` | 165 |

`unknown COD.csv` holds 152 ICD10 `S06.x` codes for traumatic brain injury with death before regaining consciousness and 13 death of unknown or ill-defined cause codes; the registered tier privacy rules file the whole list under codes subject to public knowledge, which CT+ includes. `operations_of_war` keeps its own label although it shares `military_operations`'s disposition, so the decision can be reversed per label.

### What stays `true`

- **Multi-category rows.** A `;`-separated `privacy_rule` is `false` only when every category in it is expanded. None of the nine combinations in these files qualifies (for example `liveborn; multiples`), so all stay `true`.
- **Suppressed in CT+.** `abortion`, at every tier, and free text.
- **Released through a linked dataset, not this column.** `location` ships as Zip5; `liveborn`, `liveborn_infants`, `birth.csv`, `delivery-proc.csv`, `perinatal`, `perinatal.csv` and `stillborn` ship with date of birth.
- **`06apr20 NIH List` (34 rows)** is a dated cross-category list, so it is set per concept. Its 29 motor vehicle and assault concepts are `false`, four of which also sit under `motor vehicle accident` in the other file. Its four ICD9 `763.x` perinatal codes and `Nutritional marasmus` stay `true`.
- **`old_spreadsheet_list` (2 rows).** Its source list is unrecoverable, so its concepts cannot be attributed to an element.

### Five concepts the retroactive file keeps suppressed

These are `false` above but `true` in `ct_retroactive_privacy_suppression.csv`, which this change leaves untouched. `CTRetroactivePrivacyConceptSuppression` runs at `controlled_plus_deid_base` and `controlled_plus_deid_clean`, after the two rules that read the files above run at `controlled_plus_deid`, so all five stay suppressed in CT+. Whether they should be released needs confirming with the program before release.

| concept_id | Released under | Retroactive file label | Concept |
|---|---|---|---|
| 4210749 | `assault/homicide` | `Assault/Homicide` | victim of sexual aggression |
| 44821603 | `assault/homicide` | `Assault/Homicide` | observation and evaluation for suspected abuse and neglect |
| 44821588 | `legal_intervention` | `Legal Intervention` | family disruption due to divorce or legal separation |
| 44822621 | `suicide.csv` | `Suicide` | suicide and self-inflicted injury by crashing of motor vehicle |
| 44826231 | `military_operations` | `Living situation` | family disruption due to return of family member from military deployment |

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
