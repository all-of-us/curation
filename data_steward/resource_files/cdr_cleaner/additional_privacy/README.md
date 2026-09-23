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
