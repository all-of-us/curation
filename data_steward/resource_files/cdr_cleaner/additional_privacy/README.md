## The `ct_plus_suppressed` column

A boolean `ct_plus_suppressed` column marks, per row, whether a concept stays suppressed in Controlled Tier Plus. It was added by DL-2419 with every row `true`, and DL-2421 flipped the CT+ expanded categories to `false`.

### Files carrying the column

| File | Rows | `false` |
|------|------|---------|
| `ct_additional_privacy_suppressions.csv` | 1,829 | 321 |
| `ct_rt_publicly_reportable_suppressions.csv` | 10,921 | 10,323 |
| `ct_observation_postcoordinated_suppressions.csv` | 5 | 0 |
| `ct_retroactive_privacy_suppression.csv` | 40 | 0 |

The RT files (`rt_additional_privacy_suppressions.csv`, `rt_observation_postcoordinated_suppressions.csv`, `rt_retroactive_privacy_suppression.csv`) and `ct_nph_observation_concept_suppressions.csv` do not carry the column.

CT and RT behaviour is unchanged. Their rules load every row regardless of this column; only the CT+ rule variants read it.

### The expanded categories

`privacy_rule` uses two label conventions, because the two files were populated from separately supplied lists. `ct_additional_privacy_suppressions.csv` records a category name (`assault/homicide`); `ct_rt_publicly_reportable_suppressions.csv` records the source list's filename (`assault.csv`). Both forms of an element must be listed together, or half of it stays suppressed.

| Data element | Category name | Source filename |
|---|---|---|
| Assault and homicide | `assault/homicide` | `assault.csv` |
| Motor vehicle accident | `motor vehicle accident` | `vehicle.csv` |
| Military operations and operations of war | `military_operations`, `operations_of_war` | `military.csv` |
| Legal intervention | `legal_intervention` | `legal.csv` |
| Drowning | | `drowning.csv` |
| Suffocation | `suffocation` | `suffocation.csv` |
| Suicide | `suicide` | `suicide.csv` |
| Terrorism | `terrorism` | `terrorism.csv` |
| Multiple gestation | `multiples` | `space.csv` |

`operations_of_war` is kept as a distinct label even though it shares a disposition with `military_operations`. The disposition is shared, the label is not. Keeping it separate means reversing that decision is two rows of a seed file rather than a re-derivation of which concepts were which.

### Suppressed wins on multi-category rows

A row whose `privacy_rule` holds several `;`-separated categories is `false` only when every one of them is expanded. `liveborn; multiples` stays `true`, because `liveborn` is not expanded. All nine multi-category combinations currently in the data carry at least one non-expanded category, so all of them stay `true`.

### Everything else stays suppressed

`abortion` is suppressed at every tier. `location` ships as the Zip5 linked dataset, and `liveborn`, `liveborn_infants`, `birth.csv`, `delivery-proc.csv`, `perinatal`, `perinatal.csv` and `stillborn` ship with the date of birth linked dataset, so none of them is released through this column. Free text combinations stay suppressed for MVP.

### The three labels that are not categories

Three `privacy_rule` values in `ct_rt_publicly_reportable_suppressions.csv` are not categories in the sense the rest are, and each has its own disposition.

`06apr20 NIH List` (34 rows) is a dated cross-category list, so it is dispositioned per concept rather than per label. 29 rows are `false`: 14 motor vehicle concepts and 15 assault concepts, each belonging to a category that is expanded elsewhere in these files. 5 rows stay `true`: four ICD9 `763.x` codes for delivery complications affecting the fetus or newborn, which are perinatal and ship with the date of birth linked dataset, and `Nutritional marasmus`, which no expanded category covers.

Four of the 29 also appear under `motor vehicle accident` in `ct_additional_privacy_suppressions.csv`. Leaving them suppressed here would have released and suppressed the same concept at once.

`unknown COD.csv` (165 rows) is `false` in full. 152 of its rows are ICD10 `S06.x` codes for traumatic brain injury with loss of consciousness and death before regaining consciousness; the other 13 are death of unknown cause, sudden death and ill defined cause of mortality codes. The registered tier privacy rules file the whole list under codes subject to public knowledge, and the CT+ proposal marks that element as included in CT+.

The two halves differ by two orders of magnitude in how much data they touch. Measured against the pre-suppression combined layer, the 152 traumatic brain injury codes appear on 105 rows across roughly 35 participants, while the 13 remaining codes appear on 7,888 rows across roughly 4,245, almost all of it `Death of unknown cause` and its ICD9 equivalent `799.9`.

`old_spreadsheet_list` (2 rows) stays `true`. Its source list is not recoverable, so the two concepts (`General symptoms`, `Symptoms involving skin and other integumentary tissue`) cannot be attributed to a data element.

### Note on the retroactive file

Five concept IDs are `false` in the two files above while their rows in `ct_retroactive_privacy_suppression.csv` stay `true`. The two files label them differently, so both labels are given.

| concept_id | Released under | Retroactive file label | Concept |
|---|---|---|---|
| 4210749 | `assault/homicide` | `Assault/Homicide` | victim of sexual aggression |
| 44821603 | `assault/homicide` | `Assault/Homicide` | observation and evaluation for suspected abuse and neglect |
| 44821588 | `legal_intervention` | `Legal Intervention` | family disruption due to divorce or legal separation |
| 44822621 | `suicide.csv` | `Suicide` | suicide and self-inflicted injury by crashing of motor vehicle |
| 44826231 | `military_operations` | `Living situation` | family disruption due to return of family member from military deployment |

These five stay suppressed in CT+ regardless of the flip, because the retroactive rule runs at a later stage than the rules that read these two files. `CTAdditionalPrivacyConceptSuppression` and `CTObservationPrivacySuppression` run at `controlled_plus_deid`, while `CTRetroactivePrivacyConceptSuppression` runs at `controlled_plus_deid_base`, so whatever the earlier stage releases the later one removes. That holds only while every row of `ct_retroactive_privacy_suppression.csv` is `true`, which is what keeps this file untouched.

This needs confirming with the program before release.
