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