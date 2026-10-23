# Results

The analysis highlights three things: demand responds to both temperature extremes, weekends change the daily demand profile more than its peak timing, and the 2025 forecast revision moved away from the eventual actual peak.

## Higher demand does not automatically mean underlying growth

![Electricity demand and temperature trends](<images/Electricity Demand and Temperature Trends Over Time.png>)

Summer and winter both bring elevated demand, so comparing adjacent months can confuse seasonal effects with growth. Summer 2026 also appears to have higher demand than summer 2025 without a higher monthly temperature peak. That makes growth beyond weather effects worth testing, but monthly averages can hide extreme days and differences in coverage. Comparing the same months under similar weather conditions would give a stronger answer; these charts alone cannot attribute growth to data centers.

## A single temperature trend would miss half the relationship

![Temperature and demand relationship from 2025 onward](<images/Temperature vs Demand Relationship (2025+).png>)

Demand is lowest around 10–15°C and rises toward both extremes. A simple straight-line relationship between temperature and demand could therefore look weak even though the pattern is pronounced: warming reduces demand on the cold side and increases it on the hot side.

For forecasting, this supports treating heating and cooling effects separately. The spread of demand at the same temperature also shows why hour of day and day type belong alongside weather in the analysis.

## Weekends soften the morning ramp, but retain the evening peak

![Hourly demand patterns on weekdays and weekends](<images/Hourly Demand Patterns_ Weekday vs Weekend.png>)

Around 07:00, weekday demand is roughly 1,500 MWh higher than weekend demand. By the shared peak near 18:00, the gap narrows to about 500 MWh. Overnight demand is almost identical.

The practical distinction is timing: weekends substantially reduce the morning ramp but offer much less relief at the evening peak. Using one average daily profile would hide that difference. The large overnight load also shows substantial demand persists outside daytime activity, although this chart cannot identify which consumers account for it.

## The 2025 miss was a downward revision during rising recorded peaks

![Forecast versus actual peak demand](<images/Forecast vs Actual Peak Demand.png>)

The PJM workbook records a July peak increase from **4,534 MW in 2024 to 5,563 MW in 2025**, a rise of **22.7%**. Meanwhile, the forecast for 2025 was revised down from **5,538 MW in the 2024 release to 4,970 MW in the 2025 release**. The resulting underestimate was **593 MW, or 10.7%**.

![Forecast absolute error by horizon and target year](<images/Forecast Accuracy by Horizon Years.png>)

Comparing forecasts for the same target year isolates the effect of that revision:

| Forecast release | Forecast for 2025 | Error versus actual |
| --- | ---: | ---: |
| 2023 | 5,574 MW | +0.2% |
| 2024 | 5,538 MW | −0.4% |
| 2025 | 4,970 MW | −10.7% |

The useful follow-up is to investigate what changed in the 2025 assumptions or source definitions. Earlier estimates were already close to the eventual peak. This is evidence that retaining forecast versions matters; it is not enough evidence to rank forecast horizons generally.

## Keep the two demand measures separate

The workbook's 2025 July peak of 5,563 MW is below even the typical hourly regional load shown in the demand charts. They cannot be interpreted as the same population and measure without reconciling their scope. The pipeline makes both datasets available, but matching region labels alone does not establish comparability.

*Forecast figures were calculated from the [PJM workbook](../data/raw/pjm_forecast_accuracy.xlsx), using its DOM sheet. Other numerical readings are approximate values from the charts; weather represents the Dulles station.*
