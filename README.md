# Project Scope: Janus – Macro Regime Intelligence Pipeline

## 1. Project Overview
**Project Janus** is a specialized data engineering and analytics initiative designed to construct a high-fidelity, point-in-time macroeconomic dataset. The primary objective is to synthesize a continuous, structural panel of "Nowcasts" (GDP, CPI, and auxiliary macro indicators) by stitching deep historical institutional records with real-time alternative data.

This dataset serves as the foundational inputs for **Tactical Asset Allocation (TAA)** and **Strategic Asset Allocation (SAA)** modules within the firm's **All Weather Strategy**, enabling dynamic regime identification (Growth/Inflation quadrants) based on *ex-ante* information rather than revised *ex-post* data.

## 2. Scope of Work

### 2.1 Data Fusion & Stitching (The "Time-Machine" Layer)
The core responsibility is to merge offline historical datasets with online real-time streams to create a seamless timeline from 1990/2000 to the present.

* **Historical Foundation (1967 – ~2018):**
    * Ingest and standardize the **Philadelphia Fed Greenbook Data Set**.
    * Extract critical decision variables: `F0` (Current Quarter Forecast) and `B1` (Previous Quarter Backcast) to reconstruct the "fog of war" historically.
* **Modern Extension (2018 – Present):**
    * Integrate the **Atlanta Fed GDPNow** (for GDP) and **Cleveland Fed Inflation Nowcasting** (for CPI/PCE).
    * Implement **Survey of Professional Forecasters (SPF)** data as a secondary consensus layer for calibration.
* **Stitching Logic:**
    * Develop normalization algorithms to align varying frequencies (FOMC meeting dates vs. daily/monthly releases).
    * Handle methodology discontinuities between Greenbook projections and modern algorithmic nowcasts.

### 2.2 Indicator Coverage (The "Macro Funnel")
The project will maintain a structured hierarchy of indicators to feed the investment funnel:

* **Tier 1: Core Regime Drivers (Growth & Inflation)**
    * Real GDP Growth (Nowcast `F0` & Momentum `F1-F0`)
    * Headline & Core CPI / PCE Inflation
    * GDP Deflator
* **Tier 2: Leading & Confirmation Indicators**
    * **Housing:** Housing Starts (Leading indicator for cyclical turning points).
    * **Production:** Industrial Production (High-frequency cyclical proxy).
    * **Labor:** Unemployment Rate (Lagging confirmation / Sahm Rule triggers).
* **Tier 3: Policy Impulse**
    * Fiscal Spending (Federal & State level projections).

### 2.3 Metadata Architecture
To ensure the dataset is "Quant-Ready," strict metadata governance will be applied:

* **Vintage Timestamping:** Every data point must be tagged with its `Reference_Date` (the quarter being measured) and `Observation_Date` (when the information became available).
* **Data Type Classification:** Explicit flags for `Forecast`, `Backcast`, `Initial_Release`, and `Revised`.
* **Source Attribution:** Lineage tracking (e.g., `Source: Greenbook_Mar2000` vs. `Source: GDPNow_05Dec2024`).

### 2.4 Automation & Maintenance
* **ETL Pipeline:** Build automated scripts (R/Python) to fetch latest online data (via APIs or scraping) and append to the historical master file.
* **Quality Assurance:** Automated checks for outliers, missing vintages, or "jumps" at the stitching seams.
* **Update Frequency:** The system ensures the dataset is updated in alignment with FOMC schedules and major data releases (BEA/BLS).

## 3. Deliverables
1.  **Master Panel Dataset:** A unified `.rds` (R Data) or Parquet file containing the clean, stitched time series.
2.  **Visualization Dashboard:** A diagnostic tool to view the "Gap" between Forecasts (`F0`) and Reality (`B1`) over time.
3.  **Signal Feed:** A direct API or flat-file export formatted for the **All Weather Strategy** execution engine (e.g., providing current *Growth Score* and *Inflation Score*).

## 4. Success Criteria
* **Elimination of Look-Ahead Bias:** Backtests using this dataset must reflect the information available to investors *at that specific historical moment*.
* **Continuity:** No statistical breaks in the time series between the Greenbook era and the GDPNow era.
* **Timeliness:** New regime signals are generated within 24 hours of major data releases.

***

### 下一步建议
你可以直接把这个MD文件发给你的团队或保存到你的项目ReadMe中。接下来在R里的工作，就是去实现这个文档里 **2.1 Data Fusion** 部分提到的逻辑。