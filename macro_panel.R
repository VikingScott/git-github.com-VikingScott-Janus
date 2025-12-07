# ==============================================================================
# 05_final_stitch.R
# ==============================================================================
library(tidyverse)
library(lubridate)
library(fs)

process_dir <- "data/process"

# 1. 读取三个清洗好的数据源
# ------------------------------------------------------------------------------
df_gb   <- read_csv(path(process_dir, "greenbook_daily_cleaned.csv"), show_col_types = FALSE)
df_fred <- read_csv(path(process_dir, "fred_daily_cleaned.csv"), show_col_types = FALSE)
df_clev <- read_csv(path(process_dir, "cleveland_cpi_daily_cleaned.csv"), show_col_types = FALSE)

message("数据源读取完成，准备拼接...")

# 2. 定义拼接参数
# ------------------------------------------------------------------------------
# 经过测试，2017-01-01 是一个比较平稳的过渡点
SPLICE_DATE <- ymd("2017-01-01")

# 3. 大合并 (Full Join)
# ------------------------------------------------------------------------------
df_merged <- df_gb %>%
  full_join(df_fred, by = "Date") %>%
  full_join(df_clev, by = "Date") %>%
  arrange(Date) %>%
  # 过滤掉 1990 年以前的数据
  filter(Date >= "1990-01-01")

# 4. 执行拼接逻辑 (The Stitch)
# ------------------------------------------------------------------------------
df_final <- df_merged %>%
  mutate(
    # --- 核心变量 1: 增长 (Growth) ---
    # 逻辑: 2017前信Greenbook(F0)，2017后信GDPNow
    # 两者都是季调后环比折年率 (SAAR)，单位一致
    Macro_Growth = if_else(Date < SPLICE_DATE, GB_GDP_Now, Fred_GDP_Now),
    
    # --- 核心变量 2: 核心通胀 (Core Inflation) ---
    # 逻辑: 2017前信Greenbook(F0)，2017后信Cleveland(季度环比折年率)
    # 这里的 Gap 只有 0.03%，拼接非常完美
    Macro_Inflation_Core = if_else(Date < SPLICE_DATE, GB_CPI_Core, Cleveland_CPI_Core_Q),
    
    # --- 核心变量 3: 整体通胀 (Headline Inflation) ---
    # 逻辑: 同上
    Macro_Inflation_Head = if_else(Date < SPLICE_DATE, GB_CPI_Head, Cleveland_CPI_Head_Q),
    
    # --- 风控/市场信号 (直接来自 FRED) ---
    Signal_YieldCurve = Yield_10Y2Y,     # 10Y-2Y 利差
    Signal_Risk       = Credit_Spread,   # 信用利差
    Signal_VIX        = VIX              # 恐慌指数
  ) %>%
  
  # 5. 最终清洗
  # ------------------------------------------------------------------------------
# 只保留我们策略需要的最终列
select(Date, 
       Macro_Growth, 
       Macro_Inflation_Core, 
       Macro_Inflation_Head, 
       Signal_YieldCurve, 
       Signal_Risk, 
       Signal_VIX) %>%
  
  # 再次向前填充 (防止拼接处可能有微小的空隙)
  fill(everything(), .direction = "down") %>%
  
  # 去除头部缺失值 (等待所有指标都有数的那一天开始)
  drop_na()

# 6. 保存最终成品
# ------------------------------------------------------------------------------
output_file <- path("data", "Macro_Daily_Final.csv")
write_csv(df_final, output_file)

message("------------------------------------------------")
message("🎉 恭喜！全流程处理完毕！")
message(paste("最终文件已保存至:", output_file))
message(paste("数据范围:", min(df_final$Date), "->", max(df_final$Date)))
message("前5行预览:")
print(head(df_final))