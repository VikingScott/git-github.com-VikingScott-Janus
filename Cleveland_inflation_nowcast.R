# ==============================================================================
# 04_process_cleveland_cpi.R
# ==============================================================================
library(tidyverse)
library(lubridate)
library(fs)

# 定义路径
# 注意：请确保你上传的 InflationCharting_QuarterlyAnnualized.csv 已经在 data/raw 下
raw_file_path <- "data/raw/InflationCharting_QuarterlyAnnualized.csv"
process_dir   <- "data/process"

# ==============================================================================
# 1. 读取与清洗
# ==============================================================================
message("正在处理 Cleveland Inflation 数据...")

# 读取 CSV
df_clev_raw <- read_csv(raw_file_path, show_col_types = FALSE)

# 清洗逻辑
df_clev_clean <- df_clev_raw %>%
  # 1. 转换日期 (CSV里通常是 YYYY-MM-DD)
  mutate(Date = ymd(date)) %>%
  
  # 2. 筛选时间 (我们只需要拼接 Greenbook 之后的部分，但为了检查先保留 1990+)
  filter(year(Date) >= 1990) %>%
  
  # 3. 提取核心列 & 重命名
  # cpi -> 对应 Greenbook 的 Head CPI
  # corecpi -> 对应 Greenbook 的 Core CPI
  select(Date, 
         Cleveland_CPI_Head_Q = cpi, 
         Cleveland_CPI_Core_Q = corecpi) %>%
  
  # 4. 排序
  arrange(Date)

# ==============================================================================
# 2. 日频化 (Forward Fill)
# ==============================================================================
# 因为这是季度数据，我们需要把它铺开到每一天，作为“当季已知/预测值”的占位符

# 生成标准日历
date_grid <- tibble(
  Date = seq(from = min(df_clev_clean$Date), to = Sys.Date(), by = "day")
)

df_clev_daily <- date_grid %>%
  left_join(df_clev_clean, by = "Date") %>%
  # 向前填充：在下一个季度数据公布前，我们认为当前的通胀水平维持不变
  fill(Cleveland_CPI_Head_Q, Cleveland_CPI_Core_Q, .direction = "down")

# ==============================================================================
# 3. 保存
# ==============================================================================
output_path <- path(process_dir, "cleveland_cpi_daily_cleaned.csv")
write_csv(df_clev_daily, output_path)

message("------------------------------------------------")
message("Cleveland CPI 数据处理完成！")
message(paste("输出文件:", output_path))
message("包含指标: Headline CPI (SAAR), Core CPI (SAAR)")
print(tail(df_clev_daily))