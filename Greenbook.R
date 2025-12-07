# ==============================================================================
# 0. 环境设置
# ==============================================================================
library(readxl)
library(tidyverse)
library(lubridate)
library(fs)

# 定义文件路径
raw_file_path <- "data/raw/PhilaGreenbookDataset.xlsx"
process_dir   <- "data/process"

# 检查原始文件是否存在
if (!file_exists(raw_file_path)) {
  stop(paste("错误: 找不到文件", raw_file_path, "请确认文件已放入 data/raw 文件夹"))
}

# 创建输出目录
if (!dir_exists(process_dir)) dir_create(process_dir)

# ==============================================================================
# 1. 定义通用清洗函数
# ==============================================================================
# 这个函数负责读取指定的 Sheet，提取 F0 列，并标准化日期
process_gb_sheet <- function(file_path, sheet_name, f0_col, rename_to) {
  
  message(paste("正在处理 Sheet:", sheet_name, "..."))
  
  # 读取 Excel 指定 Sheet
  df_raw <- read_excel(file_path, sheet = sheet_name)
  
  # 清洗逻辑
  df_clean <- df_raw %>%
    # 1. 转换日期: GBdate 是发布日期 (格式如 19900130)
    mutate(Date = ymd(GBdate)) %>%
    
    # 2. 筛选时间: 锁定 1990 年至今
    filter(year(Date) >= 1990) %>%
    
    # 3. 提取: 只取发布日(Date) 和 当季预测(F0)
    # 使用 all_of() 确保列名必须存在
    select(Date, Value = all_of(f0_col)) %>%
    
    # 4. 重命名: 改为更有意义的变量名
    rename(!!rename_to := Value) %>%
    
    # 5. 排序
    arrange(Date)
  
  return(df_clean)
}

# ==============================================================================
# 2. 执行读取与清洗
# ==============================================================================

# 2.1 实际 GDP (Real GDP)
df_gdp <- process_gb_sheet(
  file_path = raw_file_path,
  sheet_name = "gRGDP",      # Excel Sheet 名
  f0_col = "gRGDPF0",        # 原始列名 (当季预测)
  rename_to = "GB_GDP_Now"   # 新变量名
)

# 2.2 整体 CPI (Headline CPI)
df_cpi_head <- process_gb_sheet(
  file_path = raw_file_path,
  sheet_name = "gPCPI",
  f0_col = "gPCPIF0",
  rename_to = "GB_CPI_Head"
)

# 2.3 核心 CPI (Core CPI)
df_cpi_core <- process_gb_sheet(
  file_path = raw_file_path,
  sheet_name = "gPCPIX",
  f0_col = "gPCPIXF0",
  rename_to = "GB_CPI_Core"
)

# ==============================================================================
# 3. 合并数据 (Merging)
# ==============================================================================
message("正在合并数据...")

# 使用 reduce 一次性合并三个表 (按 Date 全连接)
list_of_dfs <- list(df_gdp, df_cpi_head, df_cpi_core)

df_merged <- list_of_dfs %>%
  reduce(full_join, by = "Date") %>%
  arrange(Date)

# ==============================================================================
# 4. 日频扩展 (Daily Expansion)
# ==============================================================================
message("正在扩展为日频数据...")

# 4.1 创建日频骨架 (从 1990-01-01 到 今天)
# 这样可以方便后面直接 left_join 现代的 FRED 数据
date_grid <- tibble(
  Date = seq(from = ymd("1990-01-01"), to = Sys.Date(), by = "day")
)

# 4.2 拼接并向前填充 (Forward Fill)
df_daily_greenbook <- date_grid %>%
  left_join(df_merged, by = "Date") %>%
  # 核心步骤: 将 NA 填充为最近一次会议的数值
  fill(GB_GDP_Now, GB_CPI_Head, GB_CPI_Core, .direction = "down")

# ==============================================================================
# 5. 保存结果
# ==============================================================================
output_file <- path(process_dir, "greenbook_daily_cleaned.csv")

write_csv(df_daily_greenbook, output_file)

# 输出摘要
message("------------------------------------------------")
message("处理完成！")
message(paste("输出文件:", output_file))
message(paste("时间跨度:", min(df_daily_greenbook$Date), "->", max(df_daily_greenbook$Date)))
message(paste("总行数:", nrow(df_daily_greenbook)))
message("前5行预览:")
print(head(df_daily_greenbook, 5))