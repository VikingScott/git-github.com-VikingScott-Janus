# ==============================================================================
# R Script: 数据整合 (Consolidation) - Long to Wide
# ==============================================================================
if (!require("data.table")) install.packages("data.table")
if (!require("arrow")) install.packages("arrow")
if (!require("readr")) install.packages("readr")

library(data.table)
library(arrow)
library(readr)

# 确保输出目录存在
dir.create("data/final", recursive = TRUE, showWarnings = FALSE)

# ==============================================================================
# 函数: 读取、合并并透视为宽表
# ==============================================================================
process_and_pivot <- function(file_pattern, value_col) {
  
  message(paste0(">>> 正在扫描并处理: *_", file_pattern, " ..."))
  
  # 1. 扫描文件
  files <- list.files("data/processed", pattern = file_pattern, full.names = TRUE)
  
  if (length(files) == 0) {
    stop(paste("未找到任何", file_pattern, "文件！"))
  }
  
  # 2. 批量读取并合并 (rbind)
  # 使用 lapply 读取所有 parquet，然后 rbindlist 拼起来
  dt_list <- lapply(files, function(f) {
    dt <- read_parquet(f)
    return(as.data.table(dt))
  })
  
  dt_all <- rbindlist(dt_list, use.names = TRUE, fill = TRUE)
  
  # 3. 检查重复 (Defensive Programming)
  # 防止同一天同一个 Ticker 有两条数据
  if (nrow(dt_all[duplicated(dt_all[, .(date, ticker)])]) > 0) {
    warning("检测到重复数据！正在取平均值去重...")
    dt_all <- dt_all[, .(value = mean(get(value_col), na.rm = TRUE)), by = .(date, ticker)]
    setnames(dt_all, "value", value_col)
  }
  
  message(paste("    合并后数据量:", nrow(dt_all), "| 包含资产:", paste(unique(dt_all$ticker), collapse = ", ")))
  
  # 4. 透视 (Pivot to Wide)
  # 行: date, 列: ticker, 值: value_col
  formula_str <- paste("date ~ ticker")
  dt_wide <- dcast(dt_all, as.formula(formula_str), value.var = value_col)
  
  # 5. 按日期排序
  setorder(dt_wide, date)
  
  # 6. 处理 NA (可选)
  # 宽表中可能出现 NA (例如 GLD 在 2004 年前是 NA)，这是正常的，保留 NA
  # 后续计算相关性时会自动处理
  
  return(dt_wide)
}

# ==============================================================================
# 执行任务
# ==============================================================================

# 任务 A: 处理价格 (Prices) -> asset_prices.csv
dt_prices_wide <- process_and_pivot("prices.parquet", "price")
write_csv(dt_prices_wide, "data/final/asset_prices.csv")
# 同时也存一份 parquet 供后续读取
write_parquet(dt_prices_wide, "data/final/asset_prices.parquet")

message("✅ 价格宽表已生成: data/final/asset_prices.csv")
print(head(dt_prices_wide))

# 任务 B: 处理收益率 (Returns) -> asset_returns.csv
dt_returns_wide <- process_and_pivot("returns.parquet", "return")
write_csv(dt_returns_wide, "data/final/asset_returns.csv")
write_parquet(dt_returns_wide, "data/final/asset_returns.parquet")

message("✅ 收益率宽表已生成: data/final/asset_returns.csv")
print(head(dt_returns_wide))