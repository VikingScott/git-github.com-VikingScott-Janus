# ==============================================================================
# R Script: 资产时间范围与完整性检查
# ==============================================================================
library(arrow)
library(data.table)

# 1. 读取最终生成的宽表
# 确保你已经运行了上一步的合并代码
dt_prices <- read_parquet("data/final/asset_prices_patched.parquet")
setDT(dt_prices) # 转为 data.table

message(paste(">>> 正在检查资产池，共包含", ncol(dt_prices) - 1, "个资产..."))

# 2. 遍历检查每个 Ticker
stats_list <- list()
tickers <- setdiff(names(dt_prices), "date") # 排除日期列

for (tk in tickers) {
  # 提取该资产非空 (NA) 的日期
  # 宽表中可能会有 NA (比如 TIP 在 2000 年前)
  valid_dates <- dt_prices[!is.na(get(tk)), date]
  
  if (length(valid_dates) > 0) {
    stats_list[[tk]] <- data.table(
      Ticker = tk,
      Start_Date = min(valid_dates),
      End_Date = max(valid_dates),
      Days = length(valid_dates),
      Years = round(length(valid_dates) / 252, 1)
    )
  } else {
    warning(paste("警告:", tk, "全是空值 (NA)!"))
  }
}

# 3. 合并并格式化输出
check_report <- rbindlist(stats_list)
setorder(check_report, Start_Date) # 按开始时间排序

print("===== 资产时间范围体检报告 =====")
print(check_report)

# 4. 自动高亮短板
late_starters <- check_report[Start_Date > as.Date("1991-01-01")]
if (nrow(late_starters) > 0) {
  message("\n⚠️ 注意：以下资产开始时间较晚，可能会影响 90 年代的回测：")
  print(late_starters[, .(Ticker, Start_Date, Years)])
} else {
  message("\n✅ 完美！所有资产都覆盖了 90 年代。")
}