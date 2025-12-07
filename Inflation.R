# ==============================================================================
# 0. 环境准备
# ==============================================================================
if (!require("quantmod")) install.packages("quantmod")
if (!require("data.table")) install.packages("data.table")
if (!require("arrow")) install.packages("arrow")
if (!require("readr")) install.packages("readr")
if (!require("ggplot2")) install.packages("ggplot2")
if (!require("patchwork")) install.packages("patchwork")

library(quantmod)
library(data.table)
library(arrow)
library(readr)
library(ggplot2)
library(patchwork)

# 确保图片目录存在
dir.create("data/plots", recursive = TRUE, showWarnings = FALSE)
dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

START_DATE <- "1990-01-01"

# ==============================================================================
# 1. 特殊数据加载函数：处理你的 DBC CSV
# ==============================================================================
load_local_db_index <- function(filepath) {
  message(paste(">>> 读取本地文件:", filepath))
  
  # 修复警告的核心：添加 quote = "" 忽略不规范的引号
  dt_raw <- fread(filepath, fill = TRUE, quote = "")
  
  # 清洗：只保留日期和 Level
  dt_clean <- dt_raw[, .(Date, Level)]
  
  # 转换日期，过滤掉转换失败的行 (即底部的文字)
  dt_clean[, Date := as.Date(Date, format = "%Y-%m-%d")]
  dt_clean <- dt_clean[!is.na(Date)]
  
  # 确保按日期排序
  setorder(dt_clean, Date)
  
  # 计算收益率
  dt_clean[, Return := Level / shift(Level, 1) - 1]
  dt_clean[is.na(Return), Return := 0]
  
  # 转为 xts 格式以便后续 merge
  xts_out <- xts(dt_clean$Return, order.by = dt_clean$Date)
  return(xts_out)
}

# ==============================================================================
# 2. 核心拼接函数 (支持 Yahoo 混合)
# ==============================================================================
splice_inflation_assets <- function(ticker_name, etf_ticker, proxy_source, proxy_ticker) {
  
  message(paste("正在处理:", ticker_name, "..."))
  
  # --- A. 获取 ETF 数据 (Yahoo) ---
  tryCatch({
    dt_etf <- getSymbols(etf_ticker, src = "yahoo", from = START_DATE, auto.assign = FALSE)
    ret_etf <- dailyReturn(Ad(dt_etf))
  }, error = function(e) {
    # 如果 GLD 下载失败，可能是网络波动，给个提示
    stop(paste("ETF 下载失败:", etf_ticker, "-", e$message))
  })
  
  # --- B. 获取 Proxy 数据 (Local CSV or Yahoo) ---
  ret_proxy <- NULL
  
  if (proxy_source == "LOCAL_CSV") {
    # 读取你上传的 DBC Index CSV
    ret_proxy <- load_local_db_index(proxy_ticker)
    
  } else if (proxy_source == "yahoo") {
    # 替代 FRED：使用 Yahoo 的黄金期货 (GC=F)
    tryCatch({
      # 注意：GC=F 在 Yahoo 上有时会有空值，需要 na.omit 处理
      dt_p <- getSymbols(proxy_ticker, src = "yahoo", from = START_DATE, auto.assign = FALSE)
      dt_p <- na.omit(dt_p) 
      ret_proxy <- dailyReturn(Ad(dt_p))
    }, error = function(e) {
      warning(paste("Proxy 下载失败:", proxy_ticker, "。将仅使用 ETF 数据。"))
      ret_proxy <- xts(order.by = as.Date(character())) # 空 xts
    })
  }
  
  # --- C. 裁剪时间范围 ---
  if (length(ret_proxy) > 0) {
    ret_proxy <- ret_proxy[index(ret_proxy) >= as.Date(START_DATE)]
  }
  
  # --- D. 合并与拼接 ---
  # 如果 Proxy 为空，直接用 ETF
  if (length(ret_proxy) == 0) {
    merged_xts <- ret_etf
    colnames(merged_xts) <- "ETF"
    merged_xts$Proxy <- NA
  } else {
    merged_xts <- merge(ret_etf, ret_proxy)
    names(merged_xts) <- c("ETF", "Proxy")
  }
  
  # 优先用 ETF，ETF 没有则用 Proxy
  merged_xts$Spliced_Ret <- ifelse(is.na(merged_xts$ETF), merged_xts$Proxy, merged_xts$ETF)
  
  # 如果连 Proxy 都没有（比如 GC=F 在 1990 年没数据），填 0
  merged_xts$Spliced_Ret[is.na(merged_xts$Spliced_Ret)] <- 0
  
  # 重建价格 (Base 100)
  merged_xts$Synth_Price <- 100 * cumprod(1 + merged_xts$Spliced_Ret)
  
  # 转为 data.table
  dt_out <- data.table(date = index(merged_xts), coredata(merged_xts))
  dt_out <- dt_out[, .(date, 
                       ticker = etf_ticker,
                       asset_name = ticker_name,
                       price = Synth_Price, 
                       return = Spliced_Ret,
                       source_type = ifelse(is.na(ETF), "Proxy", "Real"))]
  
  return(dt_out)
}

# ==============================================================================
# 3. 执行任务
# ==============================================================================

# 任务 A: 商品 (DBC + Local DB Index)
# 假设文件名是 IndexLevelsAndVolatility.csv
dt_commodity <- splice_inflation_assets(
  ticker_name = "Commodities",
  etf_ticker = "DBC", 
  proxy_source = "LOCAL_CSV", 
  proxy_ticker = "IndexLevelsAndVolatility.csv" 
)

# 任务 B: 黄金 (GLD + Yahoo GC=F) -> 修复点
dt_gold <- splice_inflation_assets(
  ticker_name = "Gold",
  etf_ticker = "GLD", 
  proxy_source = "yahoo", # 改用 Yahoo
  proxy_ticker = "GC=F"   # 黄金期货连续合约
)

# 任务 C: 抗通胀债 (TIP + VIPSX)
dt_tips <- splice_inflation_assets(
  ticker_name = "TIPS",
  etf_ticker = "TIP", 
  proxy_source = "yahoo", 
  proxy_ticker = "VIPSX" 
)

# 合并所有
dt_inflation_all <- rbind(dt_commodity, dt_gold, dt_tips)

# ==============================================================================
# 4. 可视化检查
# ==============================================================================
plot_splice_inf <- function(dt_subset, title_str) {
  # 找到 Real ETF 开始的日期
  switch_date <- dt_subset[source_type == "Real", min(date)]
  if(is.infinite(switch_date)) switch_date <- min(dt_subset$date) # 防止全是 Proxy
  
  ggplot(dt_subset, aes(x = date, y = price)) +
    geom_line(aes(color = source_type), size = 0.8) +
    geom_vline(xintercept = switch_date, linetype = "dashed", alpha = 0.5) +
    annotate("text", x = switch_date, y = max(dt_subset$price)*0.9, 
             label = paste("Start:", format(switch_date, "%Y")), 
             hjust = -0.1, size = 3, color = "gray40") +
    scale_y_log10() +
    scale_color_manual(values = c("Proxy" = "#D35400", "Real" = "#F1C40F")) +
    labs(title = title_str, y = "Log Price", x = "") +
    theme_minimal() + theme(legend.position = "none")
}

p_dbc <- plot_splice_inf(dt_commodity, "Commodities (DBC + Local Index)")
p_gld <- plot_splice_inf(dt_gold, "Gold (GLD + Futures GC=F)")
p_tip <- plot_splice_inf(dt_tips, "TIPS (TIP + VIPSX)")

final_plot <- (p_dbc / p_gld / p_tip)
print(final_plot)
ggsave("data/plots/inflation_splicing_check.png", final_plot, width = 10, height = 12)

# ==============================================================================
# 5. 保存数据
# ==============================================================================
write_parquet(dt_inflation_all[, .(date, ticker, price)], "data/processed/inflation_prices.parquet")
write_parquet(dt_inflation_all[, .(date, ticker, return)], "data/processed/inflation_returns.parquet")

message("✅ Inflation 模块修复完成！")