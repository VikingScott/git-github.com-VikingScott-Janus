# ==============================================================================
# 0. 环境设置与依赖加载
# ==============================================================================
if (!require("quantmod")) install.packages("quantmod")
if (!require("data.table")) install.packages("data.table")
if (!require("arrow")) install.packages("arrow")
if (!require("readr")) install.packages("readr") # 用于写 CSV
if (!require("ggplot2")) install.packages("ggplot2")
if (!require("patchwork")) install.packages("patchwork") # 用于拼图

library(quantmod)
library(data.table)
library(arrow)
library(readr)
library(ggplot2)
library(patchwork)

# 确保输出目录存在
dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)
dir.create("data/plots", recursive = TRUE, showWarnings = FALSE)

START_DATE <- "1990-01-01"

# ==============================================================================
# 1. 核心拼接函数 (通用版)
# ==============================================================================
splice_assets <- function(etf_ticker, proxy_ticker, start_date) {
  
  message(paste(">>> 正在处理:", etf_ticker, "+", proxy_ticker, "..."))
  
  # 1. 下载数据
  tryCatch({
    dt_etf <- getSymbols(etf_ticker, src = "yahoo", from = start_date, auto.assign = FALSE)
    dt_proxy <- getSymbols(proxy_ticker, src = "yahoo", from = start_date, auto.assign = FALSE)
  }, error = function(e) {
    stop(paste("下载失败 (", etf_ticker, "/", proxy_ticker, "): ", e$message))
  })
  
  # 2. 计算收益率
  ret_etf <- dailyReturn(Ad(dt_etf))
  ret_proxy <- dailyReturn(Ad(dt_proxy))
  
  # 3. 合并
  merged_xts <- merge(ret_etf, ret_proxy)
  names(merged_xts) <- c("ETF", "Proxy")
  
  # 4. 拼接逻辑
  merged_xts$Spliced_Ret <- ifelse(is.na(merged_xts$ETF), merged_xts$Proxy, merged_xts$ETF)
  merged_xts$Spliced_Ret[is.na(merged_xts$Spliced_Ret)] <- 0
  
  # 5. 重建价格 (Base 100)
  merged_xts$Synth_Price <- 100 * cumprod(1 + merged_xts$Spliced_Ret)
  
  # 6. 输出 data.table
  dt_out <- data.table(date = index(merged_xts), coredata(merged_xts))
  dt_out <- dt_out[, .(date, 
                       ticker = etf_ticker, 
                       proxy_used = proxy_ticker,
                       price = Synth_Price, 
                       return = Spliced_Ret,
                       source_type = ifelse(is.na(ETF), "Proxy", "Real"))]
  
  return(dt_out)
}

# ==============================================================================
# 2. 执行 Deflation 模块拼接
# ==============================================================================

# A. 长债 (TLT + VUSTX) - 核心避险
dt_tlt <- splice_assets("TLT", "VUSTX", START_DATE)

# B. 中债 (IEF + VFITX) - 压舱石
dt_ief <- splice_assets("IEF", "VFITX", START_DATE)

# C. 信用债 (LQD + VWESX) - 你要求的 Investment Grade
# 注意：这里我们使用了 VWESX (Inv Grade) 而不是 VWEHX (High Yield)
dt_lqd <- splice_assets("LQD", "VWESX", START_DATE)

# 合并所有
dt_deflation_all <- rbind(dt_tlt, dt_ief, dt_lqd)

# ==============================================================================
# 3. 生成可视化检查图 (重点看接缝处)
# ==============================================================================
plot_splice <- function(dt_subset, title_str) {
  # 找到切换点
  switch_date <- dt_subset[source_type == "Real", min(date)]
  
  ggplot(dt_subset, aes(x = date, y = price)) +
    geom_line(aes(color = source_type), size = 0.8) +
    geom_vline(xintercept = switch_date, linetype = "dashed", alpha = 0.5) +
    annotate("text", x = switch_date, y = max(dt_subset$price)*0.8, 
             label = paste("ETF Inception:", format(switch_date, "%Y-%m")), 
             hjust = -0.1, size = 3, color = "gray40") +
    scale_y_log10() +
    scale_color_manual(values = c("Proxy" = "#E74C3C", "Real" = "#2980B9")) +
    labs(title = title_str, subtitle = paste("Proxy Used:", unique(dt_subset$proxy_used)),
         y = "Log Price (Base 100)", x = "") +
    theme_minimal() +
    theme(legend.position = "none")
}

p1 <- plot_splice(dt_tlt, "Long-Term Treasury (TLT)")
p2 <- plot_splice(dt_ief, "Interm-Term Treasury (IEF)")
p3 <- plot_splice(dt_lqd, "Inv Grade Corporate (LQD)")

# 拼图展示
combined_plot <- (p1 / p2 / p3)
print(combined_plot)
ggsave("data/plots/deflation_splicing_check.png", combined_plot, width = 10, height = 12)

# ==============================================================================
# 4. 拆分并保存文件 (Parquet + CSV)
# ==============================================================================

# ---- Save Price Data ----
dt_price_out <- dt_deflation_all[, .(date, ticker, price)]
write_parquet(dt_price_out, "data/processed/deflation_prices.parquet")
write_csv(dt_price_out, "data/processed/deflation_prices.csv")

# ---- Save Return Data ----
dt_return_out <- dt_deflation_all[, .(date, ticker, return)]
write_parquet(dt_return_out, "data/processed/deflation_returns.parquet")
write_csv(dt_return_out, "data/processed/deflation_returns.csv")

message("✅ Deflation 模块处理完成！")
message("   文件已保存至 data/processed/")
message("   检查图表已保存至 data/plots/")