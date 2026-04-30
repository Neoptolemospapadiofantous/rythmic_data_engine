#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    risk_manager.hpp — Legends 50K risk enforcement in C++

    Rules enforced:
      1. Trailing drawdown cap: equity must not fall more than $2500 below
         the peak equity reached since account inception (rolling).
         Note: this is from the PEAK, not from starting capital.
      2. Consistency cap: no single day can account for more than 30% of
         total cumulative profit.
      3. Max daily trades (handled by OrbStrategy — risk_manager just enforces
         the hard stop on dollar loss).
      4. Max daily loss (implicit via trailing drawdown — once equity drops
         $2500 from peak, halt for the day).

    All state is updated via on_fill_pnl() after each trade closes.
    check_limits() is called before each new signal is accepted.
    ═══════════════════════════════════════════════════════════════════════════ */
#include "orb_config.hpp"
#include "log.hpp"
#include <atomic>
#include <cmath>
#include <mutex>
#include <string>

class RiskManager {
public:
    explicit RiskManager(const OrbConfig& cfg, double starting_equity = 50000.0)
        : cfg_(cfg)
        , equity_(starting_equity)
        , peak_equity_(starting_equity)
        , total_profit_(0.0)
        , daily_pnl_(0.0)
        , halted_(false)
    {}

    // ── Seed historical P&L from DB on startup (call once after DB connect) ──
    void seed_total_profit(double historical_pnl) {
        std::lock_guard<std::mutex> lk(mu_);
        total_profit_ = historical_pnl;
        LOG("[RISK] Seeded total_profit=%.2f from DB", total_profit_);
    }

    // ── Called at start of each trading day ───────────────────────────────────
    void reset_daily() {
        std::lock_guard<std::mutex> lk(mu_);
        daily_pnl_ = 0.0;
        halted_    = false;
        halt_reason_.clear();
        LOG("[RISK] Daily reset — equity=%.2f peak=%.2f total_profit=%.2f",
            equity_, peak_equity_, total_profit_);
    }

    // ── Called after each trade closes ────────────────────────────────────────
    // pnl_usd: positive = profit, negative = loss (after commissions)
    void on_trade_pnl(double pnl_usd) {
        std::lock_guard<std::mutex> lk(mu_);
        if (!std::isfinite(pnl_usd)) {
            halt("non_finite_pnl: " + std::to_string(pnl_usd));
            return;
        }

        equity_       += pnl_usd;
        daily_pnl_    += pnl_usd;
        total_profit_ += pnl_usd;

        // Update rolling equity peak
        if (equity_ > peak_equity_) peak_equity_ = equity_;

        // Check daily loss limit (Legends: -$1000/day)
        if (cfg_.daily_loss_limit < 0.0 && daily_pnl_ <= cfg_.daily_loss_limit) {
            halt("daily_loss_limit: daily_pnl $" +
                 std::to_string(daily_pnl_) + " <= limit $" +
                 std::to_string(cfg_.daily_loss_limit));
            return;
        }

        // Check trailing drawdown from peak
        double drawdown = peak_equity_ - equity_;
        if (drawdown >= cfg_.trailing_drawdown_cap) {
            halt("trailing_drawdown_cap: drawdown $" +
                 std::to_string(drawdown) + " >= cap $" +
                 std::to_string(cfg_.trailing_drawdown_cap));
            return;
        }

        // Check consistency cap: today's profit must not exceed 30% of total profit
        // Only enforced when total_profit > 0 (don't restrict during losses)
        // Consistency cap: today's profit must not exceed 30% of prior days' cumulative profit.
        // Compare against prior_profit (total minus today) so a fresh session never self-halts.
        double prior_profit = total_profit_ - daily_pnl_;
        if (prior_profit > 0.0 && daily_pnl_ > 0.0) {
            double daily_fraction = daily_pnl_ / prior_profit;
            if (daily_fraction > cfg_.consistency_cap_pct) {
                halt("consistency_cap: today=" + std::to_string(daily_pnl_)
                     + " / prior=" + std::to_string(prior_profit)
                     + " = " + std::to_string(daily_fraction * 100.0)
                     + "% (cap " + std::to_string(cfg_.consistency_cap_pct * 100.0) + "%)");
                return;
            }
        }

        LOG("[RISK] Trade P&L: %.2f | daily=%.2f | equity=%.2f | peak=%.2f | dd=%.2f",
            pnl_usd, daily_pnl_, equity_, peak_equity_, peak_equity_ - equity_);
    }

    // ── Check before accepting a new signal ───────────────────────────────────
    // Returns true if trading is permitted.
    bool can_trade(std::string& reason) const {
        std::lock_guard<std::mutex> lk(mu_);
        if (halted_) {
            reason = halt_reason_;
            return false;
        }
        // Re-check daily loss limit
        if (cfg_.daily_loss_limit < 0.0 && daily_pnl_ <= cfg_.daily_loss_limit) {
            reason = "daily_loss_limit active";
            return false;
        }
        // Re-check drawdown in real time (equity may have changed on open position)
        double drawdown = peak_equity_ - equity_;
        if (drawdown >= cfg_.trailing_drawdown_cap) {
            reason = "trailing_drawdown_cap active";
            return false;
        }
        return true;
    }

    bool can_trade() const {
        std::string ignored;
        return can_trade(ignored);
    }

    // ── Update equity directly (e.g. from unrealised P&L on open position) ───
    // Not used for halt checking — only on_trade_pnl() triggers halts.
    void set_equity(double eq) {
        std::lock_guard<std::mutex> lk(mu_);
        equity_ = eq;
        if (eq > peak_equity_) peak_equity_ = eq;
    }

    double equity()       const { std::lock_guard<std::mutex> lk(mu_); return equity_; }
    double peak_equity()  const { std::lock_guard<std::mutex> lk(mu_); return peak_equity_; }
    double daily_pnl()    const { std::lock_guard<std::mutex> lk(mu_); return daily_pnl_; }
    double total_profit() const { std::lock_guard<std::mutex> lk(mu_); return total_profit_; }
    bool   halted()       const { return halted_.load(std::memory_order_acquire); }

    struct Snapshot { double equity; double peak_equity; double daily_pnl; };
    Snapshot snapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        return { equity_, peak_equity_, daily_pnl_ };
    }

private:
    void halt(const std::string& reason) {
        halted_      = true;
        halt_reason_ = reason;
        LOG("[RISK] HALTED — %s", reason.c_str());
    }

    OrbConfig   cfg_;
    mutable std::mutex mu_;

    double       equity_;
    double       peak_equity_;
    double       total_profit_;
    double       daily_pnl_;
    std::atomic<bool> halted_;
    std::string  halt_reason_;
};
