#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    order_manager.hpp — Position state machine + order lifecycle

    State machine:
        FLAT → PENDING_ENTRY → LONG/SHORT → PENDING_EXIT → FLAT

    Thread safety:
        All state transitions are guarded by state_mu_.
        OrderManager methods may be called from:
          - io_context thread (on_signal, check_trail)
          - fill callback thread (on_fill_notification)
        std::mutex ensures no double-entry races.

    Rithmic ORDER_PLANT integration:
        In dry_run=true mode, orders are logged but not sent.
        In live mode, order_send_cb_ is called with a serialised
        RequestNewOrder protobuf (caller handles the WS send).
    ═══════════════════════════════════════════════════════════════════════════ */
#include "orb_config.hpp"
#include "orb_strategy.hpp"
#include "latency_logger.hpp"
#include "risk_manager.hpp"
#include "log.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <cmath>

// ─── Position states ──────────────────────────────────────────────────────────
enum class PosState { FLAT, PENDING_ENTRY, LONG, SHORT, PENDING_EXIT };

// ─── Open position record ─────────────────────────────────────────────────────
struct Position {
    PosState  state       = PosState::FLAT;
    OrbSignal direction   = OrbSignal::NONE;  // BUY=LONG, SELL=SHORT

    std::string basket_id_entry;
    std::string basket_id_exit;
    std::string basket_id_stop;   // exchange stop order tracking

    double entry_price    = 0.0;
    double exit_price     = 0.0;
    double sl_price       = 0.0;     // current stop price
    double trigger_price  = 0.0;     // ORB breakout level that triggered the entry order
    double fill_price_actual = 0.0;  // actual execution price from fill notification
    double mfe            = 0.0;     // max favourable excursion in points
    double mae            = 0.0;     // max adverse excursion in points
    int    qty            = 1;

    // Trailing state
    bool   trailing_active = false;
    std::chrono::steady_clock::time_point fill_time;

    // Exit info
    std::string exit_reason;
    double      pnl_points = 0.0;
    double      pnl_usd    = 0.0;
};

// ─── Callback types ───────────────────────────────────────────────────────────
// order_type: 2=MKT, 1=LMT, 4=STOP_MARKET
using OrderSendCallback = std::function<bool(
    const std::string& basket_id,
    const std::string& symbol,
    const std::string& exchange,
    int qty,
    int order_type,
    bool is_buy,
    double price,
    const std::string& user_tag
)>;
using OrderCancelCallback = std::function<void(const std::string& basket_id)>;
// Atomically changes the trigger_price of an existing STOP_MARKET order.
// Returns true if the modify was sent successfully.
using OrderModifyCallback = std::function<bool(
    const std::string& basket_id,
    double new_trigger_price
)>;

// ─── OrderManager ─────────────────────────────────────────────────────────────
class OrderManager {
public:
    explicit OrderManager(const OrbConfig& cfg,
                          RiskManager& risk,
                          LatencyLogger& lat)
        : cfg_(cfg), risk_(risk), lat_(lat)
    {}

    void set_order_callback(OrderSendCallback cb)   { order_cb_  = std::move(cb); }
    void set_cancel_callback(OrderCancelCallback cb) { cancel_cb_ = std::move(cb); }
    void set_modify_callback(OrderModifyCallback cb) { modify_cb_ = std::move(cb); }

    // ── Called by OrbStrategy signal callback ─────────────────────────────────
    void on_signal(OrbSignal sig, double price, const std::string& reason) {
        if (sig == OrbSignal::FLATTEN_EOD) {
            flatten_now("eod_flatten");
            return;
        }
        if (sig != OrbSignal::BUY && sig != OrbSignal::SELL) return;

        // Risk check
        std::string halt_reason;
        if (!risk_.can_trade(halt_reason)) {
            LOG("[OM] Signal rejected by risk: %s", halt_reason.c_str());
            return;
        }

        std::lock_guard<std::mutex> lk(state_mu_);
        if (pos_.state != PosState::FLAT) {
            LOG("[OM] Signal ignored — not FLAT (state=%d)", (int)pos_.state);
            return;
        }

        bool is_buy = (sig == OrbSignal::BUY);
        std::string basket = new_basket_id();

        LOG("[OM] %s Entry signal: price=%.2f basket=%s reason=%s%s",
            is_buy ? "LONG" : "SHORT", price, basket.c_str(), reason.c_str(),
            cfg_.dry_run ? " [DRY_RUN]" : "");

        lat_.on_signal(basket, price, /*is_entry=*/true);

        pos_ = Position{};
        pos_.state         = PosState::PENDING_ENTRY;
        pos_.direction     = sig;
        pos_.basket_id_entry = basket;
        pos_.qty           = cfg_.qty;
        pos_.trigger_price = price;   // ORB breakout level at time of order submission
        pos_.fill_time     = std::chrono::steady_clock::now(); // placeholder until fill

        send_market_order(basket, is_buy, price, "entry");
    }

    // ── Fill notification from ORDER_PLANT ────────────────────────────────────
    // Called with state_mu_ already held (dry-run path inside send_market_order)
    void on_fill_notification_locked(const std::string& basket_id,
                                     double fill_price,
                                     int fill_qty,
                                     bool is_entry_fill) {

        if (is_entry_fill) {
            if (pos_.state != PosState::PENDING_ENTRY) {
                LOG("[OM] Spurious entry fill basket=%s (state=%d)",
                    basket_id.c_str(), (int)pos_.state);
                return;
            }
            if (pos_.basket_id_entry != basket_id) {
                LOG("[OM] Entry fill basket mismatch: got=%s expected=%s",
                    basket_id.c_str(), pos_.basket_id_entry.c_str());
                return;
            }

            pos_.entry_price       = fill_price;
            pos_.fill_price_actual = fill_price;  // actual fill for slippage calc
            pos_.fill_time         = std::chrono::steady_clock::now();
            pos_.state = (pos_.direction == OrbSignal::BUY)
                         ? PosState::LONG : PosState::SHORT;

            // Place stop-loss
            double sl = compute_sl(fill_price, pos_.direction);
            pos_.sl_price = sl;

            auto lat_rec = lat_.on_fill(basket_id, fill_price);
            LOG("[OM] FILL entry: basket=%s price=%.2f sl=%.2f slippage=%dtick ($%.2f)",
                basket_id.c_str(), fill_price, sl,
                lat_rec.slippage_ticks, lat_rec.slippage_usd);

            last_entry_lat_ = lat_rec;

            // Submit exchange-level stop order immediately after fill
            submit_stop_order_locked(sl);

        } else {
            // Exit fill (market exit or stop fill)
            if (pos_.state != PosState::PENDING_EXIT &&
                pos_.state != PosState::LONG &&
                pos_.state != PosState::SHORT) {
                // Check for stale stop fill: cancel raced and stop fired anyway
                if (!last_stop_for_unwind_.empty() && basket_id == last_stop_for_unwind_) {
                    bool unwind_is_buy = !last_stop_was_buy_;
                    LOG("[OM] STALE STOP FILL basket=%s px=%.2f — auto-unwind %s MKT",
                        basket_id.c_str(), fill_price,
                        unwind_is_buy ? "BUY" : "SELL");
                    last_stop_for_unwind_.clear();
                    if (order_cb_) {
                        std::string basket = new_basket_id();
                        lat_.on_signal(basket, fill_price, false);
                        bool ok = order_cb_(basket, cfg_.symbol, cfg_.exchange,
                                            cfg_.qty, /*MARKET=2*/2, unwind_is_buy,
                                            0.0, "stale_stop_unwind");
                        if (ok) lat_.on_submit(basket, fill_price);
                    }
                } else {
                    LOG("[OM] Spurious exit fill basket=%s (state=%d)",
                        basket_id.c_str(), (int)pos_.state);
                }
                return;
            }

            // Exchange stop filled directly (state still LONG/SHORT) — set exit reason
            if (pos_.state == PosState::LONG || pos_.state == PosState::SHORT) {
                pos_.exit_reason = (basket_id == pos_.basket_id_stop)
                    ? "exchange_stop" : "unknown_exit";
                pos_.state = PosState::PENDING_EXIT;
            }

            pos_.exit_price  = fill_price;
            double pts = (pos_.direction == OrbSignal::BUY)
                         ? fill_price - pos_.entry_price
                         : pos_.entry_price - fill_price;
            pos_.pnl_points = pts;
            pos_.pnl_usd    = pts * cfg_.point_value
                              - 2.0 * NQ_COMMISSION; // round-turn

            // Cancel the exchange stop order if it wasn't the one that just filled
            if (!pos_.basket_id_stop.empty() && pos_.basket_id_stop != basket_id) {
                cancel_stop_locked();
            }

            auto lat_rec = lat_.on_fill(basket_id, fill_price);
            LOG("[OM] FILL exit: basket=%s price=%.2f pnl=%.2fpts ($%.2f) slippage=%dtick",
                basket_id.c_str(), fill_price, pts, pos_.pnl_usd,
                lat_rec.slippage_ticks);

            last_exit_lat_ = lat_rec;
            completed_pos_ = pos_;
            completed_pos_.exit_price = fill_price;

            risk_.on_trade_pnl(pos_.pnl_usd);

            pos_ = Position{};  // back to FLAT
            trade_completed_ = true;
        }
    }

    // Public entry point — acquires lock then delegates to _locked variant
    void on_fill_notification(const std::string& basket_id,
                               double fill_price,
                               int fill_qty,
                               bool is_entry_fill) {
        std::lock_guard<std::mutex> lk(state_mu_);
        on_fill_notification_locked(basket_id, fill_price, fill_qty, is_entry_fill);
    }

    // ── Periodic check: trailing stop and SL hit (call every tick or 1s) ─────
    void check_trail_and_stop(double current_price) {
        std::lock_guard<std::mutex> lk(state_mu_);
        if (pos_.state != PosState::LONG && pos_.state != PosState::SHORT) return;

        bool is_long = (pos_.state == PosState::LONG);
        double mfe_now = is_long
            ? current_price - pos_.entry_price
            : pos_.entry_price - current_price;
        double mae_now = is_long
            ? pos_.entry_price - current_price
            : current_price - pos_.entry_price;

        if (mfe_now > pos_.mfe) pos_.mfe = mfe_now;
        if (mae_now > pos_.mae) pos_.mae = mae_now;

        // Software SL only fires if no exchange stop is active (fallback for rejected stops).
        if (pos_.basket_id_stop.empty()) {
            if (is_long && current_price <= pos_.sl_price) {
                LOG("[OM] Software SL hit (LONG, no exchange stop): price=%.2f sl=%.2f",
                    current_price, pos_.sl_price);
                initiate_exit_locked("stop_loss", current_price);
                return;
            }
            if (!is_long && current_price >= pos_.sl_price) {
                LOG("[OM] Software SL hit (SHORT, no exchange stop): price=%.2f sl=%.2f",
                    current_price, pos_.sl_price);
                initiate_exit_locked("stop_loss", current_price);
                return;
            }
        }

        // Check trailing activation
        if (!pos_.trailing_active) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - pos_.fill_time).count();
            bool delay_met  = (elapsed >= cfg_.trail_delay_secs);
            bool mfe_met    = (pos_.mfe >= cfg_.trail_be_trigger);

            if (delay_met && mfe_met) {
                pos_.trailing_active = true;
                double be_sl = is_long
                    ? pos_.entry_price + cfg_.trail_be_offset
                    : pos_.entry_price - cfg_.trail_be_offset;
                if ((is_long && be_sl > pos_.sl_price) ||
                    (!is_long && be_sl < pos_.sl_price)) {
                    double old_sl = pos_.sl_price;
                    pos_.sl_price = be_sl;
                    LOG("[OM] Trailing activated — SL moved to BE+offset: %.2f", be_sl);
                    update_stop_order_locked(old_sl, be_sl, current_price);
                }
            }
        }

        // Update trailing stop
        if (pos_.trailing_active) {
            double trail_sl = is_long
                ? current_price - cfg_.trail_step
                : current_price + cfg_.trail_step;

            if ((is_long && trail_sl > pos_.sl_price) ||
                (!is_long && trail_sl < pos_.sl_price)) {
                double old_sl = pos_.sl_price;
                pos_.sl_price = trail_sl;
                LOG("[OM] Trail updated: price=%.2f new_sl=%.2f", current_price, trail_sl);
                update_stop_order_locked(old_sl, trail_sl, current_price);
            }
        }
    }

    // ── Force flatten (EOD or kill switch) ────────────────────────────────────
    void flatten_now(const std::string& reason) {
        std::lock_guard<std::mutex> lk(state_mu_);
        if (pos_.state == PosState::FLAT) {
            LOG("[OM] flatten_now('%s') — already flat", reason.c_str());
            return;
        }
        if (pos_.state == PosState::PENDING_ENTRY) {
            LOG("[OM] flatten_now('%s') — cancelling pending entry basket=%s",
                reason.c_str(), pos_.basket_id_entry.c_str());
            if (cancel_cb_ && !pos_.basket_id_entry.empty())
                cancel_cb_(pos_.basket_id_entry);
            pos_ = Position{};
            return;
        }
        if (pos_.state == PosState::PENDING_EXIT) {
            LOG("[OM] flatten_now('%s') — exit already pending", reason.c_str());
            return;
        }
        initiate_exit_locked(reason, 0.0);
    }

    // ── Reject notification (entry rejected by exchange) ─────────────────────
    void on_order_rejected(const std::string& basket_id, const std::string& msg) {
        std::lock_guard<std::mutex> lk(state_mu_);
        LOG("[OM] Order rejected basket=%s msg=%s", basket_id.c_str(), msg.c_str());
        if (pos_.basket_id_entry == basket_id && pos_.state == PosState::PENDING_ENTRY) {
            pos_ = Position{};
            LOG("[OM] Reverted to FLAT after entry rejection");
        } else if (pos_.basket_id_stop == basket_id) {
            // Stop order rejected — clear basket so software SL fallback activates
            pos_.basket_id_stop.clear();
            LOG("[OM] CRITICAL: Exchange stop rejected — software SL fallback now active (sl=%.2f)",
                pos_.sl_price);
        }
    }

    // ── Cancel ACK — stop was cancelled (position exit path only with modify-order trail) ──
    void on_cancel_confirmed(const std::string& basket_id) {
        std::lock_guard<std::mutex> lk(state_mu_);
        LOG("[OM] Cancel ACK basket=%s (stop removed at position close)", basket_id.c_str());
    }

    // ── Server basket_id for stop order (from tid=351 notifications) ─────────
    // The exchange assigns its own basket_id; RequestModifyOrder needs it, not our user_tag.
    void set_stop_server_basket(const std::string& server_basket_id) {
        std::lock_guard<std::mutex> lk(state_mu_);
        stop_server_basket_ = server_basket_id;
        LOG("[OM] Stop server basket_id mapped: client=%s server=%s",
            pos_.basket_id_stop.c_str(), server_basket_id.c_str());
    }

    // ── Modify response from exchange — if rejected, fall back to cancel+resubmit ──
    void on_modify_response(bool accepted, const std::string& rp_code) {
        std::lock_guard<std::mutex> lk(state_mu_);
        if (!pending_modify_) return;
        pending_modify_ = false;
        if (accepted) {
            LOG("[OM] Stop modify ACKed by exchange (new_sl=%.2f)", pending_modify_new_sl_);
            pending_modify_new_sl_ = 0.0;
        } else {
            double new_sl = pending_modify_new_sl_;
            pending_modify_new_sl_ = 0.0;
            LOG("[OM] Stop modify REJECTED (rp_code=%s) — cancel+resubmit at %.2f",
                rp_code.c_str(), new_sl);
            if (pos_.state != PosState::LONG && pos_.state != PosState::SHORT) return;
            cancel_stop_locked();
            submit_stop_order_locked(new_sl);
        }
    }

    bool has_pending_modify() const {
        std::lock_guard<std::mutex> lk(state_mu_);
        return pending_modify_;
    }

    // ── Read-only position snapshot for DB / UI writes ────────────────────────
    // Returns a copy of the current Position so the caller can build a
    // write_position() call without holding the mutex during DB I/O.
    Position position_snapshot() const {
        std::lock_guard<std::mutex> lk(state_mu_);
        return pos_;
    }

    bool is_flat() const {
        std::lock_guard<std::mutex> lk(state_mu_);
        return pos_.state == PosState::FLAT;
    }

    bool is_entry_basket(const std::string& basket_id) const {
        std::lock_guard<std::mutex> lk(state_mu_);
        return pos_.basket_id_entry == basket_id;
    }

    bool is_stop_basket(const std::string& basket_id) const {
        std::lock_guard<std::mutex> lk(state_mu_);
        return !pos_.basket_id_stop.empty() && pos_.basket_id_stop == basket_id;
    }

    PosState state() const {
        std::lock_guard<std::mutex> lk(state_mu_);
        return pos_.state;
    }

    // Returns true (and clears flag) when a trade completed since last check
    bool pop_trade_completed(Position& out) {
        std::lock_guard<std::mutex> lk(state_mu_);
        if (!trade_completed_) return false;
        out = completed_pos_;
        trade_completed_ = false;
        return true;
    }

    const TradeLatency& last_entry_lat() const { return last_entry_lat_; }
    const TradeLatency& last_exit_lat()  const { return last_exit_lat_; }

private:
    OrbConfig      cfg_;
    RiskManager&   risk_;
    LatencyLogger& lat_;
    OrderSendCallback   order_cb_;
    OrderCancelCallback cancel_cb_;
    OrderModifyCallback modify_cb_;

    mutable std::mutex state_mu_;
    Position           pos_;
    Position           completed_pos_;
    bool               trade_completed_ = false;

    TradeLatency last_entry_lat_;
    TradeLatency last_exit_lat_;

    // Server-assigned basket_id for the current stop order (needed for RequestModifyOrder)
    std::string stop_server_basket_;
    // Pending modify state: tracks an in-flight modify so rejection triggers fallback
    bool        pending_modify_      = false;
    double      pending_modify_new_sl_ = 0.0;

    // Stale stop unwind state (fires when old stop fills after position already closed)
    std::string last_stop_for_unwind_;      // basket of stop sent to cancel at position close
    bool        last_stop_was_buy_ = false; // true = stop was a BUY (SHORT position)

    static std::atomic<uint64_t> seq_;  // monotonic sequence for basket IDs

    std::string new_basket_id() {
        // Format: "NQ-<epoch_ms>-<seq>"
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return cfg_.symbol + "-" + std::to_string(ms) + "-" + std::to_string(seq_.fetch_add(1));
    }

    double compute_sl(double fill_price, OrbSignal dir) const {
        if (dir == OrbSignal::BUY)  return fill_price - cfg_.sl_points;
        if (dir == OrbSignal::SELL) return fill_price + cfg_.sl_points;
        return fill_price;
    }

    void send_market_order(const std::string& basket,
                           bool is_buy,
                           double ref_price,
                           const std::string& user_tag) {
        lat_.on_submit(basket, ref_price);

        if (cfg_.dry_run) {
            LOG("[OM] [DRY_RUN] Would send MKT %s qty=%d basket=%s",
                is_buy ? "BUY" : "SELL", cfg_.qty, basket.c_str());
            on_fill_notification_locked(basket, ref_price, cfg_.qty, /*is_entry=*/true);
            return;
        }

        if (!order_cb_) {
            LOG("[OM] ERROR: no order callback set — cannot send order");
            return;
        }

        // Use aggressive limit instead of market: 4 ticks past signal price.
        // Legends prop accounts reject market orders; limit with offset fills immediately.
        constexpr double TICK = 0.25;
        constexpr int    OFFSET_TICKS = 4;
        double limit_px = is_buy ? ref_price + OFFSET_TICKS * TICK
                                 : ref_price - OFFSET_TICKS * TICK;
        bool ok = order_cb_(basket, cfg_.symbol, cfg_.exchange,
                             cfg_.qty, /*LIMIT=1*/1, is_buy, limit_px, user_tag);
        if (!ok) {
            LOG("[OM] ERROR: order_cb_ returned false for basket=%s", basket.c_str());
            pos_ = Position{};
        }
    }

    // Submit exchange stop order (called while state_mu_ held)
    void submit_stop_order_locked(double sl_price) {
        if (cfg_.dry_run || !order_cb_) return;
        bool is_long = (pos_.state == PosState::LONG);
        bool stop_is_sell = is_long;
        std::string basket = new_basket_id();
        pos_.basket_id_stop = basket;
        stop_server_basket_.clear();   // new stop — server basket_id not yet known
        pending_modify_      = false;  // clear any stale pending modify
        LOG("[OM] Submitting exchange STOP_MARKET %s at %.2f basket=%s",
            stop_is_sell ? "SELL" : "BUY", sl_price, basket.c_str());
        // Pre-populate latency record so exchange-stop fills get meaningful metrics
        lat_.on_signal(basket, sl_price, /*is_entry=*/false);
        lat_.on_submit(basket, sl_price);
        bool ok = order_cb_(basket, cfg_.symbol, cfg_.exchange,
                            cfg_.qty, /*STOP_MARKET=4*/4, !stop_is_sell, sl_price, "stop_loss");
        if (!ok) {
            LOG("[OM] ERROR: stop order send failed — clearing basket, software SL active");
            pos_.basket_id_stop.clear();
        }
    }

    // Update stop to new_sl via cancel+resubmit (Legends rejects RequestModifyOrder).
    // Before resubmitting, validates stop is still on the correct side of current_price.
    // If price has already blown through the new SL, exits immediately instead.
    //
    // Race window: between RequestCancelOrder and the cancel ACK, the old stop is still
    // live on the exchange. If it fires during this window, on_fill_notification_locked
    // handles it via the existing exit-fill path (pos_.state is still LONG/SHORT, the
    // basket_id won't match pos_.basket_id_stop which already holds the new basket, so
    // exit_reason="unknown_exit" and the new stop is cancelled via cancel_stop_locked).
    // The stale-stop unwind guard (last_stop_for_unwind_) handles the symmetric case
    // where the fill arrives *after* the position has already gone FLAT.
    // Net risk: a trailing move can trigger at the old SL level instead of the new one
    // — effectively a one-trail-step slip. Acceptable given Legends' modify restriction.
    void update_stop_order_locked(double /*old_sl*/, double new_sl,
                                  double current_price = 0.0) {
        if (cfg_.dry_run) {
            LOG("[OM] [DRY_RUN] Trail: would update stop to %.2f", new_sl);
            return;
        }
        bool is_long = (pos_.state == PosState::LONG);

        // If price already past new SL, no point placing a stop — exit immediately.
        if (current_price > 0.0) {
            bool already_hit = is_long ? (current_price <= new_sl)
                                       : (current_price >= new_sl);
            if (already_hit) {
                LOG("[OM] Trail: price=%.2f already past new SL=%.2f — exiting immediately",
                    current_price, new_sl);
                pos_.sl_price = new_sl;
                initiate_exit_locked("stop_loss_trail", current_price);
                return;
            }
        }

        if (pos_.basket_id_stop.empty()) {
            submit_stop_order_locked(new_sl);
            return;
        }
        LOG("[OM] Trail update: cancel+resubmit stop %s trigger=%.2f",
            pos_.basket_id_stop.c_str(), new_sl);
        cancel_stop_locked();
        submit_stop_order_locked(new_sl);
    }

    // Cancel stop without replacing (called while state_mu_ held)
    void cancel_stop_locked() {
        if (pos_.basket_id_stop.empty() || !cancel_cb_) return;
        // Save basket so we can detect stale fills after position closes
        last_stop_for_unwind_ = pos_.basket_id_stop;
        last_stop_was_buy_    = (pos_.direction == OrbSignal::SELL); // SHORT has BUY stop
        LOG("[OM] Cancelling stop basket=%s (position closed)", pos_.basket_id_stop.c_str());
        cancel_cb_(pos_.basket_id_stop);
        pos_.basket_id_stop.clear();
        stop_server_basket_.clear();
        pending_modify_      = false;
        pending_modify_new_sl_ = 0.0;
    }

    void initiate_exit_locked(const std::string& reason, double ref_price) {
        if (pos_.state != PosState::LONG && pos_.state != PosState::SHORT) return;

        // Cancel the exchange stop BEFORE submitting market exit to prevent double-fill.
        cancel_stop_locked();

        pos_.state       = PosState::PENDING_EXIT;
        pos_.exit_reason = reason;

        bool exit_is_buy = (pos_.direction == OrbSignal::SELL); // SHORT → exit BUY

        std::string basket = new_basket_id();
        pos_.basket_id_exit = basket;

        LOG("[OM] Initiating exit: reason=%s ref_price=%.2f basket=%s%s",
            reason.c_str(), ref_price, basket.c_str(),
            cfg_.dry_run ? " [DRY_RUN]" : "");

        lat_.on_signal(basket, ref_price, /*is_entry=*/false);

        if (cfg_.dry_run) {
            LOG("[OM] [DRY_RUN] Would send MKT %s qty=%d basket=%s",
                exit_is_buy ? "BUY" : "SELL", pos_.qty, basket.c_str());
            on_fill_notification_locked(basket, ref_price, pos_.qty, /*is_entry=*/false);
            return;
        }

        if (!order_cb_) {
            LOG("[OM] ERROR: no order callback for exit basket=%s", basket.c_str());
            return;
        }

        lat_.on_submit(basket, ref_price);
        bool ok = order_cb_(basket, cfg_.symbol, cfg_.exchange,
                             pos_.qty, /*MARKET=2*/2, exit_is_buy, 0.0, reason);
        if (!ok) {
            LOG("[OM] ERROR: exit order_cb_ returned false basket=%s", basket.c_str());
        }
    }
};

// Static member definition (in header because it's a header-only class)
inline std::atomic<uint64_t> OrderManager::seq_{0};
