#pragma once
/*  ═══════════════════════════════════════════════════════════════════════════
    sdk_md_feed.hpp — Native Rithmic R|API+ TCP market-data feed

    Replaces the WebSocket md_loop with the official Rithmic SDK for
    lower-latency tick delivery.  The SDK runs its own internal thread;
    this class bridges to the ASIO executor via asio::post() so the tick
    lands in the same single-threaded event loop as the rest of the engine.

    Build with:  cmake -DUSE_RAPI_SDK=ON ..
    ═══════════════════════════════════════════════════════════════════════════ */

#ifdef USE_RAPI_SDK

#include "orb_config.hpp"
#include "orb_strategy.hpp"
#include "order_manager.hpp"
#include "log.hpp"

#include <RApiPlus.h>

#include <boost/asio.hpp>
namespace asio = boost::asio;
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>

// ─── Rithmic SDK connection params for AMP (Rithmic 01 / prod domain) ─────────
// These match bot/config/rithmic_live.json "amp" environment.
// Pass via env vars so they can be overridden without recompile.
static inline std::string sdk_env(const char* key, const char* def) {
    const char* v = std::getenv(key);
    return (v && *v) ? v : def;
}

struct SdkConnParams {
    std::string dmn_srvr_addr = "ritpz01001.01.rithmic.com:65000~ritpz01000.01.rithmic.com:65000~ritpz01001.01.rithmic.net:65000~ritpz01000.01.rithmic.net:65000~ritpz01001.01.theomne.net:65000~ritpz01000.01.theomne.net:65000~ritpz01001.01.theomne.com:65000~ritpz01000.01.theomne.com:65000";
    std::string domain_name   = "rithmic_prod_01_dmz_domain";
    std::string lic_srvr_addr = "ritpz01000.01.rithmic.com:56000~ritpz01001.01.rithmic.com:56000~ritpz01000.01.rithmic.net:56000~ritpz01001.01.rithmic.net:56000~ritpz01000.01.theomne.net:56000~ritpz01001.01.theomne.net:56000~ritpz01000.01.theomne.com:56000~ritpz01001.01.theomne.com:56000";
    std::string loc_brok_addr = "ritpz01000.01.rithmic.com:64100";
    std::string logger_addr   = "ritpz01000.01.rithmic.com:45454~ritpz01000.01.rithmic.net:45454~ritpz01000.01.theomne.net:45454~ritpz01000.01.theomne.com:45454";
    std::string md_cnnct_pt   = "login_agent_tp_r01c";
    std::string ssl_cert_path = "sdk/etc/rithmic_ssl_cert_auth_params";

    static SdkConnParams from_env() {
        SdkConnParams p;
        p.dmn_srvr_addr = sdk_env("SDK_DMN_SRVR_ADDR", p.dmn_srvr_addr.c_str());
        p.domain_name   = sdk_env("SDK_DOMAIN_NAME",   p.domain_name.c_str());
        p.lic_srvr_addr = sdk_env("SDK_LIC_SRVR_ADDR", p.lic_srvr_addr.c_str());
        p.loc_brok_addr = sdk_env("SDK_LOC_BROK_ADDR", p.loc_brok_addr.c_str());
        p.logger_addr   = sdk_env("SDK_LOGGER_ADDR",   p.logger_addr.c_str());
        p.md_cnnct_pt   = sdk_env("SDK_MD_CNNCT_PT",   p.md_cnnct_pt.c_str());
        p.ssl_cert_path = sdk_env("SDK_SSL_CERT_PATH",  p.ssl_cert_path.c_str());
        return p;
    }
};

// ─── AdmCallbacks stub ────────────────────────────────────────────────────────
class SdkAdmCallbacks : public RApi::AdmCallbacks {
public:
    int Alert(RApi::AlertInfo* pInfo, void*, int* aiCode) override {
        *aiCode = API_OK;
        if (pInfo->iAlertType == RApi::ALERT_LOGIN_COMPLETE)
            LOG("[SDK_MD] Repository login OK");
        else if (pInfo->iAlertType == RApi::ALERT_LOGIN_FAILED)
            LOG("[SDK_MD] Repository login FAILED");
        return OK;
    }
};

// ─── Main MD callbacks ────────────────────────────────────────────────────────
class SdkMdCallbacks : public RApi::RCallbacks {
public:
    // Inject dependencies at construction — no globals needed.
    SdkMdCallbacks(boost::asio::any_io_executor ex,
                   OrbStrategy&         strategy,
                   OrderManager&        order_mgr,
                   std::atomic<bool>&   login_ok,
                   std::atomic<bool>&   auth_rejected)
        : ex_(ex), strategy_(strategy), order_mgr_(order_mgr),
          login_ok_(login_ok), auth_rejected_(auth_rejected) {}

    // ── Login / alert ─────────────────────────────────────────────────────────
    int Alert(RApi::AlertInfo* pInfo, void*, int* aiCode) override {
        *aiCode = API_OK;
        if (pInfo->iAlertType == RApi::ALERT_LOGIN_COMPLETE) {
            LOG("[SDK_MD] MD login OK");
            login_ok_.store(true, std::memory_order_release);
        } else if (pInfo->iAlertType == RApi::ALERT_LOGIN_FAILED) {
            LOG("[SDK_MD] MD login FAILED — code=%d", pInfo->iRpCode);
            auth_rejected_.store(true, std::memory_order_release);
        } else if (pInfo->iAlertType == RApi::ALERT_CONNECTION_BROKEN) {
            LOG("[SDK_MD] Connection broken");
            login_ok_.store(false, std::memory_order_release);
        } else if (pInfo->iAlertType == RApi::ALERT_FORCED_LOGOUT) {
            LOG("[SDK_MD] FORCED LOGOUT — another session using same AMP credentials");
            login_ok_.store(false, std::memory_order_release);
        }
        return OK;
    }

    // ── Tick ─────────────────────────────────────────────────────────────────
    // TradePrint fires on every last-trade update — this is the hot path.
    int TradePrint(RApi::TradeInfo* pInfo, void*, int* aiCode) override {
        *aiCode = API_OK;
        if (!pInfo->bPriceFlag || pInfo->dPrice <= 0.0 || pInfo->llSize <= 0)
            return OK;

        int64_t ts_us = static_cast<int64_t>(pInfo->iSsboe) * 1'000'000LL
                      + pInfo->iUsecs;

        // Determine aggressor side — "B" = buy aggressor, anything else = sell
        bool buy_aggressor = (pInfo->sAggressorSide.iDataLen > 0 &&
                              pInfo->sAggressorSide.pData[0] == 'B');

        OrbTick tick{ts_us, pInfo->dPrice, static_cast<int>(pInfo->llSize), buy_aggressor};

        // Post into ASIO event loop — strategy and order_mgr are NOT thread-safe.
        // asio::post() queues the lambda for execution on the executor thread.
        boost::asio::post(ex_, [this, tick]() {
            order_mgr_.check_trail_and_stop(tick.price);
            strategy_.on_tick(tick);
        });

        return OK;
    }

    // ── Stubs for all unused RCallbacks virtual methods ───────────────────────
    // Type names taken verbatim from RApiPlus.h — the STUB macro mirrors the
    // STUB_IMPL pattern used in the official SDK sample code.
#define RAPI_STUB(method, Type) \
    int method(RApi::Type* p, void*, int* c) override { (void)p; *c=API_OK; return OK; }

    RAPI_STUB(AccountList,                AccountListInfo)
    RAPI_STUB(AccountUpdate,              AccountUpdateInfo)
    RAPI_STUB(Aggregator,                 AggregatorInfo)
    RAPI_STUB(AskQuote,                   AskInfo)
    RAPI_STUB(AssignedUserList,           AssignedUserListInfo)
    RAPI_STUB(AutoLiquidate,              AutoLiquidateInfo)
    RAPI_STUB(AuxRefData,                 AuxRefDataInfo)
    RAPI_STUB(Bar,                        BarInfo)
    RAPI_STUB(BarReplay,                  BarReplayInfo)
    RAPI_STUB(BestAskQuote,               AskInfo)
    RAPI_STUB(BestBidQuote,               BidInfo)
    RAPI_STUB(BidQuote,                   BidInfo)
    RAPI_STUB(BinaryContractList,         BinaryContractListInfo)
    RAPI_STUB(BracketReplay,              BracketReplayInfo)
    RAPI_STUB(BracketTierModify,          BracketTierModifyInfo)
    RAPI_STUB(BracketUpdate,              BracketInfo)
    RAPI_STUB(BustReport,                 OrderBustReport)
    RAPI_STUB(CancelReport,               OrderCancelReport)
    RAPI_STUB(CloseMidPrice,              CloseMidPriceInfo)
    RAPI_STUB(ClosePrice,                 ClosePriceInfo)
    RAPI_STUB(ClosingIndicator,           ClosingIndicatorInfo)
    RAPI_STUB(Dbo,                        DboInfo)
    RAPI_STUB(DboBookRebuild,             DboBookRebuildInfo)
    RAPI_STUB(EasyToBorrow,               EasyToBorrowInfo)
    RAPI_STUB(EasyToBorrowList,           EasyToBorrowListInfo)
    RAPI_STUB(EndQuote,                   EndQuoteInfo)
    RAPI_STUB(EquityOptionStrategyList,   EquityOptionStrategyListInfo)
    RAPI_STUB(ExchangeList,               ExchangeListInfo)
    RAPI_STUB(ExecutionReplay,            ExecutionReplayInfo)
    RAPI_STUB(FailureReport,              OrderFailureReport)
    RAPI_STUB(FillReport,                 OrderFillReport)
    RAPI_STUB(HighBidPrice,               HighBidPriceInfo)
    RAPI_STUB(HighPrice,                  HighPriceInfo)
    RAPI_STUB(HighPriceLimit,             HighPriceLimitInfo)
    RAPI_STUB(IbList,                     IbListInfo)
    RAPI_STUB(InstrumentByUnderlying,     InstrumentByUnderlyingInfo)
    RAPI_STUB(InstrumentSearch,           InstrumentSearchInfo)
    RAPI_STUB(LimitOrderBook,             LimitOrderBookInfo)
    RAPI_STUB(LineUpdate,                 LineInfo)
    RAPI_STUB(LowPrice,                   LowPriceInfo)
    RAPI_STUB(MarketMode,                 MarketModeInfo)
    RAPI_STUB(ModifyReport,               OrderModifyReport)
    RAPI_STUB(NotCancelledReport,         OrderNotCancelledReport)
    RAPI_STUB(NotModifiedReport,          OrderNotModifiedReport)
    RAPI_STUB(OpenInterest,               OpenInterestInfo)
    RAPI_STUB(OpeningIndicator,           OpeningIndicatorInfo)
    RAPI_STUB(OpenOrderReplay,            OrderReplayInfo)
    RAPI_STUB(OpenPrice,                  OpenPriceInfo)
    RAPI_STUB(OptionList,                 OptionListInfo)
    RAPI_STUB(OtherReport,                OrderReport)
    RAPI_STUB(PasswordChange,             PasswordChangeInfo)
    RAPI_STUB(PnlReplay,                  PnlReplayInfo)
    RAPI_STUB(PnlUpdate,                  PnlInfo)
    RAPI_STUB(PriceIncrUpdate,            PriceIncrInfo)
    RAPI_STUB(ProductRmsList,             ProductRmsListInfo)
    RAPI_STUB(Quote,                      QuoteReport)
    RAPI_STUB(RefData,                    RefDataInfo)
    RAPI_STUB(RejectReport,               OrderRejectReport)
    RAPI_STUB(SettlementPrice,            SettlementPriceInfo)
    RAPI_STUB(SingleOrderReplay,          SingleOrderReplayInfo)
    RAPI_STUB(SodUpdate,                  SodReport)
    RAPI_STUB(StatusReport,               OrderStatusReport)
    RAPI_STUB(Strategy,                   StrategyInfo)
    RAPI_STUB(StrategyList,               StrategyListInfo)
    RAPI_STUB(TradeCondition,             TradeInfo)
    RAPI_STUB(TradeCorrectReport,         OrderTradeCorrectReport)
    RAPI_STUB(TradeReplay,                TradeReplayInfo)
    RAPI_STUB(TradeRoute,                 TradeRouteInfo)
    RAPI_STUB(TradeRouteList,             TradeRouteListInfo)
    RAPI_STUB(TradeVolume,                TradeVolumeInfo)
    RAPI_STUB(TriggerPulledReport,        OrderTriggerPulledReport)
    RAPI_STUB(TriggerReport,              OrderTriggerReport)
    int BestBidAskQuote(RApi::BidInfo* b, RApi::AskInfo* a, void*, int* c) override { (void)b; (void)a; *c=API_OK; return OK; }

#undef RAPI_STUB

    // AgreementList needed for login flow
    int AgreementList(RApi::AgreementListInfo* pInfo, void*, int* aiCode) override {
        (void)pInfo;
        *aiCode = API_OK;
        return OK;
    }

private:
    boost::asio::any_io_executor ex_;
    OrbStrategy&          strategy_;
    OrderManager&         order_mgr_;
    std::atomic<bool>&    login_ok_;
    std::atomic<bool>&    auth_rejected_;
};

// ─── SdkMdFeed — RAII wrapper around REngine for the executor ─────────────────
class SdkMdFeed {
public:
    SdkMdFeed(const OrbConfig&     cfg,
              const SdkConnParams& conn,
              boost::asio::any_io_executor ex,
              OrbStrategy&         strategy,
              OrderManager&        order_mgr)
        : cfg_(cfg), conn_(conn), ex_(ex),
          callbacks_(ex, strategy, order_mgr, login_ok_, auth_rejected_) {}

    ~SdkMdFeed() { stop(); }

    // True if the last start() failure was an explicit server rejection (not a timeout/network error).
    bool auth_rejected() const { return auth_rejected_.load(std::memory_order_acquire); }

    // Start the SDK engine and block until login completes (or 10s timeout).
    // Returns true on success.
    bool start() {
        int iCode = 0;

        // Build envp strings required by REngine
        std::string e0 = "MML_DMN_SRVR_ADDR="  + conn_.dmn_srvr_addr;
        std::string e1 = "MML_DOMAIN_NAME="     + conn_.domain_name;
        std::string e2 = "MML_LIC_SRVR_ADDR="   + conn_.lic_srvr_addr;
        std::string e3 = "MML_LOC_BROK_ADDR="   + conn_.loc_brok_addr;
        std::string e4 = "MML_LOGGER_ADDR="      + conn_.logger_addr;
        std::string e5 = "MML_LOG_TYPE=log_net";
        std::string e6 = "MML_SSL_CLNT_AUTH_FILE=" + conn_.ssl_cert_path;
        std::string e7 = "USER=" + cfg_.md_user;

        char* envp[9];
        envp[0] = const_cast<char*>(e0.c_str());
        envp[1] = const_cast<char*>(e1.c_str());
        envp[2] = const_cast<char*>(e2.c_str());
        envp[3] = const_cast<char*>(e3.c_str());
        envp[4] = const_cast<char*>(e4.c_str());
        envp[5] = const_cast<char*>(e5.c_str());
        envp[6] = const_cast<char*>(e6.c_str());
        envp[7] = const_cast<char*>(e7.c_str());
        envp[8] = nullptr;

        RApi::REngineParams params;
        params.sAppName.pData        = const_cast<char*>(cfg_.app_name.c_str());
        params.sAppName.iDataLen     = (int)cfg_.app_name.size();
        params.sAppVersion.pData     = const_cast<char*>(cfg_.app_version.c_str());
        params.sAppVersion.iDataLen  = (int)cfg_.app_version.size();
        params.envp                  = envp;
        params.pAdmCallbacks         = &adm_callbacks_;
        params.sLogFilePath.pData    = const_cast<char*>("data/logs/sdk_md.log");
        params.sLogFilePath.iDataLen = (int)strlen("data/logs/sdk_md.log");

        try { engine_ = new RApi::REngine(&params); }
        catch (OmneException& e) {
            LOG("[SDK_MD] REngine create failed: %d", e.getErrorCode());
            return false;
        }

        RApi::LoginParams login;
        login.pCallbacks           = &callbacks_;
        login.sMdCnnctPt.pData     = const_cast<char*>(conn_.md_cnnct_pt.c_str());
        login.sMdCnnctPt.iDataLen  = (int)conn_.md_cnnct_pt.size();
        login.sMdUser.pData        = const_cast<char*>(cfg_.md_user.c_str());
        login.sMdUser.iDataLen     = (int)cfg_.md_user.size();
        login.sMdPassword.pData    = const_cast<char*>(cfg_.md_password.c_str());
        login.sMdPassword.iDataLen = (int)cfg_.md_password.size();

        if (!engine_->login(&login, &iCode)) {
            LOG("[SDK_MD] login() failed: %d", iCode);
            return false;
        }

        // Wait up to 10s for MD login callback
        for (int i = 0; i < 100; ++i) {
            if (login_ok_.load(std::memory_order_acquire)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!login_ok_.load()) {
            LOG("[SDK_MD] Login timed out");
            return false;
        }

        // Subscribe to last trade ticks
        std::string exch = cfg_.exchange;
        std::string sym  = cfg_.trade_contract.empty() ? cfg_.symbol : cfg_.trade_contract;

        tsNCharcb sExchange{ const_cast<char*>(exch.c_str()), (int)exch.size() };
        tsNCharcb sTicker  { const_cast<char*>(sym.c_str()),  (int)sym.size()  };

        int flags = RApi::MD_PRINTS;  // last-trade ticks only; add RApi::MD_BEST for BBO

        if (!engine_->subscribe(&sExchange, &sTicker, flags, &iCode)) {
            LOG("[SDK_MD] subscribe() failed: %d", iCode);
            return false;
        }

        LOG("[SDK_MD] Subscribed to %s/%s via TCP (native R|API+)", sym.c_str(), exch.c_str());
        return true;
    }

    void stop() {
        if (engine_) {
            int iCode = 0;
            engine_->logout(&iCode);
            delete engine_;
            engine_ = nullptr;
        }
    }

    bool is_connected() const { return login_ok_.load(std::memory_order_acquire); }

private:
    const OrbConfig&    cfg_;
    const SdkConnParams conn_;
    boost::asio::any_io_executor ex_;
    std::atomic<bool>   login_ok_{false};
    std::atomic<bool>   auth_rejected_{false};
    SdkAdmCallbacks     adm_callbacks_;
    SdkMdCallbacks      callbacks_;
    RApi::REngine*      engine_{nullptr};
};

#endif  // USE_RAPI_SDK
