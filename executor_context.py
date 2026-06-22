"""
ExecutorContext — per-executor isolated state container.

Each executor (paper, testnet) holds its own:
  - trades list
  - cooldown dicts
  - account balance
  - CSV / state file paths
  - Telegram mode prefix

Signal generation is SHARED.
Execution lifecycle is ISOLATED via this context.
"""
import threading


class ExecutorContext:
    def __init__(
        self,
        name,
        account_balance,
        trades_csv,
        state_file,
        mode_prefix,
        execution_mode,
        pause_until=0,
        equity_peak=None,
    ):
        self.name = name
        self.account_balance = account_balance
        self.equity_peak = equity_peak if equity_peak is not None else account_balance
        self.pause_until = pause_until
        self.trades_csv = trades_csv
        self.state_file = state_file
        self.mode_prefix = mode_prefix
        self.execution_mode = execution_mode

        self.trades = []
        self.entry_cooldown = {}
        self.cooldown = {}
        self.signal_state = {}
        self.lock = threading.Lock()
        self.live_pending_slots = 0

        self.initial_balance = account_balance
        self.emergency_close_count = 0

        self.early_count = 0
        self.confirm_count_this_cycle = 0
        self.session_pnl_r = 0.0

        self.stats = {
            "win": 0,
            "loss": 0,
            "be": 0,
            "opened": 0,
            "entry": 0,
            "sent": 0,
            "entry_type_stats": {},
            "bos_type_stats": {},
            "market_mode_stats": {},
            "exhaustion_stats": {},
            "wyckoff_stats": {},
        }

        self.recon_orphan_count = 0

    def load_trades(self):
        from state_manager import load_open_trades
        try:
            self.trades = load_open_trades(self.state_file)
            print(f"[{self.name.upper()} CTX] Loaded {len(self.trades)} trades from {self.state_file}")
        except RuntimeError as e:
            print(
                f"[CRITICAL] {self.name.upper()} CTX — trade hydration failed. "
                f"Cannot start executor safely. state_file={self.state_file}"
            )
            raise

    def load_account_state(self):
        if self.execution_mode not in ("testnet", "live"):
            return
        import json
        import os
        _state_filename = (
            "testnet_account_state.json"
            if self.execution_mode == "testnet"
            else "live_account_state.json"
        )
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), _state_filename)
        _tag = self.execution_mode.upper()
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    state = json.load(f)
                self.account_balance = state.get("account_balance", self.account_balance)
                self.equity_peak = state.get("equity_peak", self.account_balance)
                self.pause_until = state.get("pause_until", 0)
                print(f"[{_tag}] Execution balance restored: {round(self.account_balance, 2)} USDT")
            except Exception as e:
                print(
                    f"[CRITICAL] {_state_filename} corrupted — "
                    f"starting from configured balance. error={e}"
                )
        else:
            print(f"[{_tag}] No prior state — starting from {round(self.account_balance, 2)} USDT")

    def save_account_state(self):
        if self.execution_mode not in ("testnet", "live"):
            return
        import os
        from state_manager import atomic_save_json
        _state_filename = (
            "testnet_account_state.json"
            if self.execution_mode == "testnet"
            else "live_account_state.json"
        )
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), _state_filename)
        state = {
            "account_balance": self.account_balance,
            "equity_peak": self.equity_peak,
            "pause_until": self.pause_until,
        }
        atomic_save_json(state, path)

    def __repr__(self):
        open_count = sum(1 for t in self.trades if t.get("status") == "OPEN")
        return (
            f"ExecutorContext(name={self.name!r}, mode={self.execution_mode!r}, "
            f"open_trades={open_count}, balance={round(self.account_balance, 2)})"
        )
