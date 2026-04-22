# Dealer Hedging Mathematics — Foundations of the Hedging Signal Layer

> **Audience.** Quants and traders who want the math behind every rule
> the P1UNI hedging engine emits, and why the 6-month validation study
> (`research/results/VALIDATION/`) killed four of the five candidates
> while keeping R5.
>
> **Scope.** This document derives, from first principles, the dealer
> hedging mechanics that make index options a self-referential market.
> The goal is not formal completeness but operational clarity: every
> equation below has a direct bearing on a live rule.

---

## 0. Notation

| Symbol             | Meaning                                                                    |
| ------------------ | -------------------------------------------------------------------------- |
| $S_t$              | Underlying spot at time $t$ (ES, SPX, etc.)                                |
| $K$                | Strike                                                                     |
| $\sigma$           | Implied volatility                                                         |
| $\tau = T - t$     | Time to expiry (years)                                                     |
| $r$                | Risk-free rate                                                             |
| $V(S, \sigma, t)$  | Option value                                                               |
| $\Delta$           | $\partial V / \partial S$ — change in option value per unit spot           |
| $\Gamma$           | $\partial^2 V / \partial S^2$ — curvature of delta in spot                 |
| $\mathcal{V}$      | $\partial V / \partial \sigma$ — vega                                      |
| $\Theta$           | $\partial V / \partial t$ — time decay                                     |
| $\text{Vanna}$     | $\partial^2 V / (\partial S \, \partial \sigma)$                           |
| $\text{Charm}$     | $\partial^2 V / (\partial S \, \partial t) = - \partial \Delta / \partial t$ |
| $G(K)$             | Aggregate dealer gamma at strike $K$                                       |
| $Z$                | Zero-gamma level (spot at which net dealer gamma flips sign)               |
| $\mathrm{CW}$      | Call wall — strike with maximum dealer long gamma on calls                 |
| $\mathrm{PW}$      | Put wall — strike with maximum dealer long gamma on puts                   |
| $\mathrm{drr}$     | $\sigma_{\text{25-call}} - \sigma_{\text{25-put}}$ — risk reversal         |
| $\mathrm{drr}_z$   | Z-score of $\mathrm{drr}$ over a rolling window                            |

Convention: **the market-maker books gamma with the opposite sign of
the customer**. When retail and institutional clients buy calls, dealers
write them and inherit **short gamma**; when they buy puts, dealers
write them and inherit short gamma on the downside too. Net dealer
gamma at a strike is the *algebraic* sum weighted by open interest and
moneyness.

---

## 1. Dynamic Delta Hedging — The Core Identity

A dealer who has sold a contingent claim $V$ and wants to be instantaneously
flat with respect to small moves in $S$ holds $\Delta$ units of the
underlying against each contract:

$$
\Pi_t \;=\; -V_t \;+\; \Delta_t \, S_t
\qquad\Longrightarrow\qquad
d\Pi_t \;=\; -\,dV_t \;+\; \Delta_t \, dS_t.
$$

Applying Itô to $V(S, \sigma, t)$ under Black–Scholes GBM
$dS = \mu S\, dt + \sigma S\, dW$:

$$
dV \;=\; \Theta\, dt + \Delta\, dS + \tfrac{1}{2}\Gamma\,\sigma^2 S^2\, dt
\;+\; \text{(higher-order $\sigma$ terms)}.
$$

Substituting into $d\Pi$ the first-order $dS$ cancels and we are left with:

$$
\boxed{ \; d\Pi_t \;=\; -\,\Theta\, dt \;-\; \tfrac{1}{2}\,\Gamma\,\sigma^2 S^2\, dt
\;+\; \text{hedging re-balancing P\&L} \; }
$$

**Interpretation.** Between re-balancings, the dealer's book drifts at
rate $-\tfrac{1}{2}\Gamma\sigma^2 S^2$. The **sign of $\Gamma$ determines
whether volatility is paying them or costing them.** That asymmetry is
the engine of every rule we trade on.

Because $\Delta$ is a function of $S$, when spot moves from $S$ to
$S + \Delta S$ the hedger's required stock position changes by

$$
\Delta(S + \Delta S) - \Delta(S) \;\approx\; \Gamma\,\Delta S.
$$

The dealer **buys $\Gamma \Delta S$ shares when the move is up and
sells $\Gamma \Delta S$ when it is down** if they are *long gamma*,
and does the opposite if they are *short gamma*. This is the whole
trick.

---

## 2. Long Gamma → Mean Reversion (Pinning)

When aggregate dealer gamma $G(S) > 0$ in a neighborhood of spot:

* Spot goes **up** by $\Delta S$.
* Dealer's $\Delta$ increases by $\Gamma \, \Delta S$.
* To stay flat they must **sell** $\Gamma \Delta S$ shares.
* Selling pushes price back **down**.

Conversely down-moves trigger buying. The dealer flow is therefore
**counter-trend**: it damps realised volatility. In a stochastic-control
framing, dealer rebalancing under $G>0$ behaves like a negative-feedback
servo on $S_t$:

$$
dS_t \;=\; \mu\, dt + \sigma\, dW_t \;-\; \kappa \cdot G(S_t) \cdot (S_t - S_t^\star)\, dt,
$$

where $S_t^\star$ is the "preferred" level — typically the strike where
$G$ peaks — and $\kappa > 0$ encodes how aggressively dealers must
re-hedge (a function of OI, gamma, and book liquidity). This is the
classical **pinning** equation: price becomes an Ornstein–Uhlenbeck
process with mean $S^\star$.

**Empirical signature.** In the P1UNI dataset, regime classification by
dealer gamma cleanly segments ES behaviour:

* `PINNING` (Study A1): realised volatility 35–45 % lower than baseline,
  touch frequency at CW/PW elevated, fade-the-wall trades *look* good
  on paper — but see §9.

---

## 3. Short Gamma → Amplification

When $G(S) < 0$:

* Spot goes up → dealer $\Delta$ falls → they must **buy** to stay
  flat → further up-move.

The feedback is now **positive** and the equation flips sign:

$$
dS_t \;=\; \mu\, dt + \sigma\, dW_t \;+\; \kappa \cdot |G(S_t)| \cdot (S_t - S_t^\star)\, dt.
$$

$S_t^\star$ ceases to be an attractor and becomes a **repeller**: once
spot crosses it, convexity accelerates the move. A small positive shock
can trigger a cascade; this is the mechanics behind "gamma squeezes"
and explains the fat tails that appear when net dealer gamma is
negative around the money.

Study D1 and F1 together showed that the conditional forward return
| regime = AMPLIFICATION AND aligned ≥ 5 / 9 tickers is **positive**
in the right tail (Study F1: +90 pt on 147 trades), but the OOS sample
was thin and R4 failed walk-forward (§9).

---

## 4. Charm — Time Decay of Delta

Charm is the rate at which $\Delta$ bleeds as expiry approaches at
constant $S$:

$$
\text{Charm} \;=\; \frac{\partial \Delta}{\partial t}
\;=\; -\,\frac{\partial^2 V}{\partial S\, \partial t}.
$$

For a Black–Scholes European call:

$$
\text{Charm}_{\text{call}} \;=\;
-\,\varphi(d_1)\, \frac{2r\tau - d_2\, \sigma \sqrt{\tau}}{2\tau\, \sigma\, \sqrt{\tau}}
\;-\; r e^{-r\tau}\,\Phi(d_2),
$$

where $d_{1,2}$ are the standard B–S terms and $\varphi, \Phi$ are the
normal pdf / cdf.

**Operational reading.** An OTM call close to expiry has **negative
charm**: even with $S$ unchanged, its $\Delta$ shrinks toward zero over
time. The dealer who short-wrote it holds a partially-hedging long
stock position that is now **too big** — they must **sell stock** as
the clock runs. Across thousands of OTM calls this produces a measurable
**intraday sell-pressure bias into 15:30** (Study B1). The mirror-image
effect on OTM puts creates a buy-pressure at the same time on the
downside.

The classical "charm drag" pattern — spot drifting toward the max-pain
strike in the last 2 hours — is just the integral of this delta
re-balancing.

---

## 5. Vanna — Cross Sensitivity to Volatility

Vanna measures how $\Delta$ changes when implied vol moves:

$$
\text{Vanna} \;=\; \frac{\partial \Delta}{\partial \sigma}
\;=\; \frac{\partial^2 V}{\partial S\, \partial \sigma}.
$$

Closed form for a BS call:

$$
\text{Vanna}_{\text{call}} \;=\; -\,\varphi(d_1)\,\frac{d_2}{\sigma}.
$$

Sign chart (OTM strike, long option):

* $S < K$ (OTM call): $d_2 < 0 \Rightarrow \text{Vanna} > 0$.
  When $\sigma$ rises, the option's $\Delta$ increases. Dealer who is
  short this option must **buy more stock** — a **flow up**.

* $S > K$ (OTM put): symmetric. When $\sigma$ rises the put's $\Delta$
  magnitude shrinks; the dealer who is short the put must **sell stock**.

Translating back to the index level: during **vol-compression days**
($d\sigma < 0$, which is the modal state) short-gamma dealers on OTM
calls sell stock and short-gamma dealers on OTM puts buy stock — a
**net supportive bias**. During **vol-expansion days** the flow
reverses sharply, which is the microstructural reason sell-offs feed
on themselves.

This is also why the **drr z-score** matters: a 1-day drr jump is
essentially a shock to $\partial\sigma_{\text{call}}/\partial t$ versus
$\partial\sigma_{\text{put}}/\partial t$, which shows up in Vanna flows
asymmetrically (see §8 and §9).

---

## 6. Zero Gamma — Equilibrium or Inflection?

$Z$ is defined implicitly by

$$
G(Z) \;\equiv\; \sum_{i} \gamma_i(Z) \cdot \text{OI}_i \cdot \text{sign}_i \;=\; 0,
$$

summed across every strike and expiry weighted by its per-contract
$\Gamma$ at spot = $Z$. The classical textbook story treats $Z$ as an
equilibrium: $S > Z \Rightarrow G>0$ (pinning), $S<Z \Rightarrow G<0$
(amplification).

**What the P1UNI data actually shows.** In the 6-month dataset:

| Zone              | $\langle$|ret_5m|$\rangle$ | $\langle$touch_count$\rangle$ |
| ----------------- | -------------------------- | ----------------------------- |
| $S$ far above $Z$ | 0.18 pt                    | 0.9 / 5min                    |
| $S \approx Z$     | **0.37 pt**                | **2.4 / 5min**                |
| $S$ far below $Z$ | 0.21 pt                    | 1.1 / 5min                    |

Realised vol is **highest at $Z$, not lowest**. This reframes zero
gamma not as a mean-reversion magnet but as an **inflection point**:
the place where the convexity of dealer flow changes sign. In practice
trades initiated *at* $Z$ under-performed trades initiated on either
side, which is why no direct-Z rule survived validation.

The rule that *did* survive (R5) fires **above the call wall**, not at
$Z$ — i.e. on the strongly-long-gamma side where the mean-reversion
identity is cleanest.

---

## 7. Walls — Attractors or Barriers?

Let $\mathrm{CW} = \arg\max_K G_{\text{call}}(K)$ and
$\mathrm{PW} = \arg\max_K G_{\text{put}}(K)$ (both taken over 0DTE OI
by default in this system).

**Barrier interpretation.** Near $\mathrm{CW}$ the aggregate dealer
book is *most long gamma on calls*. When spot approaches from below,
dealers sell to re-hedge (negative-feedback loop, §2). The effective
price-ceiling mechanic is:

$$
\mathbb{E}[S_{t+\delta} \mid S_t = \mathrm{CW}^-] \;<\; \mathrm{CW},
$$

i.e. expected next-bar return is negative in a neighborhood below the
call wall.

**Attractor interpretation.** Conversely if spot is above the wall and
dealers are still long gamma (they are, since the wall is the peak),
sell pressure continues to pull spot **back toward $\mathrm{CW}$**.
This is the R5 mechanic:

$$
S_t > \mathrm{CW} \quad\text{with}\quad G(\mathrm{CW}) > 0
\;\Longrightarrow\;
\mathbb{E}[S_{t+h} - S_t] < 0 \;\text{ for } h \in [5, 30]\text{ min}.
$$

R5 packages this into an edge-triggered SHORT: fire on the fresh cross
from below-to-above, exploit the snap-back, exit at TP = 12 pt or
SL = 8 pt. Validation: 15 OOS trades, mean $+1.38$ pt/trade net of 1 pt
slippage.

**Why R3 failed (fade CW in pinning).** The fade-the-wall trade *did*
work in-sample because pinning days have the cleanest mean reversion.
But the rule was stationary — it fired on *every* touch, not just
fresh crosses, and the wall kept moving as OI re-allocated through the
day. On OOS the edge collapsed.

---

## 8. Risk Reversal and Skew

The 25-delta risk reversal is defined

$$
\mathrm{drr}_t \;=\; \sigma_{\text{25C}}(t) \;-\; \sigma_{\text{25P}}(t).
$$

A positive drr means the right tail is priced richer than the left
tail (bullish skew). The z-score

$$
\mathrm{drr}_z(t) \;=\; \frac{\mathrm{drr}_t - \mu_n(\mathrm{drr})}{\sigma_n(\mathrm{drr})},
$$

(windowed, e.g. 20-day) captures sudden regime shifts.

$\mathrm{drr}_z \geq +2$ is unusual: over the last 6 months it fired
in ≤ 5 % of sessions. The interpretation is **euphoria** — option
buyers are chasing upside calls so hard that the call skew overtakes
the put skew by more than two historical standard deviations. The
dealer response:

* Writes calls at aggressive skew → inherits short gamma on the upside.
* Short gamma → must buy on up-moves (§3), amplifying the rally.
* But the skew itself is now overextended; *any* drop in 25-call IV
  triggers a reverse Vanna flow (§5) — dealers dump stock — feeding
  a fast unwind.

R1 tried to short this euphoria in TRANSITION regime (where
amplification dominates). **It failed OOS** not because the mechanic
is wrong but because genuine drr_z $\geq +2$ events in TRANSITION are
rare; the study produced n < 10 OOS trades and the result was
statistical noise. A larger dataset (years, not months) would likely
rehabilitate the hypothesis; we do not trade it until then.

**Skew (third moment of gex-weighted strike distribution)** is a
related concept:

$$
\text{gex\_skew} \;=\; \frac{m_3}{\sigma^3},\qquad
m_k \;=\; \sum_i \bigl|\text{gex\_oi}_i\bigr| \cdot (K_i - \bar K)^k / W.
$$

Negative gex_skew means the dealer book's gamma mass sits **below**
spot — the market-maker is short gamma on downside strikes. R2 tried
to short when gex_skew $< -0.5$ in non-TRANSITION regimes. OOS mean
was negative and the rule was dropped.

---

## 9. Linking the 5 Candidate Rules to the Hedging Mechanism

| Rule  | Hypothesis                                                            | Mechanism                                                   | OOS verdict |
| ----- | --------------------------------------------------------------------- | ----------------------------------------------------------- | ----------- |
| **R1** | drr_z $\geq +2$ ∧ regime = TRANSITION → SHORT                         | §8 skew-unwind Vanna cascade                                | **FAIL** n<10 |
| **R2** | gex_skew $< -0.5$ ∧ regime ≠ TRANSITION → SHORT                       | Book short on downside → dealer amplifies fall              | **FAIL** OOS mean < 0 |
| **R3** | Fade CW in PINNING → SHORT                                             | Long gamma near wall → mean reversion (§7 barrier)          | **FAIL** train overfit |
| **R4** | CW breakout in AMPLIFICATION + cross-asset aligned ≥ 5/9 → LONG       | Short gamma + regime confirmation                           | **FAIL** n<10 |
| **R5** | $S > \mathrm{CW}$ (fresh cross) → SHORT                                | §7 attractor / long-gamma snap-back                         | **PASS** +1.38 pt/trade |

The single rule that survived is also the one with the **cleanest
hedging-mechanism story**: long gamma + spot above the peak of long
gamma is textbook pinning. It fires rarely, it fires edge-triggered
(so there is no overtrading), and the 1-pt slippage haircut does not
eat the edge.

**Why the composite failed.** The five rules overlap in ambiguous
regimes (TRANSITION often coincides with skew extremes), so a naive
union backtest stacks losing trades on top of winners: $-139$ pt total,
$238$ pt max drawdown. **Combine nothing**: only R5 runs in production,
in parallel to ML v3.5, gated by the same risk / session / bridge-
health checks.

---

## 10. Operational Summary — What the Engine Actually Does

The production `HedgingSignalEngine` implements the subset of the math
that survived validation. On every tick:

1. Read `spot`, `call_wall_oi` from the live feature vector.
2. Detect the moment $\text{spot}_{t-1} \leq \mathrm{CW} <
   \text{spot}_t$ (edge = fresh cross from below to above).
3. If the 900-second cooldown is satisfied, emit
   $(\text{SHORT}, \,\text{confidence}=0.65)$ with rule tag
   `R5_aboveCW_short`.
4. The signal then flows through the standard P1UNI risk / level /
   execution pipeline with $\text{TP}=12$ pt, $\text{SL}=8$ pt.

Everything else in this document is context. The rule is literally
four lines of arithmetic — but it is four lines that rest on a
provable identity (§1), a regime-dependent sign flip (§2, §3), and an
empirical OOS test with 1-pt slippage that *could* have killed it and
didn't.

The mathematics above is the reason we trust those four lines.

---

*Validated 2026-04-22 against the P1UNI 6-month GEX dataset. See
`research/results/VALIDATION/validation_summary.json` for the raw
verdicts, `research/study_VALIDATION_composite.py` for the full test
harness, `src/execution/hedging_signals.py` for the production code.*
