#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, multiple-statements, missing-function-docstring, missing-class-docstring, fixme.
"""
Evaluation functions collection
"""

import numpy as np
from scipy import stats

RISK_FREE = 0 # for very short expirations it is negligible

def black_scholes_call_price(volatility, K, t, S):
    d1 = (np.log(S/K) + (RISK_FREE + 0.5*volatility**2) * t)/(volatility*np.sqrt(t))
    d2 = (np.log(S/K) + (RISK_FREE - 0.5*volatility**2) * t)/(volatility*np.sqrt(t))
    return S*stats.norm.cdf(d1, 0.0, 1.0) - K*np.exp(-RISK_FREE*t)*stats.norm.cdf(d2, 0.0, 1.0)

def target_option_price(options_for_the_symbol, K, t, S):

    if options_for_the_symbol["volatility_shape"]:
        # deviation = (K/S - 1) * 100
        # a = options_for_the_symbol["volatility_shape"]["a"]
        # b = options_for_the_symbol["volatility_shape"]["b"]
        c = options_for_the_symbol["volatility_shape"]["c"]
        # k = options_for_the_symbol["volatility_shape"]["k"]
        # threshold = options_for_the_symbol["volatility_shape"]["threshold"]
        # if deviation <= threshold:
        #     correction_factor = a * deviation**2 + b * deviation + c
        # else:
        #     correction_factor = k * (deviation - threshold) + (a * threshold**2 + b * threshold + c)
        correction_factor = c
    else:
        correction_factor = 1
    S_trend_correction = options_for_the_symbol["trend_b"] * t * 365
    bs = black_scholes_call_price(options_for_the_symbol["volatility"] * correction_factor, K, t, S + S_trend_correction)
    # bs1 = black_scholes_call_price(options_for_the_symbol["volatility"] * correction_factor, K, t, S)
    #print ("volatility", (options_for_the_symbol["symbol"], K), options_for_the_symbol["volatility"], correction_factor, t, S, S_trend_correction, bs)
    return bs
