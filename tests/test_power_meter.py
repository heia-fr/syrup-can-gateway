# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supci@hefr.ch>
#
# SPDX-License-Identifier: MIT


import pytest

from syrup_can_gateway.power_meter import power


def test_power_calculation():

    assert power(20.0, 3.0) == pytest.approx(103.6)
    assert power(10.0, 6.0) == pytest.approx(83)
    assert power(60.0, 5.0) == pytest.approx(659.8)

    assert power(20, 4) < power(25.0, 4.5) < power(30.0, 5.0)
