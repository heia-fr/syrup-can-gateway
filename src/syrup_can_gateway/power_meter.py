import numpy as np

K0 = [2.5, 3.3, 4.3, 5.5, 6.6, 8.3, 9.5, 11.0]
K1 = [5.99, 7.13, 8.38, 10.21, 12.08, 14.65, 17.23, 19.51]
O1 = [-66.4, -66.6, -64.0, -64.2, -65.0, -80.6, -81.0, -88.6]
MIN_RESISTANCE = 1
MAX_RESISTANCE = 8
MIN_SPEED = 0.0  # km/h
INFLECTION_POINT = 10.0  # km/h
MAX_SPEED = 80.0  # km/h
SPEED_STEP = 10.0  # km/h


def power(speed: float, resistance: float) -> float:
    x_points = range(MIN_RESISTANCE, MAX_RESISTANCE + 1)
    k0 = np.interp(resistance, x_points, K0)
    k1 = np.interp(resistance, x_points, K1)
    o1 = np.interp(resistance, x_points, O1)

    x_points = range(int(MIN_SPEED), int(MAX_SPEED) + 1, int(SPEED_STEP))
    y_points = [k0 * x if x <= INFLECTION_POINT else k1 * x + o1 for x in x_points]

    return np.interp(speed, x_points, y_points)
