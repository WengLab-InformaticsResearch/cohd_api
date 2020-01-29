import cohd_utilities

def test_cohd_double_poisson_ci():
    assert cohd_utilities.double_poisson_ci(50) == (29.0, 75.0)


def test_ln_ratio_ci():
    assert cohd_utilities.ln_ratio_ci(50, 2.0) == (1.4552728245583282, 2.4054651081081646)
