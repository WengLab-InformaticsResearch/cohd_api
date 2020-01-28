import cohd_utilities

def test_cohd_utilties():
    assert cohd_utilities.double_poisson_ci(50) == (29.0, 75.0)

    assert cohd_utilities.ln_ratio_ci(50, 2.0) == (1.4552728245583282, 2.4054651081081646)
