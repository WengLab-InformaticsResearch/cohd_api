Testing plan for the Columbia Open Health Data (COHD) API published at https://cohd.io/api

## Manual tests

Use the 
[`notebooks/COHD_API_Example.ipynb`](https://github.com/WengLab-InformaticsResearch/cohd_api/blob/master/notebooks/COHD_API_Example.ipynb) 
Jupyter notebook to manually try queries against the [COHD API](https://cohd.io/api).

## Automated testing plan

Testing of the COHD API is separated in 3 parts:

- **Integration**: the API is tested using integration tests on a development instance of the API and with unit tests at
  every push to Github. This allows us to prevent deploying the OpenPredict API if the changes added broke some of its 
  features
- **Production**: the API hosted in production is tested by a Github workflow every day at 03:00 and 15:00 GMT, so that   
  we are quickly notified if the production API is having an issue
- **Deployment**: a workflow testing the COHD API Docker image build process will be developed to ensure the API can be 
  redeployed easily

When one of these workflows fails, we take action to fix the source of the problem.

Requirements: see requirements.txt. Install the required dependency if you want to run the tests locally:

```bash
pip install pytest
```

### Production tests

![](https://github.com/WengLab-InformaticsResearch/cohd_api/workflows/COHD%20API%20Monitoring%20Workflow/badge.svg)

Continuous monitoring tests are run automatically by the 
[GitHub Action workflow `.github/workflows/cohd-monitor.yml`](https://github.com/WengLab-InformaticsResearch/cohd_api/actions/workflows/cohd-monitor.yml)
every day at 03:00 and 15:00 GMT on the [COHD API](https://cohd.io/api)

We test all of the COHD SmartAPI endpoints and the TRAPI endpoints for an expected number of results.

To run the tests of the COHD production API locally:  
From the `cohd_api` directory, run:
```bash
pytest -s test_cohd_io.py
```

### Integration tests

![](https://github.com/WengLab-InformaticsResearch/cohd_api/workflows/COHD%20API%20Continuous%20Integration%20Workflow/badge.svg)

Integration tests and unit tests on the development instance of the COHD API are run manually prior to pushing code to 
GitHub.  
First, in `notebooks/cohd_helpers/cohd_requests.py`, change the server URL on line 11 to the development instance, e.g.,
```
server = 'https://dev.cohd.io/api'
```
Then, from the `cohd_api` directory, run:
```bash
pytest -s test_cohd_io.py
pytest -s cohd/test_unit_tests.py
```

The unit tests are also run automatically when code is pushed to GitHub by the
[GitHub Action workflow `.github/workflows/cohd-api-ci.yml`](https://github.com/WengLab-InformaticsResearch/cohd_api/actions/workflows/cohd-api-ci.yml)


To run a specific test in a specific file, and display `print()` lines in the output:

```bash
pytest -s test_cohd_io.py::test_translator_query_11x
```

## Docker tests

A workflow testing the COHD API Docker image build process will be developed to ensure the API can be redeployed easily

## Known issues

Facing issue with `pytest` install even using virtual environments? Try this solution:

```bash
python3 -m pip install -e .
python3 -m pip install pytest
python3 -m pytest
```
