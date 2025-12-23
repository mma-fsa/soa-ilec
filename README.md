# MMAgent

## Description

MMAgent is a AI-enabled web application for building actuarial assumptions.  A short video demo is available [here](https://www.youtube.com/watch?v=xFJH815tRTw). It includes the following features:

 * Fully transparent audit log containing all steps executed by the agent.
 * R-based backend for Poisson Regression MCP Tools (GLMs & Decision Trees).
 * Comprehensive export capabilities (both R code and Excel).
 * Integrated SQL editor for data exploration, preparing modeling data, and examining results.

## Colab Notebook

The easiest way to run MMAgent is via the [Google Colab Notebook](https://colab.research.google.com/drive/1aeAfdvQAaWAy8KRZsc9qGZ58Obr98MNc?usp=sharing).

## Local Setup 

1. Clone the repository
2. Create a conda environment using the `environment.yml`
3. Create a file containing (only) your openapi key
4. Update `common/env_vars.py` to match your configuration.
5. Download and unzip the [SOA ILEC Data](https://cdn-files.soa.org/research/ilec/ilec-mort-text-data-dict-2012-2019.zip).
6. Update `scripts/setup_ilec_ddb.py` to match the ILEC data location.

## Running 

1. Set the working directory to the GitHub project folder
2. Run `start_mcp_server.sh`
3. Run `start_ui_server.sh`
