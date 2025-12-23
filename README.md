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

**The code expects a Linux-like environment with rsync and unzip installed, it will not run on windows, and has not been tested on MacOS.**  If you do not have access to a Linux environment use [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install).

1. Clone the repository
2. Create a conda environment using the `environment.yml`
3. Create a file containing (only) your openapi key
4. Update `common/env_vars.py` to match your configuration.
5. Run `setup_data.sh` (download ILEC data + setup duckdb)

## Running 

1. Set the working directory to the GitHub project folder
2. Run `start_mcp_server.sh`
3. Run `start_ui_server.sh`
